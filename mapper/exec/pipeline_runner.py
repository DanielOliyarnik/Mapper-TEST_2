from __future__ import annotations

from typing import Any

from mapper.stages.common import InputRef

from .config_resolver import normalize_run_config, resolve_stage_task_config
from .path_resolver import resolve_input_ref, resolve_latest_stage_manifest, resolve_run_paths
from .run_catalog import build_pipeline_run_summary, mark_stage_status, record_run_event, write_pipeline_summary
from .stage_registry import list_stage_definitions
from .stage_runner import StageRunRecord, load_stage_record_from_manifest, run_stage


def determine_stage_sequence(stage_defs: dict[str, Any], requested_stages: tuple[str, ...]) -> tuple[str, ...]:
    ordered = sorted(stage_defs.values(), key=lambda item: item.spec.order)
    required: list[str] = []

    def add_with_upstream(stage_name: str) -> None:
        if stage_name in required:
            return
        stage_def = stage_defs[stage_name]
        for upstream_name in stage_def.spec.required_upstream:
            add_with_upstream(upstream_name)
        required.append(stage_name)

    for stage_name in requested_stages:
        add_with_upstream(stage_name)
    ordered_names = [stage_def.spec.name for stage_def in ordered]
    return tuple(name for name in ordered_names if name in required)


def _build_input_refs_from_record(record: StageRunRecord) -> dict[str, InputRef]:
    input_refs: dict[str, InputRef] = {}
    for artifact_ref in record.result.artifact_bundle.all_refs():
        input_refs[artifact_ref.key] = InputRef(
            name=artifact_ref.key,
            source_stage=record.stage_name,
            artifact_key=artifact_ref.key,
            path=artifact_ref.path,
            manifest_path=record.manifest_path,
            resolution_mode=record.execution_mode,
            required=artifact_ref.required,
        )
    return input_refs


def resolve_upstream_inputs(
    *,
    stage_def: Any,
    current_results: dict[str, StageRunRecord],
    output_root,
    run_name: str,
    current_timestamp: str,
) -> tuple[dict[str, InputRef], tuple[Any, ...], dict[str, StageRunRecord]]:
    input_refs: dict[str, InputRef] = {}
    manifest_refs: list[Any] = []
    reused_records: dict[str, StageRunRecord] = {}
    for upstream_name in stage_def.spec.required_upstream:
        if upstream_name in current_results:
            upstream_record = current_results[upstream_name]
        else:
            manifest_path = resolve_latest_stage_manifest(
                output_root,
                run_name,
                upstream_name,
                allowed_statuses=("success",),
                exclude_timestamp=current_timestamp,
            )
            if manifest_path is None:
                raise FileNotFoundError(
                    f"No successful prior manifest found for required upstream stage: {upstream_name}"
                )
            upstream_record = load_stage_record_from_manifest(upstream_name, manifest_path)
            reused_records[upstream_name] = upstream_record
        manifest_refs.append(upstream_record.manifest_path)
        input_refs.update(_build_input_refs_from_record(upstream_record))
    return input_refs, tuple(manifest_refs), reused_records


def reuse_skipped_stage_outputs(
    *,
    stage_name: str,
    output_root,
    run_name: str,
    current_timestamp: str,
) -> StageRunRecord:
    manifest_path = resolve_latest_stage_manifest(
        output_root,
        run_name,
        stage_name,
        allowed_statuses=("success",),
        exclude_timestamp=current_timestamp,
    )
    if manifest_path is None:
        raise FileNotFoundError(f"No successful prior manifest found for skipped stage: {stage_name}")
    return load_stage_record_from_manifest(stage_name, manifest_path)


def run_pipeline(
    cfg: dict[str, Any],
    *,
    run_name: str,
    stages: list[str] | None = None,
    retrain: list[str] | None = None,
) -> dict[str, Any]:
    stage_defs = list_stage_definitions()
    stage_specs = {name: stage_def.spec for name, stage_def in stage_defs.items()}
    resolved = normalize_run_config(
        cfg,
        run_name=run_name,
        stages=stages,
        retrain=retrain,
        stage_specs=stage_specs,
        stage_defs=stage_defs,
    )
    run_paths = resolve_run_paths(resolved.output_root, resolved.run_name, create=True)
    record_run_event(resolved.output_root, {"event": "run_start", "run_name": resolved.run_name, "timestamp": run_paths.timestamp})
    sequence = determine_stage_sequence(stage_defs, resolved.requested_stages)
    results: dict[str, StageRunRecord] = {}
    stage_records: list[dict[str, Any]] = []
    for stage_name in sequence:
        stage_def = stage_defs[stage_name]
        if stage_name not in resolved.requested_stages:
            record = reuse_skipped_stage_outputs(
                stage_name=stage_name,
                output_root=resolved.output_root,
                run_name=resolved.run_name,
                current_timestamp=run_paths.timestamp,
            )
        else:
            input_refs, upstream_manifest_refs, reused_records = resolve_upstream_inputs(
                stage_def=stage_def,
                current_results=results,
                output_root=resolved.output_root,
                run_name=resolved.run_name,
                current_timestamp=run_paths.timestamp,
            )
            for reused_name, reused_record in reused_records.items():
                results.setdefault(reused_name, reused_record)
            task_name = resolved.stage_task_names.get(stage_name, stage_def.spec.default_task)
            stage_cfg = resolve_stage_task_config(cfg, stage_name, task_name)
            record = run_stage(
                stage_def=stage_def,
                run_paths=run_paths,
                dataset_id=resolved.dataset_id,
                run_name=resolved.run_name,
                stage_name=stage_name,
                task_name=task_name,
                config=stage_cfg,
                input_refs=input_refs,
                upstream_manifest_refs=upstream_manifest_refs,
            )
        results[stage_name] = record
        mark_stage_status(
            resolved.output_root,
            run_name=resolved.run_name,
            timestamp=run_paths.timestamp,
            stage_name=record.stage_name,
            task_name=record.task_name,
            status=record.result.status,
            execution_mode=record.execution_mode,
            manifest_path=str(record.manifest_path),
        )
        stage_records.append(
            {
                "stage_name": record.stage_name,
                "task_name": record.task_name,
                "status": record.result.status,
                "execution_mode": record.execution_mode,
                "manifest_path": str(record.manifest_path),
            }
        )
    record_run_event(resolved.output_root, {"event": "run_end", "run_name": resolved.run_name, "timestamp": run_paths.timestamp, "stages": list(sequence)})
    summary = build_pipeline_run_summary(
        run_name=resolved.run_name,
        timestamp=run_paths.timestamp,
        stage_records=stage_records,
    )
    write_pipeline_summary(run_paths.pipeline_summary_path, summary)
    return summary
