from __future__ import annotations

from typing import Any

from mapper.stages.common import InputRef

from .config_resolver import normalize_run_config
from .path_resolver import resolve_run_paths
from .run_catalog import record_run_event
from .stage_registry import list_stage_definitions
from .stage_runner import StageRunRecord, run_stage


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


def resolve_upstream_inputs(results: dict[str, StageRunRecord], stage_name: str) -> tuple[dict[str, InputRef], tuple[Any, ...]]:
    input_refs: dict[str, InputRef] = {}
    manifest_refs: list[Any] = []
    for upstream_name, record in results.items():
        manifest_refs.append(record.manifest_path)
        for artifact_ref in record.result.artifact_bundle.all_refs():
            input_refs[artifact_ref.key] = InputRef(
                name=artifact_ref.key,
                source_stage=upstream_name,
                artifact_key=artifact_ref.key,
                path=artifact_ref.path,
                manifest_path=record.manifest_path,
                resolution_mode="pipeline",
                required=artifact_ref.required,
            )
    return input_refs, tuple(manifest_refs)


def reuse_skipped_stage_outputs(results: dict[str, StageRunRecord], stage_name: str) -> StageRunRecord | None:
    return results.get(stage_name)


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
    )
    run_paths = resolve_run_paths(resolved.output_root, resolved.run_name, create=True)
    record_run_event(resolved.output_root, {"event": "run_start", "run_name": resolved.run_name, "timestamp": run_paths.timestamp})
    sequence = determine_stage_sequence(stage_defs, resolved.requested_stages)
    results: dict[str, StageRunRecord] = {}
    for stage_name in sequence:
        input_refs, upstream_manifest_refs = resolve_upstream_inputs(results, stage_name)
        stage_def = stage_defs[stage_name]
        stage_cfg = dict(cfg.get(stage_name, {}))
        stage_cfg.setdefault("dataset", resolved.dataset_id)
        record = run_stage(
            stage_def=stage_def,
            run_paths=run_paths,
            dataset_id=resolved.dataset_id,
            run_name=resolved.run_name,
            stage_name=stage_name,
            task_name=stage_def.spec.default_task,
            config=stage_cfg,
            input_refs=input_refs,
            upstream_manifest_refs=upstream_manifest_refs,
        )
        results[stage_name] = record
    record_run_event(resolved.output_root, {"event": "run_end", "run_name": resolved.run_name, "timestamp": run_paths.timestamp, "stages": list(sequence)})
    return {
        "run_name": resolved.run_name,
        "timestamp": run_paths.timestamp,
        "stages": [
            {"stage_name": name, "task_name": record.task_name, "status": record.result.status, "manifest_path": str(record.manifest_path)}
            for name, record in results.items()
        ],
    }
