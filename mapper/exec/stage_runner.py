from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping

from mapper.stages.common import (
    ArtifactBundle,
    ArtifactRef,
    InputRef,
    StageTaskRequest,
    StageTaskResult,
    StageWarning,
    validate_artifact_bundle,
)

from .path_resolver import RunPaths, StagePaths, resolve_stage_paths
from .stage_manifest import load_stage_manifest, write_config_snapshot, write_metrics, write_stage_manifest


@dataclass
class StageRunRecord:
    stage_name: str
    task_name: str
    execution_mode: str
    stage_paths: StagePaths | None
    request: StageTaskRequest | None
    result: StageTaskResult
    manifest_path: Path
    metrics_path: Path | None
    config_snapshot_path: Path | None


def build_stage_request(
    *,
    stage_name: str,
    task_name: str,
    dataset_id: str,
    run_name: str,
    timestamp: str,
    stage_paths: StagePaths,
    config: Mapping[str, Any],
    input_refs: Mapping[str, InputRef],
    upstream_manifest_refs: tuple[Any, ...],
) -> StageTaskRequest:
    return StageTaskRequest(
        stage_name=stage_name,
        task_name=task_name,
        dataset_id=dataset_id,
        run_name=run_name,
        timestamp=timestamp,
        cache_dir=stage_paths.cache_dir,
        data_dir=stage_paths.data_dir,
        config=dict(config),
        input_refs=dict(input_refs),
        upstream_manifest_refs=upstream_manifest_refs,
    )


def _artifact_ref_from_payload(payload: Mapping[str, Any]) -> ArtifactRef:
    return ArtifactRef(
        key=str(payload["key"]),
        path=Path(str(payload["path"])),
        kind=str(payload["kind"]),
        role=str(payload["role"]),
        required=bool(payload.get("required", True)),
    )


def _warning_from_payload(payload: Mapping[str, Any]) -> StageWarning:
    return StageWarning(code=str(payload["code"]), message=str(payload["message"]))


def _coerce_result(result: StageTaskResult, stage_name: str, task_name: str) -> StageTaskResult:
    if result.status == "not_implemented" and not result.warnings:
        return StageTaskResult(
            status=result.status,
            artifact_bundle=result.artifact_bundle,
            metrics=result.metrics,
            warnings=(StageWarning(code="not_implemented", message=f"{stage_name}.{task_name} scaffold only"),),
            extras=result.extras,
        )
    return result


def load_stage_record_from_manifest(stage_name: str, manifest_path: Path) -> StageRunRecord:
    manifest_payload = load_stage_manifest(manifest_path)
    manifest_body = manifest_payload.get("manifest", {})
    output_refs = tuple(_artifact_ref_from_payload(item) for item in manifest_body.get("output_refs", []))
    warnings = tuple(_warning_from_payload(item) for item in manifest_body.get("warnings", []))
    result = StageTaskResult(
        status=str(manifest_body.get("status", "unknown")),
        artifact_bundle=ArtifactBundle(primary=output_refs),
        warnings=warnings,
    )
    metrics_path_value = manifest_body.get("metrics_path")
    config_snapshot_value = manifest_body.get("config_snapshot_path")
    return StageRunRecord(
        stage_name=stage_name,
        task_name=str(manifest_body.get("task_name", "")),
        execution_mode="reused",
        stage_paths=None,
        request=None,
        result=result,
        manifest_path=manifest_path,
        metrics_path=Path(metrics_path_value) if metrics_path_value else None,
        config_snapshot_path=Path(config_snapshot_value) if config_snapshot_value else None,
    )


def run_stage(
    *,
    stage_def,
    run_paths: RunPaths,
    dataset_id: str,
    run_name: str,
    stage_name: str,
    task_name: str,
    config: Mapping[str, Any],
    input_refs: Mapping[str, InputRef],
    upstream_manifest_refs: tuple[Any, ...],
) -> StageRunRecord:
    stage_paths = resolve_stage_paths(run_paths, stage_name, create=True)
    request = build_stage_request(
        stage_name=stage_name,
        task_name=task_name,
        dataset_id=dataset_id,
        run_name=run_name,
        timestamp=run_paths.timestamp,
        stage_paths=stage_paths,
        config=config,
        input_refs=input_refs,
        upstream_manifest_refs=upstream_manifest_refs,
    )
    task = stage_def.get_task(task_name)
    result = _coerce_result(task.run(request), stage_name, task_name)
    if result.status == "success":
        validate_artifact_bundle(result.artifact_bundle, stage_def.get_artifact_contract(task_name))
    config_snapshot_path = write_config_snapshot(stage_paths.config_snapshot_path, request.config)
    metrics_path = write_metrics(stage_paths.metrics_path, result.metrics) if result.metrics else None
    schema_path_value = request.config.get("schema_path")
    schema_path = Path(str(schema_path_value)) if schema_path_value else None
    manifest_path = write_stage_manifest(
        request=request,
        result=result,
        manifest_path=stage_paths.manifest_path,
        config_snapshot_path=config_snapshot_path,
        metrics_path=metrics_path,
        schema_path=schema_path,
    )
    return StageRunRecord(
        stage_name=stage_name,
        task_name=task_name,
        execution_mode="executed",
        stage_paths=stage_paths,
        request=request,
        result=result,
        manifest_path=manifest_path,
        metrics_path=metrics_path,
        config_snapshot_path=config_snapshot_path,
    )
