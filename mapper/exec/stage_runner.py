from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Mapping

from mapper.stages.common import InputRef, StageTaskRequest, StageTaskResult, StageWarning, validate_artifact_bundle

from .path_resolver import RunPaths, StagePaths, resolve_stage_paths
from .stage_manifest import write_config_snapshot, write_metrics, write_stage_manifest


@dataclass
class StageRunRecord:
    stage_name: str
    task_name: str
    stage_paths: StagePaths
    request: StageTaskRequest
    result: StageTaskResult
    manifest_path: Any


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
    manifest_path = write_stage_manifest(
        request=request,
        result=result,
        manifest_path=stage_paths.manifest_path,
        config_snapshot_path=config_snapshot_path,
        metrics_path=metrics_path,
    )
    return StageRunRecord(
        stage_name=stage_name,
        task_name=task_name,
        stage_paths=stage_paths,
        request=request,
        result=result,
        manifest_path=manifest_path,
    )
