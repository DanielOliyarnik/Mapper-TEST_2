from __future__ import annotations

from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Mapping

from .types import ArtifactRef, InputRef, StageTaskRequest, StageTaskResult, StageWarning


@dataclass
class StageManifestData:
    stage_name: str
    task_name: str
    dataset_id: str
    run_name: str
    timestamp: str
    status: str
    input_refs: tuple[InputRef, ...]
    output_refs: tuple[ArtifactRef, ...]
    metrics_path: Path | None
    config_snapshot_path: Path
    upstream_manifest_refs: tuple[Path, ...] = ()
    schema_path: Path | None = None
    warnings: tuple[StageWarning, ...] = ()


@dataclass
class StageManifestEnvelope:
    version: str
    manifest: StageManifestData


def build_stage_manifest(
    *,
    request: StageTaskRequest,
    result: StageTaskResult,
    config_snapshot_path: Path,
    metrics_path: Path | None,
    schema_path: Path | None = None,
) -> StageManifestData:
    return StageManifestData(
        stage_name=request.stage_name,
        task_name=request.task_name,
        dataset_id=request.dataset_id,
        run_name=request.run_name,
        timestamp=request.timestamp,
        status=result.status,
        input_refs=tuple(request.input_refs.values()),
        output_refs=result.artifact_bundle.all_refs(),
        metrics_path=metrics_path,
        config_snapshot_path=config_snapshot_path,
        upstream_manifest_refs=request.upstream_manifest_refs,
        schema_path=schema_path,
        warnings=result.warnings,
    )


def stage_manifest_to_dict(manifest: StageManifestData) -> Mapping[str, Any]:
    return asdict(StageManifestEnvelope(version="1", manifest=manifest))
