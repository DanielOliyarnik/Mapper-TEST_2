from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
import json
from pathlib import Path

from mapper.stages.common import ArtifactRef, InputRef


@dataclass(frozen=True)
class RunPaths:
    root: Path
    cache_root: Path
    data_root: Path
    manifests_root: Path
    metrics_root: Path
    logs_root: Path
    pipeline_summary_path: Path
    pipeline_event_log_path: Path
    timestamp: str

    @property
    def run_root(self) -> Path:
        return self.root


@dataclass(frozen=True)
class StagePaths:
    stage_name: str
    root: Path
    cache_dir: Path
    data_dir: Path
    eval_dir: Path
    logs_dir: Path
    manifest_path: Path
    metrics_path: Path
    config_snapshot_path: Path
    event_log_path: Path
    error_path: Path


def resolve_run_paths(output_root: Path, run_name: str, timestamp: str | None = None, *, create: bool = True) -> RunPaths:
    stamp = timestamp or datetime.now().strftime("%Y%m%d_%H%M%S")
    root = output_root / run_name / stamp
    run_paths = RunPaths(
        root=root,
        cache_root=root / "CACHE",
        data_root=root / "DATA",
        manifests_root=root / "MANIFESTS",
        metrics_root=root / "METRICS",
        logs_root=root / "LOGS",
        pipeline_summary_path=root / "pipeline_summary.json",
        pipeline_event_log_path=root / "LOGS" / "pipeline.events.jsonl",
        timestamp=stamp,
    )
    if create:
        for path in (
            run_paths.root,
            run_paths.cache_root,
            run_paths.data_root,
            run_paths.manifests_root,
            run_paths.metrics_root,
            run_paths.logs_root,
        ):
            path.mkdir(parents=True, exist_ok=True)
    return run_paths


def resolve_stage_paths(run_paths: RunPaths, stage_name: str, *, create: bool = True) -> StagePaths:
    stage_root = run_paths.root / stage_name
    stage_paths = StagePaths(
        stage_name=stage_name,
        root=stage_root,
        cache_dir=stage_root / "CACHE",
        data_dir=stage_root / "DATA",
        eval_dir=stage_root / "EVAL",
        logs_dir=stage_root / "LOGS",
        manifest_path=run_paths.manifests_root / f"{stage_name}.manifest.json",
        metrics_path=run_paths.metrics_root / f"{stage_name}.metrics.json",
        config_snapshot_path=stage_root / "config_snapshot.json",
        event_log_path=stage_root / "LOGS" / "events.jsonl",
        error_path=stage_root / "LOGS" / "error.json",
    )
    if create:
        for path in (stage_paths.root, stage_paths.cache_dir, stage_paths.data_dir, stage_paths.eval_dir, stage_paths.logs_dir):
            path.mkdir(parents=True, exist_ok=True)
    return stage_paths


def _run_name_root(output_root: Path, run_name: str) -> Path:
    return output_root / run_name


def _iter_run_roots(output_root: Path, run_name: str) -> list[Path]:
    run_name_root = _run_name_root(output_root, run_name)
    if not run_name_root.exists():
        return []
    return sorted((path for path in run_name_root.iterdir() if path.is_dir()), reverse=True)


def _load_manifest_json(manifest_path: Path) -> dict:
    return json.loads(manifest_path.read_text(encoding="utf-8"))


def resolve_latest_stage_manifest(
    output_root: Path,
    run_name: str,
    stage_name: str,
    *,
    allowed_statuses: tuple[str, ...] = ("success",),
    exclude_timestamp: str | None = None,
) -> Path | None:
    for run_root in _iter_run_roots(output_root, run_name):
        if exclude_timestamp and run_root.name == exclude_timestamp:
            continue
        manifest_path = run_root / "MANIFESTS" / f"{stage_name}.manifest.json"
        if not manifest_path.exists():
            continue
        manifest_payload = _load_manifest_json(manifest_path)
        status = manifest_payload.get("manifest", {}).get("status")
        if status in allowed_statuses:
            return manifest_path
    return None


def artifact_ref_from_payload(payload: dict) -> ArtifactRef:
    return ArtifactRef(
        key=str(payload["key"]),
        path=Path(payload["path"]),
        kind=str(payload["kind"]),
        role=str(payload["role"]),
        required=bool(payload.get("required", True)),
    )


def resolve_input_ref(
    *,
    source_stage: str,
    artifact_payload: dict,
    manifest_path: Path,
    resolution_mode: str,
) -> InputRef:
    return InputRef(
        name=str(artifact_payload["key"]),
        source_stage=source_stage,
        artifact_key=str(artifact_payload["key"]),
        path=Path(artifact_payload["path"]),
        manifest_path=manifest_path,
        resolution_mode=resolution_mode,
        required=bool(artifact_payload.get("required", True)),
    )


def resolve_latest_artifact(
    output_root: Path,
    run_name: str,
    stage_name: str,
    artifact_key: str,
    *,
    allowed_statuses: tuple[str, ...] = ("success",),
    exclude_timestamp: str | None = None,
) -> Path | None:
    manifest_path = resolve_latest_stage_manifest(
        output_root,
        run_name,
        stage_name,
        allowed_statuses=allowed_statuses,
        exclude_timestamp=exclude_timestamp,
    )
    if manifest_path is None:
        return None
    manifest_payload = _load_manifest_json(manifest_path)
    for artifact_payload in manifest_payload.get("manifest", {}).get("output_refs", []):
        if artifact_payload.get("key") == artifact_key:
            return Path(artifact_payload["path"])
    return None
