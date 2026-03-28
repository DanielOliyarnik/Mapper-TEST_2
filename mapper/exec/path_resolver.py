from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from pathlib import Path


@dataclass(frozen=True)
class RunPaths:
    root: Path
    cache_root: Path
    data_root: Path
    manifests_root: Path
    metrics_root: Path
    timestamp: str


@dataclass(frozen=True)
class StagePaths:
    stage_name: str
    root: Path
    cache_dir: Path
    data_dir: Path
    manifest_path: Path
    metrics_path: Path
    config_snapshot_path: Path


def resolve_run_paths(output_root: Path, run_name: str, timestamp: str | None = None, *, create: bool = True) -> RunPaths:
    stamp = timestamp or datetime.now().strftime("%Y%m%d_%H%M%S")
    root = output_root / run_name / stamp
    run_paths = RunPaths(
        root=root,
        cache_root=root / "CACHE",
        data_root=root / "DATA",
        manifests_root=root / "MANIFESTS",
        metrics_root=root / "METRICS",
        timestamp=stamp,
    )
    if create:
        for path in (run_paths.root, run_paths.cache_root, run_paths.data_root, run_paths.manifests_root, run_paths.metrics_root):
            path.mkdir(parents=True, exist_ok=True)
    return run_paths


def resolve_stage_paths(run_paths: RunPaths, stage_name: str, *, create: bool = True) -> StagePaths:
    stage_root = run_paths.root / stage_name
    stage_paths = StagePaths(
        stage_name=stage_name,
        root=stage_root,
        cache_dir=stage_root / "CACHE",
        data_dir=stage_root / "DATA",
        manifest_path=run_paths.manifests_root / f"{stage_name}.manifest.json",
        metrics_path=run_paths.metrics_root / f"{stage_name}.metrics.json",
        config_snapshot_path=stage_root / "config_snapshot.json",
    )
    if create:
        for path in (stage_paths.root, stage_paths.cache_dir, stage_paths.data_dir):
            path.mkdir(parents=True, exist_ok=True)
    return stage_paths
