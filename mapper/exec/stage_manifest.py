from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Mapping

from mapper.stages.common import StageTaskRequest, StageTaskResult, build_stage_manifest, stage_manifest_to_dict


def write_json(path: Path, payload: Mapping[str, Any]) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True, default=str), encoding="utf-8")
    return path


def write_config_snapshot(path: Path, config: Mapping[str, Any]) -> Path:
    return write_json(path, dict(config))


def write_metrics(path: Path, metrics: Mapping[str, Any]) -> Path:
    return write_json(path, dict(metrics))


def write_stage_manifest(
    *,
    request: StageTaskRequest,
    result: StageTaskResult,
    manifest_path: Path,
    config_snapshot_path: Path,
    metrics_path: Path | None,
    schema_path: Path | None = None,
) -> Path:
    manifest = build_stage_manifest(
        request=request,
        result=result,
        config_snapshot_path=config_snapshot_path,
        metrics_path=metrics_path,
        schema_path=schema_path,
    )
    return write_json(manifest_path, stage_manifest_to_dict(manifest))


def read_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def load_stage_manifest(path: Path) -> dict[str, Any]:
    return read_json(path)
