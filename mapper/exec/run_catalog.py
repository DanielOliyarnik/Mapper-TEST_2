from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Mapping


def _catalog_path(output_root: Path) -> Path:
    return output_root / "run_catalog.jsonl"


def record_run_event(output_root: Path, event: Mapping[str, Any]) -> Path:
    path = _catalog_path(output_root)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(dict(event), sort_keys=True) + "\n")
    return path


def append_run_catalog_entry(output_root: Path, entry: Mapping[str, Any]) -> Path:
    return record_run_event(output_root, entry)


def mark_stage_status(
    output_root: Path,
    *,
    run_name: str,
    timestamp: str,
    stage_name: str,
    task_name: str,
    status: str,
    execution_mode: str,
    manifest_path: str,
    elapsed_seconds: float | None = None,
    error_path: str | None = None,
    status_detail: str = "",
) -> Path:
    return append_run_catalog_entry(
        output_root,
        {
            "event": "stage_status",
            "run_name": run_name,
            "timestamp": timestamp,
            "stage_name": stage_name,
            "task_name": task_name,
            "status": status,
            "execution_mode": execution_mode,
            "manifest_path": manifest_path,
            "elapsed_seconds": elapsed_seconds,
            "error_path": error_path,
            "status_detail": status_detail,
        },
    )


def build_pipeline_run_summary(
    *,
    run_name: str,
    timestamp: str,
    status: str,
    stage_records: list[Mapping[str, Any]],
) -> dict[str, Any]:
    return {
        "run_name": run_name,
        "timestamp": timestamp,
        "status": status,
        "num_stages": len(stage_records),
        "stages": list(stage_records),
    }


def write_pipeline_summary(path: Path, summary: Mapping[str, Any]) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(dict(summary), indent=2, sort_keys=True, default=str), encoding="utf-8")
    return path
