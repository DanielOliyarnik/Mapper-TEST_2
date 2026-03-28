from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from .evaluator_registry import load_evaluator
from .stage_manifest import load_stage_manifest


def run_evaluator(name: str, *args: Any, **kwargs: Any) -> Any:
    evaluator_fn = load_evaluator(name)
    return evaluator_fn(*args, **kwargs)


def build_evaluator_request(stage_manifest_path: Path) -> dict[str, Any]:
    manifest_payload = load_stage_manifest(stage_manifest_path)
    manifest_body = manifest_payload.get("manifest", {})
    return {
        "stage_manifest_path": str(stage_manifest_path),
        "stage_name": manifest_body.get("stage_name"),
        "task_name": manifest_body.get("task_name"),
        "status": manifest_body.get("status"),
        "output_refs": manifest_body.get("output_refs", []),
    }


def _persist_evaluator_output(eval_dir: Path, evaluator_name: str, payload: Any) -> Path:
    eval_dir.mkdir(parents=True, exist_ok=True)
    out_path = eval_dir / f"{evaluator_name}.eval.json"
    out_path.write_text(json.dumps(payload, indent=2, sort_keys=True, default=str), encoding="utf-8")
    return out_path


def run_evaluators(stage_manifest: Path, evaluator_names: list[str], eval_dir: Path | None = None) -> list[Path]:
    evaluator_request = build_evaluator_request(stage_manifest)
    output_dir = eval_dir or stage_manifest.parent / "EVAL"
    persisted_paths: list[Path] = []
    for evaluator_name in evaluator_names:
        payload = run_evaluator(evaluator_name, evaluator_request)
        persisted_paths.append(_persist_evaluator_output(output_dir, evaluator_name, payload))
    return persisted_paths
