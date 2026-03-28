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
