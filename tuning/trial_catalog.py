from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Mapping


def record_trial(catalog_path: str | Path, payload: Mapping[str, Any]) -> Path:
    path = Path(catalog_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(dict(payload), sort_keys=True) + "\n")
    return path
