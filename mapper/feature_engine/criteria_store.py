from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from .config_resolver import load_feature_template, resolve_feature_config_path


def write_feature_criteria(dataset_id: str, feature_name: str, criteria: dict[str, Any]) -> Path:
    path = resolve_feature_config_path(dataset_id, feature_name)
    path.write_text(json.dumps(criteria, indent=2, sort_keys=True), encoding="utf-8")
    return path


def read_feature_criteria(dataset_id: str, feature_name: str, *, use_template_fallback: bool) -> dict[str, Any]:
    path = resolve_feature_config_path(dataset_id, feature_name)
    if path.exists():
        loaded = json.loads(path.read_text(encoding="utf-8"))
        if not isinstance(loaded, dict):
            raise TypeError(f"Feature criteria root must be an object: {path}")
        return loaded
    if use_template_fallback:
        template = load_feature_template(dataset_id)
        template_cfg = template.get(feature_name)
        if isinstance(template_cfg, dict):
            return dict(template_cfg)
    return {}
