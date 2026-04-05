from __future__ import annotations

import json
from pathlib import Path
from typing import Any

_ENGINE_ROOT = Path(__file__).resolve().parent
_DATASETS_ROOT = _ENGINE_ROOT / "datasets"


def resolve_feature_dataset_root(dataset_id: str) -> Path:
    dataset_name = str(dataset_id or "").strip()
    if not dataset_name:
        raise ValueError("dataset_id must be non-empty")
    root = _DATASETS_ROOT / dataset_name
    if not root.exists():
        raise FileNotFoundError(f"Feature dataset root not found: {root}")
    return root


def resolve_feature_plugin_dir(dataset_id: str) -> Path:
    path = resolve_feature_dataset_root(dataset_id) / "input" / "features_plugins"
    if not path.exists():
        raise FileNotFoundError(f"Feature plugin directory not found: {path}")
    return path


def resolve_feature_plugin_path(dataset_id: str, feature_name: str) -> Path:
    token = str(feature_name or "").strip()
    if not token:
        raise ValueError("feature_name must be non-empty")
    path = resolve_feature_plugin_dir(dataset_id) / f"{token}.py"
    if not path.exists():
        raise FileNotFoundError(f"Feature plugin not found: {path}")
    return path


def resolve_feature_template_dir(dataset_id: str) -> Path:
    path = resolve_feature_dataset_root(dataset_id) / "input" / "criteria_templates"
    if not path.exists():
        raise FileNotFoundError(f"Feature template directory not found: {path}")
    return path


def resolve_feature_template_path(dataset_id: str, template_name: str | None = None) -> Path:
    if template_name is None:
        return resolve_latest_template_path(dataset_id)
    token = str(template_name or "").strip()
    if not token:
        raise ValueError("template_name must be non-empty when provided")
    path = resolve_feature_template_dir(dataset_id) / token
    if not path.exists():
        raise FileNotFoundError(f"Feature template not found: {path}")
    return path


def resolve_latest_template_path(dataset_id: str) -> Path:
    template_dir = resolve_feature_template_dir(dataset_id)
    candidates = sorted(template_dir.glob("template_*.json"))
    if not candidates:
        raise FileNotFoundError(f"No template_*.json found in {template_dir}")
    return candidates[-1]


def resolve_feature_output_dir(dataset_id: str) -> Path:
    path = resolve_feature_dataset_root(dataset_id) / "output" / "features_configs"
    path.mkdir(parents=True, exist_ok=True)
    return path


def resolve_feature_config_path(dataset_id: str, feature_name: str) -> Path:
    token = str(feature_name or "").strip()
    if not token:
        raise ValueError("feature_name must be non-empty")
    return resolve_feature_output_dir(dataset_id) / f"{token}.json"


def load_feature_template(dataset_id: str, template_name: str | None = None) -> dict[str, Any]:
    path = resolve_feature_template_path(dataset_id, template_name)
    loaded = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(loaded, dict):
        raise TypeError(f"Feature template root must be an object: {path}")
    return loaded


def resolve_feature_config_source(dataset_id: str, feature_name: str, *, use_template_fallback: bool) -> tuple[str, Path | None]:
    path = resolve_feature_config_path(dataset_id, feature_name)
    if path.exists():
        return "generated", path
    if use_template_fallback:
        return "template", resolve_feature_template_path(dataset_id)
    return "missing", None
