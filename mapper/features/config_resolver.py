from __future__ import annotations

from pathlib import Path


def resolve_feature_dataset_root(dataset_id: str) -> Path:
    return Path(__file__).resolve().parent / "datasets" / dataset_id


def resolve_feature_plugin_path(dataset_id: str, feature_name: str) -> Path:
    return resolve_feature_dataset_root(dataset_id) / "input" / "features_plugins" / f"{feature_name}.py"


def resolve_feature_template_path(dataset_id: str, template_name: str | None = None) -> Path:
    name = template_name or "template_1.json"
    return resolve_feature_dataset_root(dataset_id) / "input" / "criteria_templates" / name


def resolve_feature_output_dir(dataset_id: str) -> Path:
    return resolve_feature_dataset_root(dataset_id) / "output" / "features_configs"
