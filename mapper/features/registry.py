from __future__ import annotations

from importlib import import_module
from pathlib import Path
from typing import Any

from .plugin_base import FeaturePlugin

_REGISTRY: dict[str, FeaturePlugin] = {}


def register(name: str, plugin_obj: FeaturePlugin) -> None:
    _REGISTRY[name] = plugin_obj


def get_feature(name: str) -> FeaturePlugin:
    if name not in _REGISTRY:
        raise KeyError(f"Feature plugin not found: {name}")
    return _REGISTRY[name]


def try_import_dataset_feature(dataset: str, feature_name: str) -> None:
    module_name = f"mapper.features.datasets.{dataset}.input.features_plugins.{feature_name}"
    try:
        import_module(module_name)
    except ModuleNotFoundError:
        return


def list_dataset_features(dataset: str) -> list[str]:
    root = get_dataset_feature_root(dataset) / "input" / "features_plugins"
    if not root.exists():
        return []
    return sorted(path.stem for path in root.glob("*.py") if path.name != "__init__.py")


def get_dataset_feature_root(dataset: str) -> Path:
    return Path(__file__).resolve().parent / "datasets" / dataset
