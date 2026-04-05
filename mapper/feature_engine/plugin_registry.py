from __future__ import annotations

from importlib import import_module
from typing import Any

from .config_resolver import resolve_feature_plugin_dir
from .plugin_base import FeaturePlugin

_REGISTRY: dict[str, FeaturePlugin] = {}


def register(name: str, plugin_obj: FeaturePlugin) -> None:
    token = str(name or "").strip()
    if not token:
        raise ValueError("Feature plugin name must be non-empty")
    _REGISTRY[token] = plugin_obj


def get_feature(name: str) -> FeaturePlugin:
    token = str(name or "").strip()
    if token not in _REGISTRY:
        raise KeyError(f"Feature plugin not found: {token}")
    return _REGISTRY[token]


def try_import_dataset_feature(dataset_id: str, feature_name: str) -> None:
    token = str(feature_name or "").strip()
    if not token:
        raise ValueError("feature_name must be non-empty")
    module_name = f"mapper.feature_engine.datasets.{dataset_id}.input.features_plugins.{token}"
    try:
        import_module(module_name)
    except ModuleNotFoundError as exc:
        if exc.name != module_name:
            raise
        return


def list_dataset_features(dataset_id: str) -> list[str]:
    plugin_dir = resolve_feature_plugin_dir(dataset_id)
    return sorted(path.stem for path in plugin_dir.glob("*.py") if path.name != "__init__.py")
