from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any


@dataclass(frozen=True)
class FeaturePluginSpec:
    dataset_id: str
    feature_name: str
    module_path: Path


@dataclass(frozen=True)
class FeatureNamespaceSpec:
    dataset_id: str
    root: Path
    input_root: Path
    output_root: Path


@dataclass(frozen=True)
class CriteriaTemplateRef:
    dataset_id: str
    template_name: str
    path: Path


@dataclass(frozen=True)
class FeatureBuildRequest:
    dataset_id: str
    feature_name: str
    trainer_cfg: dict[str, Any]
    output_dir: Path
