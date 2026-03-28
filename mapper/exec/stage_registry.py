from __future__ import annotations

from importlib import import_module
from typing import Any

from mapper.stages.common import StageDefinition

_STAGE_MODULES: dict[str, str] = {
    "s01_data": "mapper.stages.s01_data.stage",
    "s02_preprocessing": "mapper.stages.s02_preprocessing.stage",
    "s03_embedding": "mapper.stages.s03_embedding.stage",
    "s04_similarity": "mapper.stages.s04_similarity.stage",
    "s05_gnn": "mapper.stages.s05_gnn.stage",
    "s07_surrogates": "mapper.stages.s07_surrogates.stage",
}


def _load_stage_class(module_path: str) -> Any:
    module = import_module(module_path)
    if not hasattr(module, "Stage"):
        raise RuntimeError(f"Stage module missing Stage class: {module_path}")
    return getattr(module, "Stage")


def load_stage_definition(stage_name: str) -> StageDefinition:
    if stage_name not in _STAGE_MODULES:
        raise KeyError(f"Unknown stage: {stage_name}")
    stage_cls = _load_stage_class(_STAGE_MODULES[stage_name])
    return stage_cls()


def list_stage_definitions() -> dict[str, StageDefinition]:
    return {stage_name: load_stage_definition(stage_name) for stage_name in _STAGE_MODULES}
