from __future__ import annotations

from importlib import import_module
from typing import Any

from mapper.stages.common import StageDefinition, StageSpec

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
    stage_def = stage_cls()
    if stage_def.spec.name != stage_name:
        raise ValueError(f"Stage registry mismatch for {stage_name}: {stage_def.spec.name}")
    return stage_def


def list_stage_definitions() -> dict[str, StageDefinition]:
    return {stage_name: load_stage_definition(stage_name) for stage_name in _STAGE_MODULES}


def list_stage_names() -> list[str]:
    return list(_STAGE_MODULES)


def list_stage_specs() -> list[StageSpec]:
    return [load_stage_definition(stage_name).spec for stage_name in get_stage_order()]


def get_stage_order() -> list[str]:
    stage_defs = list_stage_definitions()
    return [stage_name for stage_name, _stage_def in sorted(stage_defs.items(), key=lambda item: item[1].spec.order)]
