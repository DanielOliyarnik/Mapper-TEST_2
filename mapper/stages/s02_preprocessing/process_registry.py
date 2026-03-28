from __future__ import annotations

from importlib import import_module
from pathlib import Path

_PROCESS_MODULES = {
    "align_clean": "mapper.stages.s02_preprocessing.processes.align_clean.process",
    "unit_selection": "mapper.stages.s02_preprocessing.processes.unit_selection.process",
    "smoothing_filtering": "mapper.stages.s02_preprocessing.processes.smoothing_filtering.process",
    "flagging": "mapper.stages.s02_preprocessing.processes.flagging.process",
    "features_constructor": "mapper.stages.s02_preprocessing.processes.features_constructor.process",
    "static_encode": "mapper.stages.s02_preprocessing.processes.static_encode.process",
}

_TRAINER_MODULES = {
    "smoothing_filtering": "mapper.stages.s02_preprocessing.processes.smoothing_filtering.trainer",
    "features_constructor": "mapper.stages.s02_preprocessing.processes.features_constructor.trainer",
}


def list_process_names() -> list[str]:
    return list(_PROCESS_MODULES)


def load_process(name: str):
    if name not in _PROCESS_MODULES:
        raise KeyError(f"Unknown Stage 2 process: {name}")
    return import_module(_PROCESS_MODULES[name])


def load_trainer(name: str):
    if name not in _TRAINER_MODULES:
        return None
    return import_module(_TRAINER_MODULES[name])


def get_process_root(name: str) -> Path:
    return Path(__file__).resolve().parent / "processes" / name


def describe_process_chain(process_chain: list[str]) -> dict[str, object]:
    return {
        "process_chain": list(process_chain),
        "num_processes": len(process_chain),
        "available_processes": list_process_names(),
    }
