from __future__ import annotations

from importlib import import_module
from pathlib import Path
from typing import Any

from .config_loader import load_process_config

_PROCESSES_ROOT = Path(__file__).resolve().parent / "processes"


def list_process_names() -> list[str]:
    names: list[str] = []
    for path in sorted(_PROCESSES_ROOT.iterdir()):
        if not path.is_dir() or path.name.startswith("__"):
            continue
        if (path / "process.py").exists():
            names.append(path.name)
    return names


def _load_named_class(module_name: str, class_name: str) -> type:
    module = import_module(module_name)
    try:
        cls = getattr(module, class_name)
    except AttributeError as exc:
        raise RuntimeError(f"Module '{module_name}' must define class '{class_name}'") from exc
    if not isinstance(cls, type):
        raise TypeError(f"{module_name}.{class_name} is not a class")
    return cls


def load_process_class(process_name: str) -> type:
    return _load_named_class(f"mapper.stages.s02_preprocessing.processes.{process_name}.process", "Process")


def load_trainer_class(process_name: str) -> type | None:
    try:
        return _load_named_class(f"mapper.stages.s02_preprocessing.processes.{process_name}.trainer", "Trainer")
    except ModuleNotFoundError:
        return None


def build_process_pipeline(
    cfg: dict[str, Any],
    dataset_id: str,
    reporter: Any | None = None,
    progress: Any | None = None,
) -> list[tuple[str, Any]]:
    pipeline: list[tuple[str, Any]] = []
    for item in (cfg.get("processes") or []):
        if not isinstance(item, dict):
            continue
        name = str(item["module"]).strip()
        proc_class = load_process_class(name)
        proc_cfg = load_process_config(dataset_id, name, item)
        pipeline.append((name, proc_class(proc_cfg, reporter=reporter, progress=progress)))
    return pipeline


def build_trainers(
    cfg: dict[str, Any],
    dataset_id: str,
    reporter: Any | None = None,
    progress: Any | None = None,
) -> list[tuple[str, Any | None]]:
    trainers: list[tuple[str, Any | None]] = []
    for item in (cfg.get("processes") or []):
        if not isinstance(item, dict):
            continue
        name = str(item["module"]).strip()
        trainer_item = item.get("trainer") or {}
        if not bool(trainer_item.get("enabled", False)):
            trainers.append((name, None))
            continue
        trainer_class = load_trainer_class(name)
        if trainer_class is None:
            raise RuntimeError(f"Process '{name}' enables trainer support but no trainer module exists")
        trainer_cfg = load_process_config(dataset_id, name, trainer_item)
        trainers.append((name, trainer_class(trainer_cfg, reporter=reporter, progress=progress)))
    return trainers
