from __future__ import annotations

import importlib
import inspect
from pathlib import Path
from typing import Any

from .datasets.dataset_base import DatasetBase


def resolve_dataset_package(dataset_name: str) -> str:
    token = str(dataset_name or "").strip()
    if not token:
        raise ValueError("Stage 1 dataset name is required")
    return f"mapper.stages.s01_data.datasets.{token}"


def resolve_dataset_root(dataset_name: str) -> Path:
    return Path(__file__).resolve().parent / "datasets" / str(dataset_name).strip()


def list_available_datasets() -> list[str]:
    datasets_root = Path(__file__).resolve().parent / "datasets"
    names: list[str] = []
    for child in sorted(datasets_root.iterdir()):
        if not child.is_dir():
            continue
        if child.name.startswith("__"):
            continue
        if child.name == "__pycache__":
            continue
        if (child / "__init__.py").exists():
            names.append(child.name)
    return names


def _iter_candidate_modules(dataset_name: str) -> list[Any]:
    package_name = resolve_dataset_package(dataset_name)
    package = importlib.import_module(package_name)
    modules = [package]
    try:
        modules.append(importlib.import_module(f"{package_name}.dataset"))
    except ModuleNotFoundError:
        pass
    return modules


def find_dataset_class(module_or_package: Any) -> type[DatasetBase] | None:
    candidates: list[type[DatasetBase]] = []
    preferred = getattr(module_or_package, "Dataset", None)
    if inspect.isclass(preferred) and issubclass(preferred, DatasetBase) and preferred is not DatasetBase:
        candidates.append(preferred)
    for _, obj in inspect.getmembers(module_or_package, inspect.isclass):
        if not issubclass(obj, DatasetBase) or obj is DatasetBase:
            continue
        if obj not in candidates:
            candidates.append(obj)
    if not candidates:
        return None
    if len(candidates) > 1:
        names = ", ".join(sorted(candidate.__name__ for candidate in candidates))
        raise RuntimeError(f"Multiple Stage 1 dataset classes found for module {module_or_package.__name__}: {names}")
    return candidates[0]


def validate_dataset_contract(dataset: object) -> None:
    required_methods = (
        "build_inventory",
        "ingest_data",
        "build_metadata",
        "build_brickdata",
        "build_ledger",
    )
    if not isinstance(dataset, DatasetBase):
        raise TypeError(f"Stage 1 dataset does not implement DatasetBase: {dataset!r}")
    missing = [name for name in required_methods if not callable(getattr(dataset, name, None))]
    if missing:
        raise TypeError(f"Stage 1 dataset is missing required methods: {', '.join(missing)}")


def load_dataset(dataset_name: str, cfg: dict[str, Any], reporter: Any | None = None) -> DatasetBase:
    dataset_root = resolve_dataset_root(dataset_name)
    if not dataset_root.exists():
        available = ", ".join(list_available_datasets()) or "<none>"
        raise FileNotFoundError(f"Unknown Stage 1 dataset {dataset_name!r}; available datasets: {available}")
    dataset_cls: type[DatasetBase] | None = None
    for module in _iter_candidate_modules(dataset_name):
        dataset_cls = find_dataset_class(module)
        if dataset_cls is not None:
            break
    if dataset_cls is None:
        raise RuntimeError(f"No concrete DatasetBase implementation found for Stage 1 dataset {dataset_name!r}")
    dataset = dataset_cls(dict(cfg))
    validate_dataset_contract(dataset)
    _ = reporter
    return dataset
