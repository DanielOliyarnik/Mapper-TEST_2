from __future__ import annotations

from typing import Any

from .datasets.dryad_blg59.dataset import DryadBlg59Dataset

DATASET_REGISTRY = {
    "dryad_blg59": DryadBlg59Dataset,
}


def load_dataset(name: str, cfg: dict[str, Any]):
    if name not in DATASET_REGISTRY:
        raise KeyError(f"Unknown Stage 1 dataset: {name}")
    return DATASET_REGISTRY[name]()
