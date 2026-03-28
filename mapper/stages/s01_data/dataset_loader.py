from __future__ import annotations

from typing import Any

from .datasets.dryad_blg59.dataset import DryadBlg59Dataset

DATASET_REGISTRY = {
    "dryad_blg59": DryadBlg59Dataset,
}


def load_dataset(name: str, cfg: dict[str, Any], reporter: Any | None = None):
    if name not in DATASET_REGISTRY:
        raise KeyError(f"Unknown Stage 1 dataset: {name}")
    dataset_cls = DATASET_REGISTRY[name]
    if reporter is not None:
        reporter.info("resolved dataset", dataset=name, dataset_class=dataset_cls.__name__)
    _ = cfg
    return dataset_cls()
