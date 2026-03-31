from __future__ import annotations

from abc import ABC, abstractmethod
from pathlib import Path
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    import pandas as pd


class DatasetBase(ABC):
    """
    Formal Stage 1 dataset contract.

    Stage 1 is split into a general orchestration layer and a dataset-specific
    implementation layer. Dataset implementations own dataset-local parsing,
    matching, and config resolution, while the general layer owns the stage
    contract and standardized outputs.

    Stage 1 must write:

    - `inventory.feather`
    - `raw_store.h5`
    - `metadata.feather`
    - `brickdata.feather`
    - `otherdata.feather`
    - `ledger.feather`

    Canonical raw-store contract:

    - `/series/<key>/time`
    - `/series/<key>/value`

    `input_dir` is the raw-data root for the dataset.
    """

    dataset_name: str

    def __init__(self, cfg: dict[str, Any]) -> None:
        self.cfg = cfg
        self.reporter: Any | None = None
        self.progress: Any | None = None
        self._runtime_metrics: dict[str, Any] = {}

    def bind_runtime(self, *, reporter: Any | None = None, progress: Any | None = None) -> None:
        self.reporter = reporter
        self.progress = progress

    def record_metrics(self, **metrics: Any) -> None:
        self._runtime_metrics.update(metrics)

    def pop_runtime_metrics(self) -> dict[str, Any]:
        metrics = dict(self._runtime_metrics)
        self._runtime_metrics.clear()
        return metrics

    @abstractmethod
    def build_inventory(
        self,
        input_dir: Path,
        cfg: dict[str, Any],
        out_path: Path,
    ) -> "pd.DataFrame":
        raise NotImplementedError

    @abstractmethod
    def ingest_data(
        self,
        input_dir: Path,
        cfg: dict[str, Any],
        inventory_df: "pd.DataFrame",
        h5_path: Path,
        max_workers: int = 8,
        chunk_len: int = 8192,
    ) -> int:
        raise NotImplementedError

    @abstractmethod
    def build_metadata(
        self,
        input_dir: Path,
        cfg: dict[str, Any],
        inventory_df: "pd.DataFrame",
        out_path: Path,
    ) -> "pd.DataFrame":
        raise NotImplementedError

    @abstractmethod
    def build_brickdata(
        self,
        input_dir: Path,
        cfg: dict[str, Any],
        inventory_df: "pd.DataFrame",
        out_path: Path,
    ) -> "pd.DataFrame":
        raise NotImplementedError

    @abstractmethod
    def build_otherdata(
        self,
        input_dir: Path,
        cfg: dict[str, Any],
        inventory_df: "pd.DataFrame",
        meta_df: "pd.DataFrame",
        bricks_df: "pd.DataFrame",
        out_path: Path,
    ) -> "pd.DataFrame":
        """
        Build and write the standardized otherdata table.

        Dataset-specific implementations should delegate into the general
        `helpers/otherdata_builder.py` path, the same way metadata and
        brickdata use their general helpers.
        """
        raise NotImplementedError

    @abstractmethod
    def build_ledger(
        self,
        inventory_df: "pd.DataFrame",
        meta_df: "pd.DataFrame",
        bricks_df: "pd.DataFrame",
        other_df: "pd.DataFrame",
        inventory_store_path: Path,
        ts_store_path: Path,
        meta_store_path: Path,
        bricks_store_path: Path,
        other_store_path: Path,
        out_path: Path,
        validate: bool = True,
    ) -> "pd.DataFrame":
        raise NotImplementedError
