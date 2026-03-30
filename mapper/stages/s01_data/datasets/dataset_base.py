from __future__ import annotations

from abc import ABC, abstractmethod
from pathlib import Path
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    import pandas as pd


class DatasetBase(ABC):
    """
    Formal Stage 1 dataset contract.

    Dataset implementations are responsible for producing the standardized Stage 1
    outputs while keeping dataset-specific parsing, matching, and local config
    ownership inside the dataset package.

    Implementations must write:

    - `inventory.feather`
    - `raw_store.h5`
    - `metadata.feather`
    - `otherdata.feather`
    - `brickdata.feather`
    - `ledger.feather`

    The canonical raw store contract is:

    - `/series/<key>/time`
    - `/series/<key>/value`
    """

    dataset_name: str

    def __init__(self, cfg: dict[str, Any]) -> None:
        self.cfg = cfg

    @abstractmethod
    def build_inventory(
        self,
        input_dir: Path,
        cfg: dict[str, Any],
        out_path: Path,
    ) -> "pd.DataFrame":
        """
        Build and write the canonical inventory table.

        Expected output:
        - one row per stable `key`
        - must write `out_path` as Feather
        """
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
        """
        Ingest per-key timeseries and write the canonical HDF5 store.

        Required store shape:
        - `/series/<key>/time`
        - `/series/<key>/value`

        Return:
        - number of successfully written series
        """
        raise NotImplementedError

    @abstractmethod
    def build_metadata(
        self,
        input_dir: Path,
        cfg: dict[str, Any],
        inventory_df: "pd.DataFrame",
        out_path: Path,
    ) -> "pd.DataFrame":
        """
        Build and write the standardized metadata table.

        Expected output:
        - one row per `key`
        - must write `out_path` as Feather
        """
        raise NotImplementedError

    @abstractmethod
    def build_brickdata(
        self,
        input_dir: Path,
        cfg: dict[str, Any],
        inventory_df: "pd.DataFrame",
        out_path: Path,
    ) -> "pd.DataFrame":
        """
        Build and write the standardized brickdata table.

        Expected output:
        - one row per `key` where available
        - must write `out_path` as Feather
        """
        raise NotImplementedError

    @abstractmethod
    def build_ledger(
        self,
        inventory_df: "pd.DataFrame",
        meta_df: "pd.DataFrame",
        bricks_df: "pd.DataFrame",
        inventory_store_path: Path,
        ts_store_path: Path,
        meta_store_path: Path,
        bricks_store_path: Path,
        out_path: Path,
        validate: bool = True,
    ) -> "pd.DataFrame":
        """
        Build and write the authoritative Stage 1 ledger.

        The ledger format is standardized, but the association logic between
        inventory keys and metadata/brickdata/otherdata evidence remains
        dataset-specific.
        """
        raise NotImplementedError
