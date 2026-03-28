from __future__ import annotations

from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any


class DatasetBase(ABC):
    dataset_name: str

    @abstractmethod
    def build_inventory(self, input_dir: Path, cfg: dict[str, Any], out_path: Path):
        raise NotImplementedError

    @abstractmethod
    def ingest_data(self, input_dir: Path, cfg: dict[str, Any], inventory_df: Any, h5_path: Path):
        raise NotImplementedError

    @abstractmethod
    def build_metadata(self, input_dir: Path, cfg: dict[str, Any], inventory_df: Any, out_path: Path):
        raise NotImplementedError

    @abstractmethod
    def build_brickdata(self, input_dir: Path, cfg: dict[str, Any], inventory_df: Any, out_path: Path):
        raise NotImplementedError

    @abstractmethod
    def build_ledger(self, **kwargs: Any):
        raise NotImplementedError
