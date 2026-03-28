from __future__ import annotations

from pathlib import Path
from typing import Any

from ..dataset_base import DatasetBase


class DryadBlg59Dataset(DatasetBase):
    dataset_name = "dryad_blg59"

    def build_inventory(self, input_dir: Path, cfg: dict[str, Any], out_path: Path):
        raise NotImplementedError("DryadBlg59Dataset.build_inventory is not implemented in the first scaffold")

    def ingest_data(self, input_dir: Path, cfg: dict[str, Any], inventory_df: Any, h5_path: Path):
        raise NotImplementedError("DryadBlg59Dataset.ingest_data is not implemented in the first scaffold")

    def build_metadata(self, input_dir: Path, cfg: dict[str, Any], inventory_df: Any, out_path: Path):
        raise NotImplementedError("DryadBlg59Dataset.build_metadata is not implemented in the first scaffold")

    def build_brickdata(self, input_dir: Path, cfg: dict[str, Any], inventory_df: Any, out_path: Path):
        raise NotImplementedError("DryadBlg59Dataset.build_brickdata is not implemented in the first scaffold")

    def build_ledger(self, **kwargs: Any):
        raise NotImplementedError("DryadBlg59Dataset.build_ledger is not implemented in the first scaffold")
