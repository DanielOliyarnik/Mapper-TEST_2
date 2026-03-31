from __future__ import annotations

import h5py
import pandas as pd
from pathlib import Path


def _validate_df_keys(df: pd.DataFrame, *, name: str, inventory_keys: set[str]) -> None:
    if "key" not in df.columns:
        raise ValueError(f"{name} is missing 'key'")
    keys = set(df["key"].astype("string"))
    excess = keys - inventory_keys
    if excess:
        raise ValueError(f"Key validation ({name}) failed: excess={len(excess)}")


def _validate_timeseries_keys(*, inventory_keys: set[str], h5_path: Path) -> None:
    ts_keys: set[str] = set()
    if h5_path.exists():
        with h5py.File(h5_path, "r") as h5:
            series_group = h5.get("series")
            if series_group is not None:
                ts_keys = {str(key) for key in series_group.keys()}
    missing = inventory_keys - ts_keys
    excess = ts_keys - inventory_keys
    if missing or excess:
        raise ValueError(f"Key validation (timeseries) failed: missing={len(missing)} excess={len(excess)}")


def build_ledger(
    *,
    inventory_df: pd.DataFrame,
    metadata_df: pd.DataFrame,
    brickdata_df: pd.DataFrame,
    otherdata_df: pd.DataFrame,
    inventory_store_path: Path,
    ts_store_path: Path,
    meta_store_path: Path,
    bricks_store_path: Path,
    other_store_path: Path,
    out_path: Path,
    validate: bool = True,
) -> pd.DataFrame:
    inventory_keys = set(inventory_df["key"].astype("string"))
    if validate:
        _validate_timeseries_keys(inventory_keys=inventory_keys, h5_path=ts_store_path)
        _validate_df_keys(metadata_df, name="metadata_df", inventory_keys=inventory_keys)
        _validate_df_keys(brickdata_df, name="brickdata_df", inventory_keys=inventory_keys)
        _validate_df_keys(otherdata_df, name="otherdata_df", inventory_keys=inventory_keys)

    keys = inventory_df["key"].astype("string").dropna().drop_duplicates().reset_index(drop=True)
    ledger_df = pd.DataFrame(
        {
            "key": keys,
            "inv_store": str(inventory_store_path),
            "ts_store": str(ts_store_path),
            "metadata_store": str(meta_store_path),
            "brickdata_store": str(bricks_store_path),
            "other_store": str(other_store_path),
        }
    )
    out_path.parent.mkdir(parents=True, exist_ok=True)
    ledger_df.to_feather(out_path)
    return ledger_df
