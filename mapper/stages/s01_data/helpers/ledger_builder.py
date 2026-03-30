from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING, Iterable

if TYPE_CHECKING:
    import pandas as pd


def _validate_keys(
    *,
    inventory_df: "pd.DataFrame",
    h5_path: Path,
    other_df_list: Iterable[tuple[str, "pd.DataFrame", Path]],
) -> None:
    import h5py

    def _keys(df: "pd.DataFrame", name: str) -> set[str]:
        if "key" not in df.columns:
            raise ValueError(f"{name} is missing 'key'")
        return set(df["key"].astype("string"))

    inventory_keys = _keys(inventory_df, "inventory_df")
    ts_keys: set[str] = set()
    if h5_path.exists():
        with h5py.File(h5_path, "r") as h5:
            series_group = h5.get("series")
            if series_group is not None:
                ts_keys = {str(key) for key in series_group.keys()}
    ts_missing = inventory_keys - ts_keys
    ts_excess = ts_keys - inventory_keys
    if ts_missing or ts_excess:
        raise ValueError(
            f"Key validation (timeseries) failed: missing={len(ts_missing)} excess={len(ts_excess)}"
        )
    for name, df, _ in other_df_list:
        other_keys = _keys(df, f"{name}_df")
        other_excess = other_keys - inventory_keys
        if other_excess:
            raise ValueError(f"Key validation ({name}) failed: excess={len(other_excess)}")


def build_ledger(
    *,
    inventory_df: "pd.DataFrame",
    inventory_store_path: Path,
    ts_store_path: Path,
    otherdata_list: Iterable[tuple[str, "pd.DataFrame", Path]],
    out_path: Path,
    validate: bool = True,
) -> "pd.DataFrame":
    import pandas as pd

    keys = (
        inventory_df["key"]
        .astype("string")
        .dropna()
        .drop_duplicates()
        .reset_index(drop=True)
    )
    if validate:
        _validate_keys(
            inventory_df=inventory_df,
            h5_path=ts_store_path,
            other_df_list=otherdata_list,
        )
    data: dict[str, object] = {
        "key": keys,
        "inv_store": str(inventory_store_path),
        "ts_store": str(ts_store_path),
    }
    for name, _, store_path in otherdata_list:
        data[f"{name}_store"] = str(store_path)
    ledger_df = pd.DataFrame(data)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    ledger_df.to_feather(out_path)
    return ledger_df
