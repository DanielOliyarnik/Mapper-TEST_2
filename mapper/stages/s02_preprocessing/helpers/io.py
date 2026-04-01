from __future__ import annotations

from pathlib import Path
from typing import Any

import h5py
import numpy as np
import pandas as pd

from .meta_contract import is_missing, normalize_value

_PREFERRED_META_ORDER = ["otherdata", "metadata", "brickdata"]
_STORE_NAME_MAP = {
    "metadata_store": "metadata",
    "brickdata_store": "brickdata",
    "other_store": "otherdata",
}


def load_ledger(ledger_path: Path) -> pd.DataFrame:
    path = Path(ledger_path)
    if path.suffix.lower() == ".feather":
        return pd.read_feather(path)
    try:
        return pd.read_csv(path)
    except Exception as exc:
        raise ValueError(f"Failed to read upstream ledger '{path}': {exc}") from exc


def resolve_store_paths(ledger_df: pd.DataFrame) -> tuple[Path, Path, dict[str, Path], list[str]]:
    cols = list(ledger_df.columns)
    required = {"key", "inv_store", "ts_store"}
    missing = sorted(required - set(cols))
    if missing:
        raise ValueError(f"Ledger must contain columns: {', '.join(sorted(required))}; missing: {', '.join(missing)}")

    def _get_path(col: str) -> Path:
        series = ledger_df[col].dropna()
        if series.empty:
            raise FileNotFoundError(f"No file path found under ledger column '{col}'")
        return Path(str(series.iloc[0]))

    inv_path = _get_path("inv_store")
    ts_path = _get_path("ts_store")

    discovered_order: list[str] = []
    meta_stores: dict[str, Path] = {}
    for col in cols:
        if not col.endswith("_store") or col in {"inv_store", "ts_store"}:
            continue
        name = _STORE_NAME_MAP.get(col, col[:-6])
        discovered_order.append(name)
        meta_stores[name] = _get_path(col)

    ordered_names = [name for name in _PREFERRED_META_ORDER if name in meta_stores]
    ordered_names.extend(name for name in discovered_order if name not in ordered_names)
    return inv_path, ts_path, meta_stores, ordered_names


def read_store_table(table_path: Path) -> pd.DataFrame:
    path = Path(table_path)
    if path.suffix.lower() == ".feather":
        df = pd.read_feather(path)
    elif path.suffix.lower() in {".parquet", ".pq"}:
        df = pd.read_parquet(path)
    else:
        try:
            df = pd.read_csv(path)
        except Exception as exc:
            raise ValueError(f"Failed to read upstream store '{path}': {exc}") from exc

    if "key" not in df.columns:
        raise ValueError(f"Upstream store '{path}' must contain a 'key' column")

    return (
        df.copy()
        .astype({"key": "string"})
        .dropna(subset=["key"])
        .drop_duplicates(subset=["key"])
        .reset_index(drop=True)
    )


def open_ts_store(ts_path: Path) -> h5py.File:
    return h5py.File(str(ts_path), "r")


def extract_period(cfg: dict[str, Any]) -> tuple[pd.Timestamp, pd.Timestamp]:
    period = ((cfg or {}).get("io") or {}).get("period") or {}
    start_ts = pd.to_datetime((period.get("start") or "").strip(), utc=True, errors="coerce")
    end_ts = pd.to_datetime((period.get("end") or "").strip(), utc=True, errors="coerce")
    return start_ts, end_ts


def read_series_for_key(
    h5: h5py.File,
    key: str,
    start: pd.Timestamp | str | None = None,
    end: pd.Timestamp | str | None = None,
) -> pd.Series | None:
    group = h5.get(f"series/{key}")
    if group is None:
        return None

    values = np.asarray(group["value"], dtype="float32")
    times = np.asarray(group["time"], dtype="int64")
    if values.size == 0 or times.size == 0:
        return None

    mx = int(times.max())
    if mx >= 1_000_000_000_000_000_000:
        unit_norm = "ns"
        div_norm = 1
    elif mx >= 1_000_000_000_000_000:
        unit_norm = "us"
        div_norm = 1_000
    elif mx >= 1_000_000_000_000:
        unit_norm = "ms"
        div_norm = 1_000_000
    else:
        unit_norm = "s"
        div_norm = 1_000_000_000

    if not bool(np.all(times[1:] >= times[:-1])):
        order = np.argsort(times)
        times = times[order]
        values = values[order]

    def _norm_select(ts: pd.Timestamp | str | None) -> int | None:
        if ts is None:
            return None
        if not isinstance(ts, pd.Timestamp):
            ts = pd.to_datetime(ts, utc=True, errors="coerce")
        if ts is None or pd.isna(ts):
            return None
        return int(ts.value // div_norm)

    start_norm = _norm_select(start)
    end_norm = _norm_select(end)
    left = 0 if start_norm is None else int(np.searchsorted(times, start_norm, side="left"))
    right = times.size if end_norm is None else int(np.searchsorted(times, end_norm, side="right"))
    if right <= left:
        return None

    index = pd.to_datetime(times[left:right], unit=unit_norm, utc=True)
    series = pd.Series(values[left:right], index=index, dtype="float32")
    if series.empty:
        return None
    return series.sort_index()


def build_meta_lookup(
    store_frames: dict[str, pd.DataFrame],
    lookup_order: list[str],
    selected_fields: list[str],
    contract: dict[str, Any],
) -> dict[str, dict[str, Any]]:
    lookup_table: dict[str, dict[str, Any]] = {}
    all_keys: set[str] = set()
    for df in store_frames.values():
        if df is not None and not df.empty:
            all_keys.update(df["key"].astype("string").tolist())

    for key in all_keys:
        merged_meta: dict[str, Any] = {}
        for name in lookup_order:
            df = store_frames.get(name)
            if df is None or df.empty:
                continue
            row = df.loc[df["key"] == key]
            if row.empty:
                continue
            row_dict = row.iloc[0].to_dict()
            for field in selected_fields:
                if field not in row_dict:
                    continue
                value = normalize_value(field, row_dict[field], contract)
                if is_missing(value, contract):
                    continue
                if field not in merged_meta:
                    merged_meta[field] = value
        lookup_table[str(key)] = merged_meta
    return lookup_table
