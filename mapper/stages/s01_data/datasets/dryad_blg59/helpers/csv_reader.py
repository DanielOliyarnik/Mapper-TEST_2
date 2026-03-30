from __future__ import annotations

import pandas as pd
from pathlib import Path
from typing import Any

from ....helpers.hdf_writer import write_series


def _to_utc_index(ts, timezone: str, dt_format: str | None):
    parsed = pd.to_datetime(ts, format=dt_format, errors="coerce")
    parsed_idx = pd.DatetimeIndex(parsed)
    if parsed_idx.tz is None:
        if timezone and timezone.upper() != "UTC":
            parsed_idx = parsed_idx.tz_localize(timezone, ambiguous="infer", nonexistent="shift_forward")
        else:
            parsed_idx = parsed_idx.tz_localize("UTC")
    return parsed_idx.tz_convert("UTC")


def ingest_csv_files(
    *,
    input_dir: Path,
    ingest_cfg: dict[str, Any],
    inventory_df,
    h5_path: Path,
    chunk_len: int,
) -> int:
    inventory = inventory_df.copy()
    inventory["key"] = inventory["key"].astype("string")
    inventory["source_file"] = inventory["source_file"].astype("string")
    keys_by_file: dict[str, list[str]] = {}
    for row in inventory.to_dict(orient="records"):
        keys_by_file.setdefault(str(row["source_file"]), []).append(str(row["key"]))

    written = 0
    value_dtype = str(ingest_cfg["value_dtype"])

    for spec in list(ingest_cfg["files"]):
        csv_path = Path(input_dir) / str(spec["path"]).strip()
        if not csv_path.exists():
            raise FileNotFoundError(f"dryad_blg59 timeseries file not found: {csv_path}")
        wanted = keys_by_file.get(str(csv_path), [])
        if not wanted:
            continue
        time_col = str(spec["time_column"])
        timezone = str(spec["timezone"])
        dt_format = spec["datetime_format"]
        usecols = [time_col] + [key for key in wanted if key != time_col]
        df = pd.read_csv(csv_path, usecols=lambda column: column in usecols)
        if time_col not in df.columns:
            raise ValueError(f"dryad_blg59 time column {time_col!r} missing in {csv_path}")
        time_index = _to_utc_index(df[time_col], timezone=timezone, dt_format=dt_format)
        valid_mask = ~time_index.isna()
        time_index = time_index[valid_mask]
        for key in wanted:
            if key not in df.columns:
                continue
            values = pd.to_numeric(df[key], errors="coerce")
            values = values[valid_mask].astype(value_dtype, copy=False)
            series = pd.Series(values.values, index=time_index, name="value").dropna()
            if series.empty:
                continue
            if not series.index.is_monotonic_increasing:
                series = series.sort_index()
            if series.index.has_duplicates:
                series = series[~series.index.duplicated(keep="last")]
            if series.empty:
                continue
            write_series(h5_path=h5_path, key=str(key), series=series, chunk_len=chunk_len)
            written += 1
    return written
