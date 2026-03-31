from __future__ import annotations

import pandas as pd
from pathlib import Path
from typing import Any


def _to_utc_index(ts, timezone: str, dt_format: str | None):
    parsed = pd.to_datetime(ts, format=dt_format, errors="coerce")
    parsed_idx = pd.DatetimeIndex(parsed)
    if parsed_idx.tz is None:
        if timezone and timezone.upper() != "UTC":
            parsed_idx = parsed_idx.tz_localize(timezone, ambiguous="infer", nonexistent="shift_forward")
        else:
            parsed_idx = parsed_idx.tz_localize("UTC")
    return parsed_idx.tz_convert("UTC")


def build_csv_ingest_records(
    *,
    input_dir: Path,
    ingest_cfg: dict[str, Any],
    inventory_df: pd.DataFrame,
) -> list[dict[str, Any]]:
    if "key" not in inventory_df.columns or "source_file" not in inventory_df.columns:
        raise ValueError("dryad_blg59 ingest requires inventory_df['key'] and inventory_df['source_file']")
    inventory = inventory_df.loc[:, ["key", "source_file"]].copy()
    inventory["key"] = inventory["key"].astype("string")
    inventory["source_file"] = inventory["source_file"].astype("string")
    keys_by_file: dict[str, list[str]] = {}
    for row in inventory.drop_duplicates().to_dict(orient="records"):
        keys_by_file.setdefault(str(row["source_file"]), []).append(str(row["key"]))

    records: list[dict[str, Any]] = []
    value_dtype = str(ingest_cfg["value_dtype"])
    for spec in ingest_cfg["files"]:
        csv_path = Path(input_dir) / str(spec["path"]).strip()
        if not csv_path.exists():
            raise FileNotFoundError(f"dryad_blg59 timeseries file not found: {csv_path}")
        wanted = keys_by_file.get(str(csv_path), [])
        if not wanted:
            continue
        records.append(
            {
                "csv_path": str(csv_path),
                "keys": list(dict.fromkeys(wanted)),
                "time_column": str(spec["time_column"]),
                "timezone": str(spec["timezone"]),
                "datetime_format": spec["datetime_format"],
                "value_dtype": value_dtype,
            }
        )
    return records


def read_csv_record(record: dict[str, Any]) -> dict[str, pd.Series]:
    csv_path = Path(record["csv_path"])
    time_col = str(record["time_column"])
    timezone = str(record["timezone"])
    dt_format = record["datetime_format"]
    value_dtype = str(record["value_dtype"])
    keys = [str(key) for key in record["keys"] if str(key) != time_col]
    usecols = [time_col] + keys
    df = pd.read_csv(csv_path, usecols=lambda column: column in usecols)
    if time_col not in df.columns:
        raise ValueError(f"dryad_blg59 time column {time_col!r} missing in {csv_path}")
    time_index = _to_utc_index(df[time_col], timezone=timezone, dt_format=dt_format)
    valid_mask = ~time_index.isna()
    time_index = time_index[valid_mask]
    out: dict[str, pd.Series] = {}
    for key in keys:
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
        out[key] = series
    return out
