from __future__ import annotations

import h5py
from pathlib import Path


def write_series(
    h5_path: Path,
    key: str,
    series,
    *,
    chunk_len: int = 8192,
) -> None:
    if series is None or len(series) == 0:
        return
    h5_path.parent.mkdir(parents=True, exist_ok=True)
    times = series.index.view("int64")
    values = series.values.astype("float32", copy=False)
    chunks = (min(len(series), chunk_len),)
    with h5py.File(h5_path, "a") as h5:
        group = h5.require_group(f"series/{key}")
        for item in ("time", "value"):
            if item in group:
                del group[item]
        group.create_dataset("time", data=times, compression="gzip", compression_opts=4, shuffle=True, chunks=chunks)
        group.create_dataset("value", data=values, compression="gzip", compression_opts=4, shuffle=True, chunks=chunks)
