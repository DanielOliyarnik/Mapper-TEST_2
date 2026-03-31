from __future__ import annotations

import pandas as pd
import concurrent.futures as cf
import threading
from pathlib import Path
from collections.abc import Iterable, Mapping
from typing import Any, Callable

from .hdf_writer import write_series


SeriesPayload = Mapping[str, pd.Series] | Iterable[tuple[str, pd.Series]] | None


def _normalize_series(series: pd.Series | None) -> pd.Series | None:
    if series is None or len(series) == 0:
        return None
    if not isinstance(series.index, pd.DatetimeIndex):
        index = pd.to_datetime(series.index, utc=True)
    elif series.index.tz is None:
        index = series.index.tz_localize("UTC")
    else:
        index = series.index.tz_convert("UTC")
    normalized = pd.Series(series.values, index=index, name="value")
    normalized = normalized.astype("float32", copy=False).sort_index()
    if normalized.index.has_duplicates:
        normalized = normalized[~normalized.index.duplicated(keep="last")]
    if normalized.empty:
        return None
    return normalized


def _iter_payload_items(payload: SeriesPayload) -> list[tuple[str, pd.Series]]:
    if payload is None:
        return []
    if isinstance(payload, Mapping):
        return [(str(key), series) for key, series in payload.items()]
    return [(str(key), series) for key, series in payload]


def ingest_records(
    read_fn: Callable[[dict[str, Any]], SeriesPayload],
    *,
    records: list[dict[str, Any]],
    h5_path: Path,
    max_workers: int = 1,
    chunk_len: int = 8192,
    reporter: Any | None = None,
    progress: Any | None = None,
    progress_label: str = "ingest",
    progress_unit: str = "records",
) -> int:
    total = len(records)
    if progress is not None:
        progress.start(progress_label, total=total, unit=progress_unit)
    done = 0
    written = 0
    lock = threading.Lock()
    try:
        with cf.ThreadPoolExecutor(max_workers=max(1, int(max_workers))) as executor:
            future_to_record = {executor.submit(read_fn, record): record for record in records}
            for future in cf.as_completed(future_to_record):
                record = future_to_record[future]
                try:
                    payload = future.result()
                except Exception as exc:
                    key = record.get("key") or record.get("csv_path") or "<unknown>"
                    if reporter is not None:
                        reporter.error("read_failed", key=key, error=str(exc))
                    raise RuntimeError(f"FAILED TO READ INGEST RECORD {key!r}") from exc
                for key, raw_series in _iter_payload_items(payload):
                    series = _normalize_series(raw_series)
                    if series is None:
                        continue
                    with lock:
                        write_series(h5_path, key, series, chunk_len=chunk_len)
                    written += 1
                done += 1
                if progress is not None:
                    progress.update(done, total=total, extra={"written": written})
    finally:
        if progress is not None:
            progress.finish(extra={"written": written})
    return written
