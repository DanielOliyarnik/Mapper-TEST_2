from __future__ import annotations

import concurrent.futures as cf
import threading
from pathlib import Path
from typing import TYPE_CHECKING, Any, Callable

from .hdf_writer import write_series

if TYPE_CHECKING:
    import pandas as pd


def ingest_rows(
    read_fn: Callable[[dict[str, Any]], "pd.Series | None"],
    *,
    rows: "pd.DataFrame",
    h5_path: Path,
    max_workers: int = 1,
    chunk_len: int = 8192,
    reporter: Any | None = None,
) -> int:
    import pandas as pd

    records = rows.to_dict(orient="records")
    total = len(records)
    done = 0
    written = 0
    lock = threading.Lock()
    step = max(1, total // 20) if total else 1
    with cf.ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_record = {executor.submit(read_fn, record): record for record in records}
        for future in cf.as_completed(future_to_record):
            record = future_to_record[future]
            try:
                series = future.result()
            except Exception as exc:
                key = record.get("key")
                if reporter is not None:
                    reporter.error("read_failed", key=key, error=str(exc))
                raise RuntimeError(f"FAILED TO READ ROW FOR KEY {key!r}") from exc
            if series is not None and len(series) > 0:
                if not isinstance(series.index, pd.DatetimeIndex):
                    series.index = pd.to_datetime(series.index, utc=True)
                elif series.index.tz is None:
                    series.index = series.index.tz_localize("UTC")
                else:
                    series.index = series.index.tz_convert("UTC")
                series = series.astype("float32", copy=False).sort_index()
                with lock:
                    write_series(h5_path, str(record["key"]), series, chunk_len=chunk_len)
                written += 1
            done += 1
            if reporter is not None and total and (done == 1 or done % step == 0 or done == total):
                reporter.progress("ingest", completed=done, total=total)
    return written
