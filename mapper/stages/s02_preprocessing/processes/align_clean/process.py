from __future__ import annotations

from typing import Any

import numpy as np
import pandas as pd
from pandas.tseries.frequencies import to_offset

from ..process_base import Payload, ProcessBase


class Process(ProcessBase):
    def apply(self, proc_payload: Payload) -> Payload | None:
        series: pd.Series = proc_payload["series"]
        if series is None or series.empty:
            raise ValueError("-=== (align_clean): Need timeseries, received either empty or None timeseries ===-")

        cfg = self.cfg or {}
        ts_idx = series.index
        if not isinstance(ts_idx, pd.DatetimeIndex):
            ts_idx = pd.to_datetime(ts_idx, utc=True, errors="coerce")
        elif ts_idx.tz is None:
            ts_idx = ts_idx.tz_localize("UTC")
        else:
            ts_idx = ts_idx.tz_convert("UTC")

        if ts_idx.hasnans:
            mask = ~pd.isna(ts_idx)
            series = pd.Series(series.values[mask], index=ts_idx[mask])
        else:
            series = pd.Series(series.values, index=ts_idx)

        freq = str(cfg.get("resample_freq") or "").strip()
        if not freq:
            raise ValueError("-=== (align_clean): Need to provide resample_freq ===-")
        try:
            _ = to_offset(freq)
        except Exception as exc:
            raise ValueError(f"-=== (align_clean): invalid resample_freq '{freq}': {exc} ===-") from exc

        series = series.sort_index().resample(freq).mean()

        limit_min = float(cfg.get("ffill_limit_minutes", 60))
        try:
            offset = to_offset(freq)
            idx = pd.date_range("2000-01-01", periods=2, freq=offset, tz="UTC")
            step_min = float((idx[1] - idx[0]).total_seconds()) / 60.0
        except Exception:
            step_min = 15.0
        limit_periods = int(max(0, round(limit_min / max(step_min, 1e-9))))
        if limit_periods > 0:
            series = series.ffill(limit=limit_periods)

        bounds = cfg.get("bounds") or {}
        lo = bounds.get("lo")
        hi = bounds.get("hi")
        if lo is not None:
            series = series.where(series >= float(lo))
        if hi is not None:
            series = series.where(series <= float(hi))

        proc_payload["series"] = series.astype("float32")
        if proc_payload["series"].empty:
            return None
        return proc_payload
