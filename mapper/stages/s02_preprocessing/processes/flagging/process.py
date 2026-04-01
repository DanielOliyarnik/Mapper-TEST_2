from __future__ import annotations

from typing import Any

import numpy as np
import pandas as pd

from ..meta_predicates import eval_predicate
from ..process_base import Payload, ProcessBase


def _rule_enabled(rule: dict[str, Any]) -> bool:
    return bool(rule.get("enabled", True))


def _to_series_mask(mask: np.ndarray, index: pd.Index) -> pd.Series:
    return pd.Series(mask.astype(bool), index=index)


def _rule_finite(series: pd.Series, params: dict[str, Any]) -> pd.Series:
    vals = np.asarray(series.values, dtype="float64")
    return _to_series_mask(~np.isfinite(vals), series.index)


def _rule_range(series: pd.Series, params: dict[str, Any]) -> pd.Series:
    vals = np.asarray(series.values, dtype="float64")
    violation = np.zeros(len(vals), dtype=bool)
    lo = params.get("lo")
    hi = params.get("hi")
    include_lo = bool(params.get("include_lo", True))
    include_hi = bool(params.get("include_hi", True))
    finite = np.isfinite(vals)
    if lo is not None:
        lo_v = float(lo)
        violation |= finite & ((vals < lo_v) if include_lo else (vals <= lo_v))
    if hi is not None:
        hi_v = float(hi)
        violation |= finite & ((vals > hi_v) if include_hi else (vals >= hi_v))
    return _to_series_mask(violation, series.index)


def _rule_rate_change(series: pd.Series, params: dict[str, Any]) -> pd.Series:
    vals = np.asarray(series.values, dtype="float64")
    delta = np.abs(np.diff(vals, prepend=np.nan))
    violation = np.zeros(len(vals), dtype=bool)
    finite = np.isfinite(delta)
    if params.get("max_abs_delta") is not None:
        violation |= finite & (delta > float(params["max_abs_delta"]))
    if params.get("min_abs_delta") is not None:
        violation |= finite & (delta < float(params["min_abs_delta"]))
    return _to_series_mask(violation, series.index)


def _rule_stale(series: pd.Series, params: dict[str, Any]) -> pd.Series:
    vals = np.asarray(series.values, dtype="float64")
    epsilon = float(params.get("epsilon", 1e-6))
    min_run = max(2, int(params.get("min_run_len", 8)))
    diff = np.abs(np.diff(vals, prepend=np.nan))
    same = np.isfinite(diff) & (diff <= epsilon)
    stale = np.zeros(len(vals), dtype=bool)
    run_start = None
    run_len = 0
    for idx, is_same in enumerate(same):
        if idx == 0:
            continue
        if is_same:
            if run_start is None:
                run_start = idx - 1
                run_len = 2
            else:
                run_len += 1
        else:
            if run_start is not None and run_len >= min_run:
                stale[run_start : run_start + run_len] = True
            run_start = None
            run_len = 0
    if run_start is not None and run_len >= min_run:
        stale[run_start : run_start + run_len] = True
    return _to_series_mask(stale, series.index)


def _rule_outlier(series: pd.Series, params: dict[str, Any]) -> pd.Series:
    vals = np.asarray(series.values, dtype="float64")
    finite = np.isfinite(vals)
    violation = np.zeros(len(vals), dtype=bool)
    if not finite.any():
        return _to_series_mask(violation, series.index)
    method = str(params.get("method", "zscore")).strip().lower()
    threshold = float(params.get("threshold", 4.0))
    clean = vals[finite]
    if method == "iqr":
        q1 = float(np.nanpercentile(clean, 25))
        q3 = float(np.nanpercentile(clean, 75))
        iqr = q3 - q1
        if iqr <= 0:
            return _to_series_mask(violation, series.index)
        lo = q1 - threshold * iqr
        hi = q3 + threshold * iqr
        violation |= finite & ((vals < lo) | (vals > hi))
    else:
        med = float(np.nanmedian(clean))
        mad = float(np.nanmedian(np.abs(clean - med)))
        if mad <= 0:
            return _to_series_mask(violation, series.index)
        robust_z = 0.6745 * (vals - med) / mad
        violation |= finite & (np.abs(robust_z) > threshold)
    return _to_series_mask(violation, series.index)


def _compute_rule_mask(series: pd.Series, rule: dict[str, Any]) -> pd.Series:
    rule_type = str(rule.get("type", "finite")).strip().lower()
    params = dict(rule.get("params") or {})
    if rule_type == "finite":
        return _rule_finite(series, params)
    if rule_type == "range":
        return _rule_range(series, params)
    if rule_type in {"rate_change", "rate"}:
        return _rule_rate_change(series, params)
    if rule_type in {"stale", "flatline"}:
        return _rule_stale(series, params)
    if rule_type == "outlier":
        return _rule_outlier(series, params)
    return pd.Series(False, index=series.index)


class Process(ProcessBase):
    def apply(self, proc_payload: Payload) -> Payload:
        series: pd.Series = proc_payload["series"]
        if series is None or series.empty:
            raise ValueError("-=== (flagging): Need timeseries, received either empty or None timeseries ===-")
        meta = dict(proc_payload.get("meta") or {})
        static = dict(proc_payload.get("static") or {})
        context = {**meta, **static, "meta": meta, "static": static}
        cfg = self.cfg or {}
        rules = cfg.get("rules") or [{"name": "finite", "type": "finite", "action": "drop"}]
        flag = pd.Series(np.ones(len(series), dtype="int8"), index=series.index)
        series_out = series.astype("float32", copy=False)
        for rule in rules:
            if not isinstance(rule, dict) or not _rule_enabled(rule):
                continue
            if not eval_predicate(context, rule.get("apply_if")):
                continue
            mask = _compute_rule_mask(series_out, rule)
            if mask is None or mask.empty or not bool(mask.any()):
                continue
            action = str(rule.get("action", "mark_only")).strip().lower()
            if action not in {"mark_only", "drop", "warn"}:
                action = "mark_only"
            flag.loc[mask] = 0
            if action in {"drop", "warn"}:
                series_out = series_out.mask(mask)
            if action == "warn":
                name = str(rule.get("name") or rule.get("type") or "rule")
                self._emit(f"[flagging][warn] rule={name} key={proc_payload.get('key')} flagged={int(mask.sum())}")
        fill_strategy = str(cfg.get("fillna_strategy", "constant")).strip().lower()
        if fill_strategy == "ffill":
            series_out = series_out.ffill()
        elif fill_strategy == "bfill":
            series_out = series_out.bfill()
        elif fill_strategy != "none":
            series_out = series_out.fillna(float(cfg.get("fillna_value", 0.0)))
        proc_payload["flag"] = flag.astype("int8")
        proc_payload["series"] = series_out.astype("float32", copy=False)
        return proc_payload
