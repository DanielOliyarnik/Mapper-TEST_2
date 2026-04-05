from __future__ import annotations

from typing import Any

import pandas as pd


def compute_timeseries_metrics(
    series: Any,
    metrics_spec: list[dict[str, Any]] | dict[str, Any],
    *,
    flag: Any | None = None,
) -> dict[str, Any]:
    if not isinstance(series, pd.Series) or series.empty:
        return {}

    specs = _coerce_metrics_spec(metrics_spec)
    metrics: dict[str, Any] = {}
    for spec in specs:
        metric_name = str(spec["metric"]).strip().lower()
        key = str(spec.get("key") or metric_name).strip()
        params = dict(spec.get("params") or {})
        work_series = series
        if bool(spec.get("apply_flag_mask", False)) and isinstance(flag, pd.Series):
            aligned_flag = flag.reindex(series.index).fillna(0)
            work_series = series.loc[aligned_flag.astype(bool) == False]
        if work_series.empty:
            metrics[key] = None
            continue
        if metric_name == "min":
            metrics[key] = float(work_series.min())
        elif metric_name == "max":
            metrics[key] = float(work_series.max())
        elif metric_name in {"mean", "avg", "average"}:
            metrics[key] = float(work_series.mean())
        elif metric_name == "std":
            metrics[key] = float(work_series.std())
        elif metric_name == "abs_mean":
            metrics[key] = float(work_series.abs().mean())
        elif metric_name == "pct_nonzero":
            metrics[key] = float((work_series != 0).mean())
        elif metric_name == "rate_of_change_mean":
            diff = work_series.diff().dropna()
            metrics[key] = float(diff.abs().mean()) if not diff.empty else 0.0
        elif metric_name == "rate_of_change_max":
            diff = work_series.diff().dropna()
            metrics[key] = float(diff.abs().max()) if not diff.empty else 0.0
        elif metric_name == "last":
            metrics[key] = float(work_series.iloc[-1])
        elif metric_name == "first":
            metrics[key] = float(work_series.iloc[0])
        elif metric_name == "range":
            metrics[key] = float(work_series.max() - work_series.min())
        elif metric_name == "quantile":
            q = float(params.get("q", 0.5))
            metrics[key] = float(work_series.quantile(q))
        else:
            raise ValueError(f"Unsupported timeseries metric: {metric_name}")
    return metrics


def _coerce_metrics_spec(metrics_spec: list[dict[str, Any]] | dict[str, Any]) -> list[dict[str, Any]]:
    if isinstance(metrics_spec, dict):
        items = [metrics_spec]
    elif isinstance(metrics_spec, list):
        items = metrics_spec
    else:
        raise TypeError("metrics_spec must be an object or list of objects")

    out: list[dict[str, Any]] = []
    for item in items:
        if not isinstance(item, dict):
            raise TypeError("metrics_spec entries must be objects")
        metric_name = str(item.get("metric") or item.get("name") or "").strip()
        if not metric_name:
            raise ValueError("metrics_spec entries require 'metric'")
        out.append(item)
    return out
