from __future__ import annotations

from typing import Any

import pandas as pd

from ..meta_predicates import eval_predicate
from ..process_base import Payload, ProcessBase


def _legacy_default(cfg: dict[str, Any]) -> dict[str, Any]:
    method = str(cfg.get("method") or "rolling_mean").strip().lower()
    params = {
        "window": int(cfg.get("window", 9)),
        "center": bool(cfg.get("center", True)),
        "min_periods": int(cfg.get("min_periods", 1)),
    }
    return {"method": method, "params": params}


def _normalize_policy(raw: dict[str, Any], fallback_cfg: dict[str, Any]) -> dict[str, Any]:
    raw = raw if isinstance(raw, dict) else {}
    default_raw = raw.get("default") if isinstance(raw.get("default"), dict) else _legacy_default(fallback_cfg)
    default_method = str(default_raw.get("method") or fallback_cfg.get("method") or "rolling_mean").strip().lower()
    default_params = dict(default_raw.get("params") or {})
    if "window" not in default_params and "window" in fallback_cfg:
        default_params["window"] = int(fallback_cfg.get("window"))
    if "center" not in default_params and "center" in fallback_cfg:
        default_params["center"] = bool(fallback_cfg.get("center"))
    if "min_periods" not in default_params and "min_periods" in fallback_cfg:
        default_params["min_periods"] = int(fallback_cfg.get("min_periods"))

    selectors_out: list[dict[str, Any]] = []
    for sel in (raw.get("selectors") or []):
        if not isinstance(sel, dict):
            continue
        selectors_out.append(
            {
                "id": str(sel.get("id") or f"selector_{len(selectors_out)}"),
                "when": sel.get("when"),
                "method": str(sel.get("method") or default_method).strip().lower(),
                "params": dict(sel.get("params") or {}),
            }
        )
    return {"default": {"method": default_method, "params": default_params}, "selectors": selectors_out}


def _apply_method(series: pd.Series, method: str, params: dict[str, Any]) -> pd.Series:
    method = str(method or "rolling_mean").strip().lower()
    if method in {"none", "identity", "pass"}:
        return series
    if method == "rolling_mean":
        return series.rolling(window=int(params.get("window", 9)), center=bool(params.get("center", True)), min_periods=int(params.get("min_periods", 1))).mean()
    if method == "rolling_median":
        return series.rolling(window=int(params.get("window", 9)), center=bool(params.get("center", True)), min_periods=int(params.get("min_periods", 1))).median()
    if method == "ema":
        return series.ewm(span=int(params.get("span", 9)), min_periods=int(params.get("min_periods", 1)), adjust=bool(params.get("adjust", False))).mean()
    return series


class Process(ProcessBase):
    def _resolve_policy(self) -> dict[str, Any]:
        source = self._state if isinstance(self._state, dict) and self._state else self.cfg
        return _normalize_policy(source, self.cfg or {})

    def _select_effective(self, policy: dict[str, Any], context: dict[str, Any]) -> dict[str, Any]:
        default_cfg = dict(policy.get("default") or {})
        eff_method = str(default_cfg.get("method") or "rolling_mean").strip().lower()
        eff_params = dict(default_cfg.get("params") or {})
        for sel in (policy.get("selectors") or []):
            if eval_predicate(context, sel.get("when")):
                eff_method = str(sel.get("method") or eff_method).strip().lower()
                eff_params.update(dict(sel.get("params") or {}))
                break
        return {"method": eff_method, "params": eff_params}

    def apply(self, proc_payload: Payload) -> Payload:
        series: pd.Series = proc_payload["series"]
        if series is None or series.empty:
            return proc_payload
        meta = dict(proc_payload.get("meta") or {})
        static = dict(proc_payload.get("static") or {})
        context = {**meta, **static, "meta": meta, "static": static}
        policy = self._resolve_policy()
        effective = self._select_effective(policy, context)
        proc_payload["series"] = _apply_method(series, effective["method"], effective["params"])
        return proc_payload
