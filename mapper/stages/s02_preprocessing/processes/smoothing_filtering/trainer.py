from __future__ import annotations

from typing import Any, Callable, Iterable

import numpy as np
import pandas as pd

from ..meta_predicates import eval_predicate
from ..trainer_base import TrainerBase


def _normalize_train_spec(cfg: dict[str, Any]) -> dict[str, Any]:
    default_raw = cfg.get("default") if isinstance(cfg.get("default"), dict) else {}
    method = str(default_raw.get("method") or cfg.get("method") or "rolling_mean").strip().lower()
    params = dict(default_raw.get("params") or {})
    if "center" not in params and "center" in cfg:
        params["center"] = bool(cfg.get("center"))
    if "min_periods" not in params and "min_periods" in cfg:
        params["min_periods"] = int(cfg.get("min_periods"))
    candidates = dict(default_raw.get("candidates") or {})
    if "window" not in candidates:
        candidates["window"] = list(map(int, cfg.get("candidates", [3, 5, 7, 9, 11])))
    selectors: list[dict[str, Any]] = []
    for raw_sel in (cfg.get("selectors") or []):
        if not isinstance(raw_sel, dict):
            continue
        selectors.append(
            {
                "id": str(raw_sel.get("id") or f"selector_{len(selectors)}"),
                "when": raw_sel.get("when"),
                "method": str(raw_sel.get("method") or method).strip().lower(),
                "params": dict(raw_sel.get("params") or {}),
                "candidates": dict(raw_sel.get("candidates") or {}),
                "sample_keys": raw_sel.get("sample_keys"),
            }
        )
    return {"default": {"method": method, "params": params, "candidates": candidates}, "selectors": selectors}


def _score_rolling_window(
    read_fn: Callable[[str], pd.Series | None],
    keys: list[str],
    window: int,
    center: bool,
    min_periods: int,
) -> float | None:
    mae_sum = 0.0
    n_used = 0
    for key in keys:
        series = read_fn(key)
        if series is None or series.empty:
            continue
        vals = series.values.astype("float64", copy=False)
        mask = np.isfinite(vals)
        if not mask.any():
            continue
        smooth = series.rolling(window=window, center=center, min_periods=min_periods).mean().values
        valid = mask & np.isfinite(smooth)
        if not valid.any():
            continue
        mae_sum += float(np.abs(smooth[valid] - vals[valid]).mean())
        n_used += 1
    if n_used == 0:
        return None
    return mae_sum / n_used


def _sample_keys(keys: list[str], sample_n: int | None) -> list[str]:
    if not sample_n or sample_n <= 0 or len(keys) <= sample_n:
        return list(keys)
    rng = np.random.default_rng(seed=42)
    idx = rng.choice(len(keys), size=int(sample_n), replace=False)
    return [keys[i] for i in sorted(idx.tolist())]


class Trainer(TrainerBase):
    def _train_branch(
        self,
        read_fn: Callable[[str], pd.Series | None],
        keys: list[str],
        method: str,
        params: dict[str, Any],
        candidates: dict[str, Any],
    ) -> dict[str, Any]:
        if method != "rolling_mean":
            return {"method": method, "params": params, "selected_by": "config_only", "candidates_tested": []}
        center = bool(params.get("center", True))
        min_periods = int(params.get("min_periods", 1))
        windows = list(map(int, candidates.get("window", [int(params.get("window", 9))])))
        scores: dict[int, float] = {}
        for idx, window in enumerate(windows, 1):
            self.prog_tick("smoothing_filtering", idx, len(windows))
            score = _score_rolling_window(read_fn, keys, window, center, min_periods)
            if score is not None:
                scores[window] = float(score)
        if not scores:
            fallback_w = int(params.get("window", windows[0] if windows else 9))
            return {"method": method, "params": {**params, "window": fallback_w}, "selected_by": "fallback_no_scores", "candidates_tested": windows}
        best_w = min(scores.keys(), key=lambda window: (scores[window], window))
        return {"method": method, "params": {**params, "window": int(best_w)}, "selected_by": "cv_mae", "candidates_tested": windows}

    def fit(
        self,
        _read_fn: Callable[[str], pd.Series | None],
        keys: Iterable[str],
        meta_lookup: dict[str, dict[str, Any]],
        prev_state: dict[str, Any] | None = None,
        prev_hparams: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        cfg = self.cfg or {}
        read_fn = self.build_read_fn("smoothing_filtering", _read_fn)
        spec = _normalize_train_spec(cfg)
        all_keys = [str(key) for key in keys]
        global_sample = int(cfg.get("sample_keys", 256))
        sampled_all = _sample_keys(all_keys, global_sample)
        default_branch = spec["default"]
        default_model = self._train_branch(read_fn, sampled_all, default_branch["method"], dict(default_branch["params"] or {}), dict(default_branch["candidates"] or {}))
        selector_models: list[dict[str, Any]] = []
        for sel in (spec.get("selectors") or []):
            selector_id = str(sel.get("id") or f"selector_{len(selector_models)}")
            selector_keys: list[str] = []
            for key in all_keys:
                meta = dict(meta_lookup.get(key) or {})
                context = {**meta, "meta": meta}
                if eval_predicate(context, sel.get("when")):
                    selector_keys.append(key)
            if not selector_keys:
                continue
            sample_n = sel.get("sample_keys")
            sampled_selector = _sample_keys(selector_keys, int(sample_n) if sample_n is not None else global_sample)
            model = self._train_branch(
                read_fn,
                sampled_selector,
                str(sel.get("method") or default_model["method"]),
                {**dict(default_model.get("params") or {}), **dict(sel.get("params") or {})},
                dict(sel.get("candidates") or default_branch.get("candidates") or {}),
            )
            selector_models.append(
                {
                    "id": selector_id,
                    "when": sel.get("when"),
                    "method": model["method"],
                    "params": model["params"],
                    "selected_by": model.get("selected_by"),
                    "candidates_tested": model.get("candidates_tested", []),
                }
            )
        return {
            "default": {
                "method": default_model["method"],
                "params": default_model["params"],
                "selected_by": default_model.get("selected_by"),
                "candidates_tested": default_model.get("candidates_tested", []),
            },
            "selectors": selector_models,
        }
