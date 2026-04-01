from __future__ import annotations

import re
from typing import Any, Iterable

import numpy as np
import pandas as pd

from ...helpers.unit_contract import canonicalize_unit, merge_unit_contract
from ..process_base import Payload, ProcessBase


def _normalize_candidates(cands: Iterable[str], units_map: dict[str, str] | None = None) -> list[str]:
    out: list[str] = []
    seen = set()
    u_map = units_map or {}
    for cand in (cands or []):
        unit = canonicalize_unit(cand, u_map)
        if not unit or unit in seen:
            continue
        seen.add(unit)
        out.append(unit)
    return out


def _meta_text(meta: dict[str, Any], field: str) -> str:
    value = meta.get(field)
    if value is None:
        return ""
    if isinstance(value, list):
        return " ".join(str(v) for v in value if v is not None)
    return str(value)


def _extract_meta_candidates(meta: dict[str, Any], units_map: dict[str, str]) -> list[str]:
    raw = meta.get("unit_candidates")
    if isinstance(raw, str):
        vals = [item.strip() for item in re.split(r"[,\|;]", raw) if item.strip()]
    elif isinstance(raw, (list, tuple)):
        vals = list(raw)
    else:
        vals = []
    return _normalize_candidates(vals, units_map)


def _extract_meta_unit(meta: dict[str, Any], units_map: dict[str, str]) -> list[str]:
    unit = canonicalize_unit(meta.get("unit"), units_map)
    return [unit] if unit else []


def _match_text_rule(meta: dict[str, Any], rule: dict[str, Any]) -> bool:
    fields = rule.get("fields")
    if not isinstance(fields, list) or not fields:
        fields = [str(rule.get("field", "label"))]
    mode = str(rule.get("mode", "any")).lower()
    texts = [_meta_text(meta, field) for field in fields]

    pattern = rule.get("regex") or rule.get("pattern")
    if pattern:
        flags = re.IGNORECASE if bool(rule.get("ignore_case", True)) else 0
        regex = re.compile(str(pattern), flags=flags)
        matches = [bool(regex.search(text)) for text in texts]
        return any(matches) if mode != "all" else all(matches)

    contains_any = [str(v) for v in (rule.get("contains_any") or []) if str(v).strip()]
    contains_all = [str(v) for v in (rule.get("contains_all") or []) if str(v).strip()]
    if not contains_any and not contains_all:
        return False
    text_up = " ".join(texts).upper()
    any_ok = True if not contains_any else any(token.upper() in text_up for token in contains_any)
    all_ok = True if not contains_all else all(token.upper() in text_up for token in contains_all)
    return any_ok and all_ok


def _extract_rule_candidates(meta: dict[str, Any], rules: list[dict[str, Any]], units_map: dict[str, str]) -> list[str]:
    out: list[str] = []
    for rule in (rules or []):
        if isinstance(rule, dict) and _match_text_rule(meta, rule):
            out.extend(rule.get("candidates") or [])
    return _normalize_candidates(out, units_map)


def _contract_keyword_candidates(
    meta: dict[str, Any],
    contract_map: dict[str, list[str]],
    fields: list[str],
    units_map: dict[str, str],
) -> list[str]:
    haystack = " ".join(_meta_text(meta, field).upper() for field in (fields or []))
    out: list[str] = []
    for token, cands in (contract_map or {}).items():
        norm = str(token).strip().upper()
        if norm and norm in haystack:
            out.extend(cands or [])
    return _normalize_candidates(out, units_map)


def _series_stats(series: pd.Series) -> dict[str, float]:
    arr = np.asarray(series.values, dtype="float64")
    arr = arr[np.isfinite(arr)]
    if arr.size == 0:
        return {}
    mn = float(arr.min())
    mx = float(arr.max())
    return {"min": mn, "max": mx, "range": mx - mn, "abs_max": float(np.max(np.abs(arr)))}


def _check_stat_conditions(stats: dict[str, float], cond: dict[str, Any]) -> bool:
    if not stats:
        return False
    checks = (
        ("min_gte", lambda s, v: s["min"] >= float(v)),
        ("min_gt", lambda s, v: s["min"] > float(v)),
        ("max_lte", lambda s, v: s["max"] <= float(v)),
        ("max_lt", lambda s, v: s["max"] < float(v)),
        ("range_lte", lambda s, v: s["range"] <= float(v)),
        ("range_gte", lambda s, v: s["range"] >= float(v)),
        ("abs_max_lte", lambda s, v: s["abs_max"] <= float(v)),
        ("abs_max_gte", lambda s, v: s["abs_max"] >= float(v)),
    )
    for key, fn in checks:
        if key in cond and not fn(stats, cond[key]):
            return False
    if "non_negative" in cond:
        want = bool(cond["non_negative"])
        if want != (stats["min"] >= 0.0):
            return False
    return True


def _extract_value_rule_candidates(series: pd.Series, rules: list[dict[str, Any]], units_map: dict[str, str]) -> list[str]:
    stats = _series_stats(series)
    out: list[str] = []
    for rule in (rules or []):
        if not isinstance(rule, dict):
            continue
        cond = rule.get("conditions") or {}
        if isinstance(cond, dict) and _check_stat_conditions(stats, cond):
            out.extend(rule.get("candidates") or [])
    return _normalize_candidates(out, units_map)


def _select_best_unit(
    series: pd.Series,
    candidates: list[str],
    tests: dict[str, Any],
    min_fraction: float,
    select_cfg: dict[str, Any],
) -> tuple[str | None, float, dict[str, float]]:
    if not candidates:
        return None, -1.0, {}
    vals = np.asarray(series.values, dtype="float64")
    mask = np.isfinite(vals)
    if not mask.any():
        return None, -1.0, {}
    abs_for_units = {str(unit).strip().upper() for unit in (select_cfg.get("abs_for_units") or [])}
    per_unit_frac: dict[str, float] = {}
    best_unit, best_frac = None, -1.0
    for unit in candidates:
        cfg = tests.get(unit) or {}
        rng = cfg.get("range")
        if not (isinstance(rng, (list, tuple)) and len(rng) == 2):
            per_unit_frac[unit] = 0.0
            continue
        lo = float(rng[0])
        hi = float(rng[1])
        eval_vals = np.abs(vals) if unit in abs_for_units else vals
        frac = float(((eval_vals >= lo) & (eval_vals <= hi) & mask).sum()) / float(mask.sum())
        per_unit_frac[unit] = frac
        if frac > best_frac:
            best_frac = frac
            best_unit = unit
    if best_unit is None or best_frac < float(min_fraction):
        return None, best_frac, per_unit_frac
    return best_unit, best_frac, per_unit_frac


class Process(ProcessBase):
    def _get_unit_contract(self) -> dict[str, Any]:
        cached = getattr(self, "_unit_contract_cache", None)
        if isinstance(cached, dict):
            return cached
        contract = merge_unit_contract(self.cfg)
        self._unit_contract_cache = contract
        return contract

    def _resolve_unit_contract(self, proc_payload: Payload) -> dict[str, Any]:
        payload_contract = proc_payload.get("unit_contract")
        if isinstance(payload_contract, dict) and payload_contract:
            return payload_contract
        return self._get_unit_contract()

    def _policy_action(self, policy_cfg: dict[str, Any], key: str, message: str) -> bool:
        action = str(policy_cfg.get(key, "raise")).lower().strip()
        if action == "skip":
            return True
        if action == "warn":
            self._emit(f"[unit_selection][warn] {message}")
            return True
        raise RuntimeError(message)

    def apply(self, proc_payload: Payload) -> Payload:
        key = str(proc_payload["key"])
        meta = dict(proc_payload.get("meta") or {})
        series: pd.Series = proc_payload["series"]
        if series is None or series.empty:
            self._policy_action(self.cfg.get("strictness") or {}, "on_empty_series", f"-=== (unit_selection): key={key} has empty series ===-")
            return proc_payload

        contract = self._resolve_unit_contract(proc_payload)
        units_map: dict[str, str] = contract.get("units_map") or {}
        unit_tests: dict[str, Any] = contract.get("tests") or {}
        contract_candidates_map: dict[str, list[str]] = contract.get("unit_candidates_map") or {}
        strict_policy = self.cfg.get("strictness") or {}

        if contract.get("strict", False) and not unit_tests:
            raise RuntimeError("-=== (unit_selection): strict unit_contract enabled but no unit tests were resolved ===-")

        unit_from_meta = canonicalize_unit(meta.get("unit"), units_map)
        if unit_from_meta:
            proc_payload["static"]["unit"] = unit_from_meta
            return proc_payload

        sources = self.cfg.get("candidate_sources") or [
            "meta_unit_candidates",
            "meta_unit",
            "contract_keyword_map",
            "candidate_rules",
            "value_rules",
        ]
        candidate_fields = self.cfg.get("contract_keyword_fields") or ["label", "key", "hierarchy", "class", "brick_class"]
        candidate_rules = self.cfg.get("candidate_rules") or []
        value_rules = self.cfg.get("value_rules") or []

        gathered: list[str] = []
        for source in sources:
            src = str(source).strip().lower()
            if src == "meta_unit_candidates":
                gathered.extend(_extract_meta_candidates(meta, units_map))
            elif src == "meta_unit":
                gathered.extend(_extract_meta_unit(meta, units_map))
            elif src == "contract_keyword_map":
                gathered.extend(_contract_keyword_candidates(meta, contract_candidates_map, candidate_fields, units_map))
            elif src == "candidate_rules":
                gathered.extend(_extract_rule_candidates(meta, candidate_rules, units_map))
            elif src == "value_rules":
                gathered.extend(_extract_value_rule_candidates(series, value_rules, units_map))

        unit_candidates = _normalize_candidates(gathered, units_map)
        if not unit_candidates and self._policy_action(
            strict_policy,
            "on_missing_candidates",
            f"-=== (unit_selection): key={key} has no candidate units after configured sources ===-",
        ):
            return proc_payload

        existing = _extract_meta_candidates(meta, units_map)
        merged = _normalize_candidates(existing + unit_candidates, units_map)
        if merged:
            meta["unit_candidates"] = merged
            proc_payload["meta"] = meta

        tests_enabled = bool(self.cfg.get("filter_candidates_to_tests", True))
        tested_candidates = [unit for unit in unit_candidates if unit in unit_tests] if (tests_enabled and unit_tests) else list(unit_candidates)
        if not tested_candidates and self._policy_action(
            strict_policy,
            "on_no_matching_tests",
            f"-=== (unit_selection): key={key} has candidates but none matched configured unit tests ===-",
        ):
            return proc_payload

        min_fraction = float(self.cfg.get("min_fraction", 0.8))
        select_cfg = self.cfg.get("select_settings") or {}
        best_unit, best_frac, per_unit_frac = _select_best_unit(series, tested_candidates, unit_tests, min_fraction, select_cfg)
        if best_unit is None and self._policy_action(
            strict_policy,
            "on_below_min_fraction",
            (
                "-=== "
                f"(unit_selection): key={key} could not finalize unit; "
                f"best_frac={best_frac:.3f} < min_fraction={min_fraction:.3f}; "
                f"tested={{ {', '.join([f'{u}:{per_unit_frac.get(u, 0.0):.3f}' for u in tested_candidates])} }} "
                "===-"
            ),
        ):
            return proc_payload

        proc_payload["static"]["unit"] = best_unit
        return proc_payload
