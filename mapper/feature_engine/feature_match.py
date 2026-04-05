from __future__ import annotations

import re
from typing import Any, Callable

import pandas as pd

from .plugin_base import FeatureResult
from .plugin_registry import get_feature, try_import_dataset_feature
from .timeseries_metrics import compute_timeseries_metrics


def is_missing(value: Any) -> bool:
    return value is None or value == ""


def build_match_fields(match_fields: Any, context_name: str = "match_fields") -> list[str]:
    if match_fields is None:
        return []
    if isinstance(match_fields, str):
        return [item.strip() for item in match_fields.split(",") if item.strip()]
    if isinstance(match_fields, (list, tuple)):
        return [str(item).strip() for item in match_fields if str(item).strip()]
    raise TypeError(f"{context_name} must be a string or list of strings")


def normalize_field_value(value: Any, list_joiner: str = "|") -> Any:
    if isinstance(value, (list, tuple, set)):
        return list_joiner.join(str(item) for item in value)
    return value


def project_match_fields(
    primary: dict[str, Any],
    fields: list[str],
    *,
    secondary: dict[str, Any] | None = None,
    list_joiner: str = "|",
) -> dict[str, Any]:
    projected: dict[str, Any] = {}
    for field in fields:
        if field in primary and not is_missing(primary[field]):
            projected[field] = normalize_field_value(primary[field], list_joiner)
        elif secondary and field in secondary and not is_missing(secondary[field]):
            projected[field] = normalize_field_value(secondary[field], list_joiner)
        else:
            projected[field] = None
    return projected


def match_feature(
    dataset_id: str,
    feature_name: str,
    *,
    payload: dict[str, Any],
    criteria: dict[str, Any],
    _read_fn: Callable[[str], Any] | None = None,
    list_joiner: str = "|",
) -> FeatureResult:
    try_import_dataset_feature(dataset_id, feature_name)
    plugin = get_feature(feature_name)

    fields = build_match_fields(criteria.get("match_fields") or payload.get("meta_contract_fields"))
    meta_row = dict(payload.get("meta") or {})
    static_row = dict(payload.get("static") or {})
    match_static = project_match_fields(meta_row, fields, secondary=static_row, list_joiner=list_joiner)
    match_static.setdefault("key", payload.get("key"))

    runtime_input = {
        "series": payload.get("series"),
        "flag": payload.get("flag"),
        "payload": payload,
        "_read_fn": _read_fn,
    }
    return plugin.match(runtime_input, match_static, criteria)


def match_resolved_criteria(series_input: Any, static: dict[str, Any], criteria: dict[str, Any]) -> FeatureResult:
    series, flag = _extract_runtime_series(series_input)
    metadata_rules = list(criteria.get("metadata_rules") or [])
    timeseries_rules = list(criteria.get("timeseries_rules") or [])
    logic = str(criteria.get("logic") or "all").strip().lower()
    metadata_results = [_evaluate_metadata_rule(static, rule) for rule in metadata_rules]
    metrics = compute_timeseries_metrics(series, timeseries_rules, flag=flag) if timeseries_rules else {}
    timeseries_results = [_evaluate_timeseries_rule(metrics, rule) for rule in timeseries_rules]

    checks = metadata_results + timeseries_results
    if not checks:
        matched = False
    elif logic == "any":
        matched = any(checks)
    else:
        matched = all(checks)

    details = {
        "logic": logic,
        "metadata_rules": metadata_results,
        "timeseries_rules": timeseries_results,
        "metrics": metrics,
    }
    return FeatureResult(matched=matched, details=details)


def _extract_runtime_series(series_input: Any) -> tuple[pd.Series, pd.Series | None]:
    if isinstance(series_input, dict):
        series = series_input.get("series")
        flag = series_input.get("flag")
    else:
        series = series_input
        flag = None
    if not isinstance(series, pd.Series) or series.empty:
        raise ValueError("Feature matching requires a non-empty series")
    if flag is not None and not isinstance(flag, pd.Series):
        flag = None
    return series, flag


def _evaluate_metadata_rule(static: dict[str, Any], rule: dict[str, Any]) -> bool:
    field = str(rule.get("field") or "").strip()
    if not field:
        raise ValueError("metadata rule requires 'field'")
    op = str(rule.get("op") or "eq").strip().lower()
    value = static.get(field)
    if op == "exists":
        return field in static and not is_missing(value)
    if op == "truthy":
        return bool(value)
    if op == "contains":
        return _contains(value, rule.get("value"))
    if op == "contains_any":
        return any(_contains(value, item) for item in list(rule.get("values") or []))
    if op == "regex":
        pattern = str(rule.get("value") or "")
        return bool(re.search(pattern, str(value or ""), flags=re.IGNORECASE))
    if op == "in":
        return _in_values(value, list(rule.get("values") or []))
    if op == "not_in":
        return not _in_values(value, list(rule.get("values") or []))
    if op == "eq":
        return _normalize_token(value) == _normalize_token(rule.get("value"))
    if op == "ne":
        return _normalize_token(value) != _normalize_token(rule.get("value"))
    raise ValueError(f"Unsupported metadata op: {op}")


def _evaluate_timeseries_rule(metrics: dict[str, Any], rule: dict[str, Any]) -> bool:
    metric_name = str(rule.get("key") or rule.get("metric") or rule.get("name") or "").strip()
    if not metric_name:
        raise ValueError("timeseries rule requires 'metric'")
    actual = metrics.get(metric_name)
    if actual is None:
        return False
    op = str(rule.get("op") or ">=").strip().lower()
    expected = rule.get("value")
    if op == ">":
        return actual > expected
    if op == ">=":
        return actual >= expected
    if op == "<":
        return actual < expected
    if op == "<=":
        return actual <= expected
    if op == "eq":
        return actual == expected
    if op == "ne":
        return actual != expected
    raise ValueError(f"Unsupported timeseries op: {op}")


def _contains(value: Any, search: Any) -> bool:
    return _normalize_token(search) in _normalize_token(value)


def _in_values(value: Any, values: list[Any]) -> bool:
    token = _normalize_token(value)
    token_parts = {part for part in token.split("|") if part}
    allowed = {_normalize_token(item) for item in values}
    return token in allowed or bool(token_parts.intersection(allowed))


def _normalize_token(value: Any) -> str:
    return str(value or "").strip().upper()
