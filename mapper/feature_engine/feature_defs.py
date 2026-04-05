from __future__ import annotations

from copy import deepcopy
from typing import Any


def resolve_feature_defs(
    raw_features: Any,
    *,
    use_template_features: bool,
    dataset_id: str,
    template: dict[str, Any] | None = None,
) -> list[dict[str, Any]]:
    feature_defs: list[dict[str, Any]] = []
    if raw_features is None:
        raw_items: list[Any] = []
    elif isinstance(raw_features, (str, dict)):
        raw_items = [raw_features]
    elif isinstance(raw_features, list):
        raw_items = list(raw_features)
    else:
        raise TypeError("features must be null, string, object, or list")

    for index, item in enumerate(raw_items):
        feature_def = _coerce_one_feature_def(item)
        _validate_feature_def(feature_def, f"features[{index}]")
        feature_defs.append(feature_def)

    if feature_defs:
        return feature_defs

    if not use_template_features:
        return []

    template_obj = dict(template or {})
    names = [str(name).strip() for name in template_obj.keys() if str(name).strip()]
    if not names:
        raise ValueError(f"No template feature definitions found for dataset '{dataset_id}'")
    return [{"name": name} for name in names]


def _coerce_one_feature_def(raw: Any) -> dict[str, Any]:
    if isinstance(raw, str):
        return {"name": raw.strip()}
    if not isinstance(raw, dict):
        raise TypeError("feature definitions must be strings or objects")
    feature_def = deepcopy(raw)
    if "match_fields" in feature_def and isinstance(feature_def["match_fields"], str):
        feature_def["match_fields"] = [item.strip() for item in feature_def["match_fields"].split(",") if item.strip()]
    if feature_def.get("candidate_filter") is None:
        feature_def.pop("candidate_filter", None)
    if feature_def.get("criteria_overrides") is None:
        feature_def.pop("criteria_overrides", None)
    return feature_def


def _validate_feature_def(feature_def: dict[str, Any], context_name: str) -> None:
    name = str(feature_def.get("name") or "").strip()
    if not name:
        raise ValueError(f"{context_name}.name is required")
    match_fields = feature_def.get("match_fields")
    if match_fields is not None:
        if not isinstance(match_fields, list) or not all(isinstance(item, str) and item.strip() for item in match_fields):
            raise TypeError(f"{context_name}.match_fields must be list[str] when provided")
    candidate_filter = feature_def.get("candidate_filter")
    if candidate_filter is not None and not isinstance(candidate_filter, dict):
        raise TypeError(f"{context_name}.candidate_filter must be an object when provided")
    criteria_overrides = feature_def.get("criteria_overrides")
    if criteria_overrides is not None and not isinstance(criteria_overrides, dict):
        raise TypeError(f"{context_name}.criteria_overrides must be an object when provided")
