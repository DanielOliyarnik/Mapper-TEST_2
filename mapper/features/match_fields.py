from __future__ import annotations

from typing import Any


def is_missing(value: Any) -> bool:
    return value is None or value == ""


def coerce_match_fields(match_fields: Any, context_name: str = "match_fields") -> list[str]:
    if match_fields is None:
        return []
    if isinstance(match_fields, str):
        values = [item.strip() for item in match_fields.split(",") if item.strip()]
        return values
    if isinstance(match_fields, (list, tuple)):
        return [str(item).strip() for item in match_fields if str(item).strip()]
    raise TypeError(f"{context_name} must be a string or list of strings")


def normalize_feature_specs(raw_features: Any) -> list[dict[str, Any]]:
    if raw_features is None:
        return []
    if isinstance(raw_features, str):
        return [{"name": raw_features}]
    normalized: list[dict[str, Any]] = []
    for item in raw_features:
        if isinstance(item, str):
            normalized.append({"name": item})
        elif isinstance(item, dict):
            normalized.append(dict(item))
        else:
            raise TypeError("Feature specs must be strings or objects")
    return normalized


def normalize_field_value(value: Any, list_joiner: str = "|") -> Any:
    if isinstance(value, (list, tuple)):
        return list_joiner.join(str(item) for item in value)
    return value


def project_match_fields(primary: dict[str, Any], fields: list[str], *, secondary: dict[str, Any] | None = None, list_joiner: str = "|") -> dict[str, Any]:
    projected: dict[str, Any] = {}
    for field in fields:
        if field in primary and not is_missing(primary[field]):
            projected[field] = normalize_field_value(primary[field], list_joiner)
        elif secondary and field in secondary and not is_missing(secondary[field]):
            projected[field] = normalize_field_value(secondary[field], list_joiner)
        else:
            projected[field] = None
    return projected
