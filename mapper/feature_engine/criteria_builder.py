from __future__ import annotations

from copy import deepcopy
from typing import Any, Callable

from .config_resolver import load_feature_template
from .criteria_store import read_feature_criteria
from .feature_defs import resolve_feature_defs
from .plugin_registry import get_feature, try_import_dataset_feature


def resolve_feature_criteria(
    dataset_id: str,
    feature_def: dict[str, Any],
    *,
    use_template_fallback: bool,
    _read_fn: Callable[[str], Any] | None = None,
    meta_lookup: dict[str, dict[str, Any]] | None = None,
    candidate_keys: list[str] | None = None,
) -> dict[str, Any]:
    feature_name = str(feature_def["name"]).strip()
    try_import_dataset_feature(dataset_id, feature_name)
    plugin = get_feature(feature_name)

    template = load_feature_template(dataset_id)
    template_cfg = dict(template.get(feature_name) or {})
    generated_cfg = read_feature_criteria(dataset_id, feature_name, use_template_fallback=False)
    criteria_overrides = dict(feature_def.get("criteria_overrides") or {})

    base_cfg = _merge_dicts(template_cfg, generated_cfg)
    criteria = plugin.build_criteria(feature_def, base_cfg, criteria_overrides)
    if not isinstance(criteria, dict):
        raise TypeError(f"Feature plugin '{feature_name}' must return an object from build_criteria()")

    if "name" not in criteria:
        criteria["name"] = feature_name
    if feature_def.get("match_fields") is not None:
        criteria["match_fields"] = list(feature_def["match_fields"])
    if feature_def.get("candidate_filter") is not None:
        criteria["candidate_filter"] = deepcopy(feature_def["candidate_filter"])

    calibration_cfg = criteria.get("calibration")
    if calibration_cfg and candidate_keys and meta_lookup is not None:
        candidate_rows = []
        for key in candidate_keys:
            if key in meta_lookup:
                candidate_rows.append({"key": key, **dict(meta_lookup[key])})
        criteria = plugin.calibrate_criteria(criteria, candidate_rows, _read_fn)
        if not isinstance(criteria, dict):
            raise TypeError(f"Feature plugin '{feature_name}' must return an object from calibrate_criteria()")

    if not criteria and use_template_fallback:
        criteria = dict(template_cfg)
    if not criteria:
        raise ValueError(f"Feature '{feature_name}' did not resolve any criteria")
    return criteria


def resolve_feature_criteria_set(
    dataset_id: str,
    feature_defs: list[dict[str, Any]],
    *,
    use_template_fallback: bool,
    _read_fn: Callable[[str], Any] | None = None,
    meta_lookup: dict[str, dict[str, Any]] | None = None,
    candidate_keys: list[str] | None = None,
) -> dict[str, dict[str, Any]]:
    resolved_defs = resolve_feature_defs(
        feature_defs,
        use_template_features=False,
        dataset_id=dataset_id,
        template=load_feature_template(dataset_id),
    )
    out: dict[str, dict[str, Any]] = {}
    for feature_def in resolved_defs:
        feature_name = str(feature_def["name"]).strip()
        out[feature_name] = resolve_feature_criteria(
            dataset_id,
            feature_def,
            use_template_fallback=use_template_fallback,
            _read_fn=_read_fn,
            meta_lookup=meta_lookup,
            candidate_keys=candidate_keys,
        )
    return out


def _merge_dicts(base: dict[str, Any], override: dict[str, Any]) -> dict[str, Any]:
    merged = deepcopy(base)
    for key, value in (override or {}).items():
        if isinstance(merged.get(key), dict) and isinstance(value, dict):
            merged[key] = _merge_dicts(merged[key], value)
        else:
            merged[key] = deepcopy(value)
    return merged
