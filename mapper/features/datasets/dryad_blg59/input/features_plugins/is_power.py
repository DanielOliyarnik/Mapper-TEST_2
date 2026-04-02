from __future__ import annotations

from typing import Any

from mapper.features.plugin_base import FeaturePlugin, FeatureResult, label_in_list
from mapper.features.registry import register


def _normalize_units(raw: Any) -> list[str]:
    if raw is None:
        return []
    if isinstance(raw, str):
        return [raw.upper()]
    return [str(item).upper() for item in raw]


def _classify_power_kind(unit: str, cfg: dict[str, Any]) -> str:
    electric_terms = [str(item).upper() for item in cfg.get("electric_terms", ["KW", "KWH", "W"])] # Can just be configured via config (most datasets)
    if label_in_list(unit, electric_terms):
        return "electric"
    return "generic"


class IsPower(FeaturePlugin):
    name = "is_power"

    def infer_attributes(self, series, static: dict[str, Any], params: dict[str, Any]) -> dict[str, Any]:
        units = _normalize_units(static.get("unit") or static.get("unit_candidates"))
        first_unit = units[0] if units else ""
        return {"attributes": {"power_kind": _classify_power_kind(first_unit, params)}}

    def match(self, series, static: dict[str, Any], config: dict[str, Any]) -> FeatureResult:
        units = _normalize_units(static.get("unit") or static.get("unit_candidates"))
        matched = any(unit in {"KW", "KWH", "W"} for unit in units)
        return FeatureResult(matched=matched, details={"units": units})


register("is_power", IsPower())
