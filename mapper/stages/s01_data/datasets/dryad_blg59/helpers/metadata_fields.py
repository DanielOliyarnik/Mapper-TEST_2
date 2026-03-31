from __future__ import annotations

import re
from typing import Any


ROLE_ORDER = ["setpoint", "actuator", "sensor"]
ROLE_HINTS = {
    "setpoint": [r"_cooling_sp$", r"_heating_sp$", r"_econ_stpt_", r"_pa_static_stpt_", r"_sat_sp_", r"_sat_sp$"],
    "actuator": [r"_hw_valve$", r"_oadmpr_pct$", r"_fan_spd$"],
    "sensor": [r"_temp$", r"_co2$", r"_flow", r"plenum_press", r"_fbk_"],
}


def _title_label(key: str) -> str:
    return re.sub(r"\s+", " ", str(key).replace("_", " ").strip()).upper()


def _match_role(key: str, role_hint: str | None) -> str:
    if role_hint:
        return role_hint
    for role in ROLE_ORDER:
        if any(re.search(pattern, key, flags=re.IGNORECASE) for pattern in ROLE_HINTS[role]):
            return role
    return "sensor"


def _unit_candidates_for_key(*, key: str, source_group: str, meta_cfg: dict[str, Any]) -> list[str]:
    rules = dict(meta_cfg["source_unit_rules"])
    group_cfg = dict(rules[source_group]) if source_group in rules else {}
    exact_units = dict(group_cfg["exact_units"]) if "exact_units" in group_cfg else {}
    candidates = [str(value) for value in list(group_cfg["default_candidates"])] if "default_candidates" in group_cfg else []
    unit = None
    for pattern, value in exact_units.items():
        if re.search(str(pattern), key, flags=re.IGNORECASE):
            unit = str(value)
            break
    out: list[str] = []
    seen: set[str] = set()
    for item in ([unit] if unit else []) + candidates:
        token = str(item).strip()
        if not token or token in seen:
            continue
        seen.add(token)
        out.append(token)
    return out


def get_metadata_fields(meta_row: dict[str, Any], meta_cfg: dict[str, Any]) -> dict[str, Any]:
    key = str(meta_row.get("key") or "").strip()
    source_group = str(meta_row.get("source_group") or "").strip()
    role_hint = str(meta_row.get("role_hint") or "").strip() or None
    role = _match_role(key, role_hint)
    candidates = _unit_candidates_for_key(key=key, source_group=source_group, meta_cfg=meta_cfg)
    unit = candidates[0] if candidates else None
    location = meta_row.get("location_hint")
    if location is not None and str(location).strip() == "":
        location = None
    return {
        "label": _title_label(str(meta_row.get("label_seed") or key)),
        "role": role,
        "location": str(location).strip() if location else None,
        "unit": unit,
        "unit_candidates": " ".join(candidates) if candidates else None,
        "source_file": str(meta_row.get("source_file") or "").strip() or None,
        "source_group": source_group or None,
        "source_unit_rule": str(meta_row.get("source_unit_rule") or "").strip() or None,
        "zone_hint": meta_row.get("zone_hint"),
        "equip_hint": meta_row.get("equip_hint"),
        "location_hint": location,
        "class_hint": meta_row.get("class_hint"),
    }
