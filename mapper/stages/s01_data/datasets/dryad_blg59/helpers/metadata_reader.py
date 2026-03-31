from __future__ import annotations

import re
import pandas as pd
from pathlib import Path
from typing import Any


ROLE_HINTS = {
    "setpoint": [r"_cooling_sp$", r"_heating_sp$", r"_econ_stpt_", r"_pa_static_stpt_", r"_sat_sp_", r"_sat_sp$"],
    "actuator": [r"_hw_valve$", r"_oadmpr_pct$", r"_fan_spd$"],
    "sensor": [r"_temp$", r"_co2$", r"_flow", r"plenum_press", r"_fbk_"],
}


def _role_hint(key: str) -> str | None:
    for role, patterns in ROLE_HINTS.items():
        if any(re.search(pattern, key, flags=re.IGNORECASE) for pattern in patterns):
            return role
    return None


def _zone_hint(key: str) -> str | None:
    match = re.search(r"^(zone_[0-9]+)_", key, flags=re.IGNORECASE)
    return match.group(1) if match else None


def _equip_hint(key: str) -> str | None:
    for pattern, template in [
        (r"^rtu_(?P<capture>[0-9]+)_", "RTU_{capture}"),
        (r"^aru_(?P<capture>[0-9]+)_", "ARU_{capture}"),
        (r"^(hp)", "HP"),
        (r"^(ashp)", "ASHP"),
    ]:
        match = re.search(pattern, key, flags=re.IGNORECASE)
        if match:
            captures = match.groupdict()
            if "capture" in captures:
                return template.format(capture=captures["capture"])
            return template
    return None


def read_metadata_inferred(*, input_dir: Path, cfg: dict[str, Any], inventory_df, meta_cfg: dict[str, Any]):
    rows: list[dict[str, Any]] = []
    for row in inventory_df.to_dict(orient="records"):
        key = str(row.get("key") or "").strip()
        if not key:
            continue
        source_group = str(row.get("source_group") or "").strip()
        zone_hint = _zone_hint(key)
        equip_hint = _equip_hint(key)
        rows.append(
            {
                "key": key,
                "label_seed": key,
                "source_file": str(row.get("source_file") or ""),
                "source_group": source_group,
                "source_unit_rule": source_group,
                "role_hint": _role_hint(key),
                "zone_hint": zone_hint,
                "equip_hint": equip_hint,
                "location_hint": zone_hint or equip_hint,
                "class_hint": source_group,
            }
        )
    _ = (input_dir, cfg, meta_cfg)
    return pd.DataFrame.from_records(rows)
