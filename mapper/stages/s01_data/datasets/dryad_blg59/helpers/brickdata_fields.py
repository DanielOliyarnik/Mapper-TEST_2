from __future__ import annotations

import re
from typing import Any


def _norm_label(label: str) -> str:
    text = str(label or "").strip().replace("_", " ")
    return re.sub(r"\s+", " ", text).upper()


def _match_role_overrides(*, key: str, label: str, brick_class: str | None, brick_class_local: str | None, cfg: dict[str, Any]) -> str | None:
    haystacks = {
        "key": str(key or ""),
        "label": str(label or ""),
        "brick_class": str(brick_class or ""),
        "brick_class_local": str(brick_class_local or ""),
    }
    for rule in list(cfg.get("role_overrides") or []):
        pattern = str(rule.get("pattern") or "").strip()
        role = str(rule.get("role") or "").strip().lower()
        source = str(rule.get("source") or "key").strip()
        if pattern and role and re.search(pattern, haystacks.get(source, haystacks["key"]), flags=re.IGNORECASE):
            return role
    return None


def _match_role_from_rules(*, key: str, label: str, brick_class: str | None, brick_class_local: str | None, cfg: dict[str, Any]) -> str | None:
    local_class = str(brick_class_local or "")
    allow_class_setpoint = bool(cfg.get("allow_class_setpoint", False))
    haystacks = {
        "key": str(key or ""),
        "label": str(label or ""),
        "brick_class": str(brick_class or ""),
        "brick_class_local": str(brick_class_local or ""),
    }
    for rule in list(cfg.get("role_rules") or []):
        pattern = str(rule.get("pattern") or "").strip()
        role = str(rule.get("role") or "").strip()
        source = str(rule.get("source") or "brick_class_local").strip()
        if pattern and role and re.search(pattern, haystacks.get(source, haystacks["brick_class_local"]), flags=re.IGNORECASE):
            return role
    class_roles = cfg.get("class_roles") or {}
    full_token = f"brick:{local_class}" if local_class else ""
    if full_token in class_roles:
        mapped = str(class_roles[full_token]).strip().lower()
        if mapped == "setpoint" and not allow_class_setpoint:
            mapped = ""
        if mapped:
            return mapped
    upper = local_class.upper()
    if "COMMAND" in upper:
        return "actuator"
    if "SENSOR" in upper or "STATUS" in upper:
        return "sensor"
    if "SETPOINT" in upper and allow_class_setpoint:
        return "setpoint"
    return None


def _match_role_fallback(*, key: str, label: str) -> str:
    key_upper = str(key or "").upper()
    label_upper = str(label or "").upper()
    if re.search(r"_COOLING_SP$|_HEATING_SP$|_ECON_STPT_|_PA_STATIC_STPT_|_SAT_SP_", key_upper):
        return "setpoint"
    if re.search(r"_HW_VALVE$|_OADMPR_PCT$|_FAN_SPD$", key_upper):
        return "actuator"
    if "COMMAND" in label_upper or "CMD" in label_upper:
        return "actuator"
    return "sensor"


def _append_unique(values: list[str], additions: list[str]) -> list[str]:
    seen = {str(value) for value in values}
    out = list(values)
    for unit in additions:
        token = str(unit).strip()
        if not token or token in seen:
            continue
        seen.add(token)
        out.append(token)
    return out


def _unit_candidates(*, key: str, label: str, class_local: str | None, brick_class: str | None, cfg: dict[str, Any]) -> list[str]:
    rules = cfg.get("unit_candidates_tokens") or cfg.get("unit_candidates") or {}
    hay = " ".join([str(key or ""), str(label or ""), str(class_local or ""), str(brick_class or "")]).upper()
    out: list[str] = []
    seen: set[str] = set()
    for token, units in rules.items():
        if str(token).upper() not in hay:
            continue
        for unit in (units if isinstance(units, list) else [units]):
            unit_token = str(unit)
            if unit_token and unit_token not in seen:
                seen.add(unit_token)
                out.append(unit_token)
    haystacks = {
        "key": str(key or ""),
        "label": str(label or ""),
        "brick_class": str(brick_class or ""),
        "brick_class_local": str(class_local or ""),
    }
    for rule in list(cfg.get("unit_candidates_overrides") or []):
        source = str(rule.get("source") or "key")
        pattern = str(rule.get("pattern") or "").strip()
        additions = rule.get("add") or []
        if pattern and re.search(pattern, haystacks.get(source, haystacks["key"]), flags=re.IGNORECASE):
            add_list = additions if isinstance(additions, list) else [additions]
            out = _append_unique(out, [str(item) for item in add_list])
    return out


def get_brick_fields(brick_row: dict[str, Any], brick_cfg: dict[str, Any]) -> dict[str, Any]:
    key = str(brick_row.get("key") or "")
    label = _norm_label(str(brick_row.get("label") or key))
    class_local = brick_row.get("brick_class_local")
    brick_class = brick_row.get("brick_class")
    role = _match_role_overrides(
        key=key,
        label=label,
        brick_class=brick_class,
        brick_class_local=class_local,
        cfg=brick_cfg,
    )
    if not role:
        role = _match_role_from_rules(
            key=key,
            label=label,
            brick_class=brick_class,
            brick_class_local=class_local,
            cfg=brick_cfg,
        )
    if not role:
        role = _match_role_fallback(key=key, label=label)
    if not role:
        role = str(brick_cfg.get("default_role") or "sensor").strip().lower()
    candidates = _unit_candidates(
        key=key,
        label=label,
        class_local=class_local,
        brick_class=brick_class,
        cfg=brick_cfg,
    )
    return {
        "label": label,
        "brick_uri": brick_row.get("brick_uri"),
        "brick_class": brick_class,
        "role": role,
        "location": None,
        "unit": None,
        "unit_candidates": " ".join(candidates) if candidates else None,
        "zone_hint": brick_row.get("zone_hint"),
        "equip_hint": brick_row.get("equip_hint"),
        "owner_id": brick_row.get("owner_id"),
        "owner_class": brick_row.get("owner_class"),
        "brick_class_local": class_local,
        "class_tag_hint": brick_row.get("class_tag_hint"),
    }
