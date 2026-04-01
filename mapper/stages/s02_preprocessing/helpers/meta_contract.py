from __future__ import annotations

import json
import math
from copy import deepcopy
from typing import Any, Iterable

DEFAULT_META_CONTRACT: dict[str, Any] = {
    "strict": True,
    "required_fields": [
        "key",
        "role",
        "label",
        "location",
        "hierarchy",
        "unit",
        "unit_candidates",
        "zone_id",
        "group_id",
    ],
    "optional_fields": [
        "loop_ids",
        "equip_id",
        "class",
        "brick_class",
    ],
    "passthrough_mode": "optional_only",
    "list_fields": ["unit_candidates", "loop_ids"],
    "null_like": [None, "", " ", "nan"],
}


def _dedupe_keep_order(values: Iterable[str]) -> list[str]:
    out: list[str] = []
    seen = set()
    for value in values:
        key = str(value).strip()
        if not key or key in seen:
            continue
        seen.add(key)
        out.append(key)
    return out


def _extract_contract_section(cfg: dict[str, Any] | None) -> dict[str, Any]:
    if not isinstance(cfg, dict):
        return {}
    if "meta_contract" in cfg and isinstance(cfg["meta_contract"], dict):
        return dict(cfg["meta_contract"])
    return dict(cfg)


def load_meta_contract(cfg: dict[str, Any] | None) -> dict[str, Any]:
    loaded = deepcopy(DEFAULT_META_CONTRACT)
    user_cfg = _extract_contract_section(cfg)
    for key in (
        "strict",
        "required_fields",
        "optional_fields",
        "passthrough_mode",
        "list_fields",
        "null_like",
    ):
        if key in user_cfg:
            loaded[key] = deepcopy(user_cfg[key])

    loaded["strict"] = bool(loaded.get("strict", True))
    required_fields = _dedupe_keep_order(loaded.get("required_fields") or [])
    optional_fields = _dedupe_keep_order(loaded.get("optional_fields") or [])
    if "key" not in required_fields:
        required_fields = ["key", *required_fields]
    loaded["required_fields"] = required_fields
    loaded["optional_fields"] = [field for field in optional_fields if field != "key"]
    loaded["list_fields"] = _dedupe_keep_order(loaded.get("list_fields") or [])

    mode = str(loaded.get("passthrough_mode") or "optional_only").strip().lower()
    if mode not in {"none", "optional_only", "all"}:
        raise ValueError(f"Invalid meta_contract.passthrough_mode '{mode}'")
    loaded["passthrough_mode"] = mode
    return loaded


def is_missing(value: Any, contract: dict[str, Any]) -> bool:
    if value is None:
        return True
    if isinstance(value, float) and math.isnan(value):
        return True
    if isinstance(value, str):
        val = value.strip().lower()
        null_like = {
            str(item).strip().lower()
            for item in (contract.get("null_like") or [])
            if isinstance(item, str)
        }
        return val in null_like
    if isinstance(value, (list, tuple, set, dict)):
        return len(value) == 0
    return False


def _normalize_list_value(value: Any, contract: dict[str, Any]) -> list[str]:
    if is_missing(value, contract):
        return []
    if isinstance(value, (list, tuple, set)):
        items = list(value)
    elif isinstance(value, str):
        raw = value.strip()
        if raw.startswith("[") and raw.endswith("]"):
            try:
                parsed = json.loads(raw)
                items = parsed if isinstance(parsed, list) else [value]
            except Exception:
                items = [value]
        elif "|" in raw:
            items = raw.split("|")
        elif "," in raw:
            items = raw.split(",")
        elif ";" in raw:
            items = raw.split(";")
        else:
            items = [raw]
    else:
        items = [value]

    out: list[str] = []
    seen = set()
    for item in items:
        if is_missing(item, contract):
            continue
        token = str(item).strip()
        if not token or token in seen:
            continue
        seen.add(token)
        out.append(token)
    return out


def normalize_value(field: str, value: Any, contract: dict[str, Any]) -> Any:
    if is_missing(value, contract):
        return None
    if field in set(contract.get("list_fields") or []):
        return _normalize_list_value(value, contract)
    return value


def discover_store_columns(store_dfs: dict[str, Any]) -> dict[str, list[str]]:
    discovered: dict[str, list[str]] = {}
    for name, df in (store_dfs or {}).items():
        discovered[str(name)] = [] if df is None else list(df.columns)
    return discovered


def resolve_fields(contract: dict[str, Any], discovered: dict[str, list[str]]) -> dict[str, Any]:
    discovered_union = {str(col) for cols in (discovered or {}).values() for col in (cols or [])}

    required = [field for field in (contract.get("required_fields") or []) if field != "key"]
    optional = [field for field in (contract.get("optional_fields") or []) if field != "key"]

    selected_fields: list[str] = []
    selected_fields.extend(required)

    mode = str(contract.get("passthrough_mode") or "optional_only")
    if mode in {"optional_only", "all"}:
        selected_fields.extend(optional)
    if mode == "all":
        selected_fields.extend([field for field in sorted(discovered_union) if field != "key"])

    selected_fields = _dedupe_keep_order(selected_fields)
    missing_required = [field for field in required if field not in discovered_union]
    return {"selected_fields": selected_fields, "missing_required": missing_required}
