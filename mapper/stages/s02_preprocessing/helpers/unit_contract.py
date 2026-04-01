from __future__ import annotations

import json
from pathlib import Path
from typing import Any


def _normalize_unit_symbol(unit: Any) -> str:
    return str(unit or "").strip().upper()


def _normalize_units_map(raw_map: dict[str, Any] | None) -> dict[str, str]:
    out: dict[str, str] = {}
    for raw_key, raw_val in (raw_map or {}).items():
        key = _normalize_unit_symbol(raw_key)
        val = _normalize_unit_symbol(raw_val)
        if not key or not val:
            continue
        out[key] = val
    return out


def _normalize_unit_candidates_map(raw_map: dict[str, Any] | None) -> dict[str, list[str]]:
    out: dict[str, list[str]] = {}
    for raw_key, raw_list in (raw_map or {}).items():
        key = _normalize_unit_symbol(raw_key)
        if not key:
            continue
        values = raw_list if isinstance(raw_list, list) else [raw_list]
        seen = set()
        norm_values: list[str] = []
        for item in values:
            unit = _normalize_unit_symbol(item)
            if not unit or unit in seen:
                continue
            seen.add(unit)
            norm_values.append(unit)
        out[key] = norm_values
    return out


def _normalize_tests(raw_tests: dict[str, Any] | None, units_map: dict[str, str]) -> dict[str, dict[str, Any]]:
    out: dict[str, dict[str, Any]] = {}
    for raw_unit, cfg in (raw_tests or {}).items():
        key = _normalize_unit_symbol(raw_unit)
        unit = units_map.get(key, key)
        if not unit or not isinstance(cfg, dict):
            continue
        out[unit] = dict(cfg)
    return out


def _load_standard_output(path_str: str | None) -> dict[str, Any]:
    if not path_str:
        return {}
    path = Path(path_str)
    if not path.exists():
        raise FileNotFoundError(f"-=== unit_contract: STANDARD_OUTPUT config not found: {path} ===-")
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except Exception as exc:
        raise ValueError(f"-=== unit_contract: failed to parse STANDARD_OUTPUT JSON '{path}': {exc} ===-") from exc
    other = raw.get("otherdata_config")
    return other if isinstance(other, dict) else {}


def canonicalize_unit(unit: Any, units_map: dict[str, str]) -> str:
    raw = _normalize_unit_symbol(unit)
    if not raw:
        return ""
    return units_map.get(raw, raw)


def merge_unit_contract(proc_cfg: dict[str, Any] | None) -> dict[str, Any]:
    cfg = dict(proc_cfg or {})
    contract_cfg = dict(cfg.get("unit_contract") or {})

    standard_output_path = contract_cfg.get("standard_output_config_path")
    baseline_other = _load_standard_output(standard_output_path)

    units_map = _normalize_units_map(baseline_other.get("units_map"))
    unit_candidates = _normalize_unit_candidates_map(baseline_other.get("unit_candidates"))
    tests = _normalize_tests(baseline_other.get("unit_tests"), units_map)

    units_map.update(_normalize_units_map(contract_cfg.get("units_map_override")))
    unit_candidates.update(_normalize_unit_candidates_map(contract_cfg.get("unit_candidates_override")))
    tests.update(_normalize_tests(contract_cfg.get("tests_override"), units_map))
    tests.update(_normalize_tests(cfg.get("tests"), units_map))

    return {
        "standard_output_config_path": str(standard_output_path) if standard_output_path else None,
        "units_map": units_map,
        "unit_candidates_map": unit_candidates,
        "tests": tests,
        "strict": bool(contract_cfg.get("strict", False)),
    }
