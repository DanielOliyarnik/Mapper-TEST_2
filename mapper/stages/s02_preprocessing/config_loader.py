from __future__ import annotations

import json
from copy import deepcopy
from pathlib import Path
from typing import Any

from .helpers.unit_contract import merge_unit_contract

_STAGE_ROOT = Path(__file__).resolve().parent
_PROCESS_ROOT = _STAGE_ROOT / "processes"
_REQUIRED_PROCESS_KEYS = {"module", "config_name"}


def load_config(stage_cfg: dict[str, Any]) -> dict[str, Any]:
    cfg = dict(stage_cfg or {})
    validate_config(cfg)
    return cfg


def validate_config(cfg: dict[str, Any]) -> None:
    if not isinstance(cfg, dict):
        raise TypeError("Stage 2 resolved config must be an object")
    processes = cfg.get("processes")
    if not isinstance(processes, list) or not processes:
        raise ValueError("Stage 2 requires an explicit non-empty processes list")
    for idx, item in enumerate(processes):
        path = f"processes[{idx}]"
        if not isinstance(item, dict):
            raise TypeError(f"Stage 2 {path} must be an object")
        missing = [key for key in _REQUIRED_PROCESS_KEYS if not str(item.get(key) or "").strip()]
        if missing:
            raise ValueError(f"Stage 2 {path} is missing required keys: {', '.join(missing)}")
        trainer = item.get("trainer")
        if trainer is not None:
            if not isinstance(trainer, dict):
                raise TypeError(f"Stage 2 {path}.trainer must be an object")
            if bool(trainer.get("enabled")) and not str(trainer.get("config_name") or "").strip():
                raise ValueError(f"Stage 2 {path}.trainer.config_name is required when trainer.enabled=true")
    io_cfg = cfg.get("io")
    if io_cfg is not None and not isinstance(io_cfg, dict):
        raise TypeError("Stage 2 io must be an object when provided")
    for section_name in ("meta_contract", "unit_contract"):
        section = cfg.get(section_name)
        if section is not None and not isinstance(section, dict):
            raise TypeError(f"Stage 2 {section_name} must be an object when provided")


def merge_like_fields(base_cfg: dict[str, Any], override_cfg: dict[str, Any]) -> dict[str, Any]:
    merged = deepcopy(base_cfg)

    def _merge(dst: dict[str, Any], src: dict[str, Any]) -> None:
        for key, value in src.items():
            if isinstance(dst.get(key), dict) and isinstance(value, dict):
                _merge(dst[key], value)
            else:
                dst[key] = deepcopy(value)

    if isinstance(override_cfg, dict):
        _merge(merged, override_cfg)
    return merged


def _normalize_config_basename(config_name: str) -> str:
    token = str(config_name or "").strip()
    if not token:
        raise ValueError("config_name must be non-empty")
    return token[:-5] if token.endswith(".json") else token


def resolve_process_config_path(dataset_id: str, process_name: str, config_name: str) -> Path:
    process_root = _PROCESS_ROOT / process_name / "configs"
    if not process_root.exists():
        raise FileNotFoundError(f"Process config directory not found: {process_root}")
    basename = _normalize_config_basename(config_name)
    candidates = [process_root / f"{dataset_id}_{basename}.json", process_root / f"{basename}.json"]
    for candidate in candidates:
        if candidate.exists():
            return candidate
    raise FileNotFoundError(
        f"Process config not found for process '{process_name}' and config_name '{config_name}'. Tried: {', '.join(str(path) for path in candidates)}"
    )


def _load_json(path: Path) -> dict[str, Any]:
    try:
        loaded = json.loads(path.read_text(encoding="utf-8"))
    except Exception as exc:
        raise ValueError(f"Failed to parse JSON config '{path}': {exc}") from exc
    if not isinstance(loaded, dict):
        raise TypeError(f"Config root must be an object: {path}")
    return loaded


def _bind_dataset_context(process_name: str, resolved_cfg: dict[str, Any], dataset_id: str) -> dict[str, Any]:
    bound = dict(resolved_cfg)
    if process_name == "feature_handler" and not str(bound.get("dataset_id") or "").strip():
        bound["dataset_id"] = dataset_id
    return bound


def _item_overrides(item_cfg: dict[str, Any]) -> dict[str, Any]:
    return {
        key: value
        for key, value in item_cfg.items()
        if key not in {"module", "config_name", "trainer", "enabled"}
    }


def load_process_config(dataset_id: str, process_name: str, proc_cfg: dict[str, Any]) -> dict[str, Any]:
    path = resolve_process_config_path(dataset_id, process_name, str(proc_cfg["config_name"]))
    loaded = _load_json(path)
    merged = merge_like_fields(loaded, _item_overrides(proc_cfg))
    return _bind_dataset_context(process_name, merged, dataset_id)


def load_meta_contract_cfg(cfg: dict[str, Any]) -> dict[str, Any]:
    return dict(cfg.get("meta_contract") or {})


def load_unit_contract_cfg(cfg: dict[str, Any]) -> dict[str, Any]:
    return dict(cfg.get("unit_contract") or {})


def resolve_process_configs(cfg: dict[str, Any], dataset_id: str) -> list[tuple[str, dict[str, Any]]]:
    resolved: list[tuple[str, dict[str, Any]]] = []
    for item in (cfg.get("processes") or []):
        if not isinstance(item, dict):
            continue
        module_name = str(item.get("module") or "").strip()
        if not module_name:
            continue
        resolved.append((module_name, load_process_config(dataset_id, module_name, item)))
    return resolved


def build_shared_context(cfg: dict[str, Any], process_list: list[tuple[str, Any]]) -> dict[str, Any]:
    shared: dict[str, Any] = {}
    stage_unit_contract = load_unit_contract_cfg(cfg)
    process_unit_cfg: dict[str, Any] = {}
    for name, proc in process_list:
        if name == "unit_selection" and isinstance(getattr(proc, "cfg", None), dict):
            process_unit_cfg = deepcopy(proc.cfg)
            break
    if stage_unit_contract or process_unit_cfg:
        unit_cfg = deepcopy(process_unit_cfg) if process_unit_cfg else {}
        existing = dict(unit_cfg.get("unit_contract") or {})
        unit_cfg["unit_contract"] = {**stage_unit_contract, **existing}
        shared["unit_contract"] = merge_unit_contract(unit_cfg)
    return shared
