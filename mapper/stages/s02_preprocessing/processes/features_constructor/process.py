from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from mapper.features.match_fields import coerce_match_fields, normalize_feature_specs, project_match_fields
from mapper.features.registry import get_feature, try_import_dataset_feature

from ..process_base import Payload, ProcessBase

_CONFIG_CACHE: dict[tuple[str, str], dict[str, Any]] = {}
_TEMPLATE_CACHE: dict[str, dict[str, Any]] = {}


def _dataset_root(dataset: str) -> Path:
    root_mapper = Path(__file__).resolve().parents[4]
    path = root_mapper / "features" / "datasets" / dataset
    if path.exists():
        return path
    raise FileNotFoundError(f"-=== Feature dataset folder not found: {path} ===-")


def _latest_template(dataset: str) -> dict[str, Any]:
    if dataset in _TEMPLATE_CACHE:
        return _TEMPLATE_CACHE[dataset]
    root = _dataset_root(dataset)
    template_dir = root / "input" / "criteria_templates"
    candidates = sorted(template_dir.glob("template_*.json"))
    if not candidates:
        raise FileNotFoundError(f"-=== No template_*.json found in {template_dir} ===-")
    data = json.loads(candidates[-1].read_text(encoding="utf-8"))
    _TEMPLATE_CACHE[dataset] = data
    return data


def _load_feature_cfg(dataset: str, name: str, use_template_fallback: bool) -> dict[str, Any]:
    cache_key = (dataset, name)
    if cache_key in _CONFIG_CACHE:
        return _CONFIG_CACHE[cache_key]
    cfg_dir = _dataset_root(dataset) / "output" / "features_configs"
    path = cfg_dir / f"{name}.json"
    if path.exists():
        cfg = json.loads(path.read_text(encoding="utf-8"))
        _CONFIG_CACHE[cache_key] = cfg
        return cfg
    if use_template_fallback:
        template = _latest_template(dataset)
        if name in template and isinstance(template[name], dict):
            cfg = dict(template[name])
            _CONFIG_CACHE[cache_key] = cfg
            return cfg
    raise FileNotFoundError(f"-=== features_constructor: missing config for '{name}': {path} ===-")


def _resolve_feature_specs(cfg: dict[str, Any], state: dict[str, Any] | None, dataset: str) -> list[dict[str, Any]]:
    specs = normalize_feature_specs(cfg.get("features"))
    if specs:
        return specs
    state_names: list[str] = []
    if isinstance(state, dict):
        state_names = [str(name).strip() for name in (state.get("feature_names") or []) if str(name).strip()]
    if state_names:
        return [{"name": name} for name in state_names]
    if bool(cfg.get("use_template_features", False)):
        template = _latest_template(dataset)
        names = [str(key).strip() for key in template.keys() if str(key).strip()]
        if not names:
            raise ValueError("-=== features_constructor: template has no feature keys ===-")
        return [{"name": name} for name in names]
    raise ValueError("-=== features_constructor: no feature specs resolved (set features[] or use_template_features=true) ===-")


def _resolve_global_match_fields(cfg: dict[str, Any], state: dict[str, Any] | None, payload: Payload) -> list[str]:
    cfg_fields = coerce_match_fields(cfg.get("match_fields"))
    if cfg_fields:
        return cfg_fields
    state_fields = coerce_match_fields((state or {}).get("match_fields")) if isinstance(state, dict) else []
    if state_fields:
        return state_fields
    payload_fields = coerce_match_fields(payload.get("meta_contract_fields"))
    if payload_fields:
        return payload_fields
    raise ValueError("-=== features_constructor: match_fields unresolved (required by new standard) ===-")


class Process(ProcessBase):
    def _handle_feature_error(self, policy: str, feature_name: str, message: str) -> None:
        mode = str(policy or "raise").strip().lower()
        if mode == "warn":
            self._emit(f"[features_constructor][warn][{feature_name}] {message}")
            return
        if mode == "skip":
            return
        raise RuntimeError(message)

    def apply(self, proc_payload: Payload) -> Payload:
        series = proc_payload.get("series")
        if series is None or series.empty:
            raise ValueError("-=== (features_constructor): Need non-empty series ===-")
        cfg = self.cfg or {}
        state = self._state if isinstance(self._state, dict) else {}
        dataset = str(cfg.get("dataset") or state.get("dataset") or "").strip()
        if not dataset:
            raise ValueError("-=== (features_constructor): dataset is required ===-")
        feature_specs = _resolve_feature_specs(cfg, state, dataset)
        global_fields = _resolve_global_match_fields(cfg, state, proc_payload)
        list_joiner = str(cfg.get("list_joiner") or "|")
        write_info = bool(cfg.get("write_features_info", True))
        on_feature_error = str(cfg.get("on_feature_error", "raise"))
        use_template_fallback = bool(cfg.get("use_template_features", False))
        static_table = dict(proc_payload.get("static") or {})
        meta = dict(proc_payload.get("meta") or {})
        features_info = dict(static_table.get("features_info") or {}) if write_info else None
        for spec in feature_specs:
            feature_name = spec["name"]
            fields = coerce_match_fields(spec.get("match_fields")) or list(global_fields)
            if not fields:
                self._handle_feature_error(on_feature_error, feature_name, f"-=== (features_constructor): no match_fields resolved for feature '{feature_name}' ===-")
                static_table[feature_name] = False
                if features_info is not None:
                    features_info[feature_name] = {"matched": False, "details": {"reason": "no_match_fields"}, "fields_used": []}
                continue
            try:
                try_import_dataset_feature(dataset, feature_name)
                plugin = get_feature(feature_name)
                feature_cfg = _load_feature_cfg(dataset, feature_name, use_template_fallback)
                static_match = project_match_fields(primary=meta, secondary=static_table, fields=fields, list_joiner=list_joiner)
                result = plugin.match(series, static_match, feature_cfg)
                matched = bool(getattr(result, "matched", False))
                details = dict(getattr(result, "details", {}) or {})
                static_table[feature_name] = matched
                if features_info is not None:
                    features_info[feature_name] = {"matched": matched, "details": details, "fields_used": list(fields)}
            except Exception as exc:
                self._handle_feature_error(on_feature_error, feature_name, f"-=== (features_constructor): feature '{feature_name}' failed: {exc} ===-")
                static_table[feature_name] = False
                if features_info is not None:
                    features_info[feature_name] = {"matched": False, "details": {"reason": "feature_error", "error": str(exc)}, "fields_used": list(fields)}
        if features_info is not None:
            static_table["features_info"] = features_info
        proc_payload["static"] = static_table
        return proc_payload
