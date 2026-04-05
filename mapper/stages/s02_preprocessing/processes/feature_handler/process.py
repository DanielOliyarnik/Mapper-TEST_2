from __future__ import annotations

from copy import deepcopy
from typing import Any

from mapper.feature_engine.config_resolver import load_feature_template
from mapper.feature_engine.criteria_builder import resolve_feature_criteria_set
from mapper.feature_engine.feature_defs import resolve_feature_defs
from mapper.feature_engine.feature_match import build_match_fields, match_feature

from ..process_base import Payload, ProcessBase


class Process(ProcessBase):
    def __init__(self, cfg: dict[str, Any], reporter: Any | None = None, progress: Any | None = None) -> None:
        super().__init__(cfg, reporter=reporter, progress=progress)
        self._resolved_defs: list[dict[str, Any]] | None = None
        self._criteria_map: dict[str, dict[str, Any]] | None = None

    def _handle_feature_error(self, policy: str, feature_name: str, message: str) -> None:
        mode = str(policy or "raise").strip().lower()
        if mode == "warn":
            self._emit(f"[feature_handler][warn][{feature_name}] {message}")
            return
        if mode == "skip":
            return
        raise RuntimeError(message)

    def _resolve_feature_defs(self, payload: Payload) -> list[dict[str, Any]]:
        if self._resolved_defs is not None:
            return [deepcopy(item) for item in self._resolved_defs]

        cfg = self.cfg or {}
        dataset_id = str(cfg.get("dataset_id") or "").strip()
        if not dataset_id:
            raise ValueError("-=== (feature_handler): dataset_id is required ===-")

        template = load_feature_template(dataset_id)
        feature_defs = resolve_feature_defs(
            cfg.get("features"),
            use_template_features=bool(cfg.get("use_template_features", False)),
            dataset_id=dataset_id,
            template=template,
        )
        if not feature_defs:
            raise ValueError("-=== (feature_handler): no feature definitions resolved ===-")

        global_fields = build_match_fields(cfg.get("match_fields") or payload.get("meta_contract_fields"))
        if not global_fields:
            raise ValueError("-=== (feature_handler): match_fields unresolved ===-")

        resolved_defs: list[dict[str, Any]] = []
        for feature_def in feature_defs:
            item = deepcopy(feature_def)
            if not item.get("match_fields"):
                item["match_fields"] = list(global_fields)
            resolved_defs.append(item)

        self._resolved_defs = resolved_defs
        return [deepcopy(item) for item in resolved_defs]

    def _resolve_criteria_map(self, payload: Payload) -> dict[str, dict[str, Any]]:
        if self._criteria_map is not None:
            return {name: dict(criteria) for name, criteria in self._criteria_map.items()}

        cfg = self.cfg or {}
        dataset_id = str(cfg.get("dataset_id") or "").strip()
        feature_defs = self._resolve_feature_defs(payload)
        criteria_map = resolve_feature_criteria_set(
            dataset_id=dataset_id,
            feature_defs=feature_defs,
            use_template_fallback=bool(cfg.get("use_template_features", False)),
            _read_fn=None,
            meta_lookup=None,
            candidate_keys=None,
        )
        self._criteria_map = criteria_map
        return {name: dict(criteria) for name, criteria in criteria_map.items()}

    def apply(self, proc_payload: Payload) -> Payload:
        cfg = self.cfg or {}
        dataset_id = str(cfg.get("dataset_id") or "").strip()
        if not dataset_id:
            raise ValueError("-=== (feature_handler): dataset_id is required ===-")

        series = proc_payload.get("series")
        if series is None or series.empty:
            raise ValueError("-=== (feature_handler): Need non-empty series ===-")

        feature_defs = self._resolve_feature_defs(proc_payload)
        criteria_map = self._resolve_criteria_map(proc_payload)
        list_joiner = str(cfg.get("list_joiner") or "|")
        write_info = bool(cfg.get("write_features_info", True))
        on_feature_error = str(cfg.get("on_feature_error", "raise"))

        static_table = dict(proc_payload.get("static") or {})
        features_info = dict(static_table.get("features_info") or {}) if write_info else None
        runtime_read_fn = None
        runtime_context = proc_payload.get("runtime_context")
        if isinstance(runtime_context, dict):
            runtime_read_fn = runtime_context.get("_read_fn")

        for feature_def in feature_defs:
            feature_name = str(feature_def["name"])
            try:
                result = match_feature(
                    dataset_id=dataset_id,
                    feature_name=feature_name,
                    payload=proc_payload,
                    criteria=criteria_map[feature_name],
                    _read_fn=runtime_read_fn,
                    list_joiner=list_joiner,
                )
                matched = bool(result.matched)
                details = dict(result.details or {})
                static_table[feature_name] = matched
                if features_info is not None:
                    fields_used = list(criteria_map[feature_name].get("match_fields") or feature_def.get("match_fields") or [])
                    features_info[feature_name] = {"matched": matched, "details": details, "fields_used": fields_used}
            except Exception as exc:
                self._handle_feature_error(on_feature_error, feature_name, f"-=== (feature_handler): feature '{feature_name}' failed: {exc} ===-")
                static_table[feature_name] = False
                if features_info is not None:
                    features_info[feature_name] = {
                        "matched": False,
                        "details": {"reason": "feature_error", "error": str(exc)},
                        "fields_used": list(feature_def.get("match_fields") or []),
                    }
        if features_info is not None:
            static_table["features_info"] = features_info
        proc_payload["static"] = static_table
        return proc_payload
