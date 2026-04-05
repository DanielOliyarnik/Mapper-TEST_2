from __future__ import annotations

from pathlib import Path
from typing import Any

from .unit_contract import merge_unit_contract

ERR_PREFIX = "-=== (s02_preprocessing.validation)"

_ALLOWED_META_CONTRACT_KEYS = {"strict", "required_fields", "optional_fields", "passthrough_mode", "list_fields", "null_like"}
_ALLOWED_PASSTHROUGH_MODE = {"none", "optional_only", "all"}
_ALLOWED_UNIT_CONTRACT_KEYS = {"standard_output_config_path", "strict", "units_map_override", "unit_candidates_override", "tests_override"}
_ALLOWED_CANDIDATE_SOURCES = {"meta_unit_candidates", "meta_unit", "contract_keyword_map", "candidate_rules", "value_rules"}
_ALLOWED_STRICTNESS_ACTIONS = {"raise", "warn", "skip"}
_ALLOWED_UNIT_STRICTNESS_KEYS = {"on_empty_series", "on_missing_candidates", "on_no_matching_tests", "on_below_min_fraction"}
_ALLOWED_SMOOTHING_METHODS = {"none", "identity", "pass", "rolling_mean", "rolling_median", "ema"}
_ALLOWED_FLAG_FILL = {"constant", "ffill", "bfill", "none"}
_ALLOWED_FLAG_TYPES = {"finite", "range", "rate_change", "rate", "stale", "flatline", "outlier"}
_ALLOWED_FLAG_ACTIONS = {"mark_only", "drop", "warn"}
_ALLOWED_OUTLIER_METHODS = {"zscore", "iqr"}
_ALLOWED_FEATURE_ERROR_POLICY = {"raise", "warn", "skip"}
_ALLOWED_PREDICATE_OPS = {"exists", "truthy", "eq", "ne", "contains", "regex", "in", "not_in"}
_ALLOWED_VALUE_CONDITION_KEYS = {"min_gte", "min_gt", "max_lte", "max_lt", "range_lte", "range_gte", "abs_max_lte", "abs_max_gte", "non_negative"}
_ALLOWED_UNIT_SELECTION_KEYS = {"unit_contract", "candidate_sources", "contract_keyword_fields", "candidate_rules", "value_rules", "strictness", "filter_candidates_to_tests", "min_fraction", "tests", "select_settings"}
_ALLOWED_SMOOTHING_KEYS = {"default", "selectors", "sample_keys"}
_ALLOWED_FLAGGING_KEYS = {"fillna_strategy", "fillna_value", "rules"}
_ALLOWED_FEATURE_HANDLER_KEYS = {
    "dataset_id",
    "use_template_features",
    "match_fields",
    "list_joiner",
    "on_feature_error",
    "features",
    "write_features_info",
}


def _err(errors: list[str], message: str) -> None:
    errors.append(f"{ERR_PREFIX}: {message} ===-")


def _is_non_empty_str(value: Any) -> bool:
    return isinstance(value, str) and bool(value.strip())


def _is_list_of_str(value: Any) -> bool:
    return isinstance(value, list) and all(isinstance(v, str) for v in value)


def _coerce_str_list(value: Any) -> list[str]:
    if not isinstance(value, list):
        return []
    out: list[str] = []
    for item in value:
        if isinstance(item, str):
            token = item.strip()
            if token:
                out.append(token)
    return out


def _validate_unknown_keys(section: str, cfg: dict[str, Any], allowed: set[str], errors: list[str]) -> None:
    for key in cfg.keys():
        if key not in allowed:
            _err(errors, f"{section}: unknown key '{key}'")


def _validate_range_obj(path: str, value: Any, errors: list[str]) -> None:
    if not isinstance(value, dict):
        _err(errors, f"{path} must be object")
        return
    rng = value.get("range")
    if not isinstance(rng, list) or len(rng) != 2:
        _err(errors, f"{path}.range must be [lo, hi]")
        return
    try:
        lo = float(rng[0])
        hi = float(rng[1])
    except Exception:
        _err(errors, f"{path}.range values must be numeric")
        return
    if lo > hi:
        _err(errors, f"{path}.range invalid: lo ({lo}) > hi ({hi})")


def validate_predicate_shape(pred: Any, path: str, errors: list[str]) -> None:
    if pred is None:
        return
    if isinstance(pred, list):
        for idx, item in enumerate(pred):
            validate_predicate_shape(item, f"{path}[{idx}]", errors)
        return
    if not isinstance(pred, dict):
        _err(errors, f"{path} must be object/list/null")
        return
    keys = set(pred.keys())
    branch_keys = keys.intersection({"all", "any", "not"})
    if branch_keys:
        if len(branch_keys) > 1:
            _err(errors, f"{path} has multiple predicate branches {sorted(branch_keys)}; use only one")
            return
        branch = next(iter(branch_keys))
        if branch == "not":
            validate_predicate_shape(pred.get("not"), f"{path}.not", errors)
            return
        value = pred.get(branch)
        if not isinstance(value, list):
            _err(errors, f"{path}.{branch} must be a list")
            return
        for idx, item in enumerate(value):
            validate_predicate_shape(item, f"{path}.{branch}[{idx}]", errors)
        return
    field = pred.get("field")
    if not _is_non_empty_str(field):
        _err(errors, f"{path}.field is required and must be non-empty string")
        return
    op = str(pred.get("op", "eq")).strip().lower()
    if op not in _ALLOWED_PREDICATE_OPS:
        _err(errors, f"{path}.op invalid '{op}' (allowed: {sorted(_ALLOWED_PREDICATE_OPS)})")
        return
    if op in {"exists", "truthy"}:
        return
    if op in {"in", "not_in"}:
        has_value = "value" in pred
        has_values = "values" in pred
        if not (has_value or has_values):
            _err(errors, f"{path} requires 'value' or 'values' for op '{op}'")
            return
        if has_values and not isinstance(pred.get("values"), list):
            _err(errors, f"{path}.values must be list when provided")
        return
    if "value" not in pred:
        _err(errors, f"{path}.value is required for op '{op}'")


def validate_meta_contract_cfg(cfg: dict[str, Any] | None, errors: list[str]) -> None:
    if cfg is None:
        return
    if not isinstance(cfg, dict):
        _err(errors, "preprocessing.meta_contract must be object")
        return
    _validate_unknown_keys("preprocessing.meta_contract", cfg, _ALLOWED_META_CONTRACT_KEYS, errors)
    strict = cfg.get("strict")
    if strict is not None and not isinstance(strict, bool):
        _err(errors, "preprocessing.meta_contract.strict must be boolean")
    for field_name in ("required_fields", "optional_fields", "list_fields"):
        val = cfg.get(field_name)
        if val is not None and not _is_list_of_str(val):
            _err(errors, f"preprocessing.meta_contract.{field_name} must be list[str]")
    mode = cfg.get("passthrough_mode")
    if mode is not None and str(mode).strip().lower() not in _ALLOWED_PASSTHROUGH_MODE:
        _err(errors, f"preprocessing.meta_contract.passthrough_mode invalid '{mode}'")
    if "required_fields" in cfg:
        required = _coerce_str_list(cfg.get("required_fields"))
        if "key" not in required:
            _err(errors, "preprocessing.meta_contract.required_fields must include 'key'")
    null_like = cfg.get("null_like")
    if null_like is not None and not isinstance(null_like, list):
        _err(errors, "preprocessing.meta_contract.null_like must be list")


def validate_unit_contract_cfg(cfg: dict[str, Any] | None, errors: list[str], section: str) -> None:
    if cfg is None:
        return
    if not isinstance(cfg, dict):
        _err(errors, f"{section} must be object")
        return
    _validate_unknown_keys(section, cfg, _ALLOWED_UNIT_CONTRACT_KEYS, errors)
    strict = cfg.get("strict")
    if strict is not None and not isinstance(strict, bool):
        _err(errors, f"{section}.strict must be boolean")
    path = cfg.get("standard_output_config_path")
    if path is not None and not _is_non_empty_str(path):
        _err(errors, f"{section}.standard_output_config_path must be non-empty string or null")
    if _is_non_empty_str(path) and not Path(str(path)).exists():
        _err(errors, f"{section}.standard_output_config_path not found: {path}")
    units_map = cfg.get("units_map_override")
    if units_map is not None:
        if not isinstance(units_map, dict):
            _err(errors, f"{section}.units_map_override must be object")
        else:
            for k, v in units_map.items():
                if not _is_non_empty_str(k) or not _is_non_empty_str(v):
                    _err(errors, f"{section}.units_map_override entries must be string->string")
                    break
    unit_candidates = cfg.get("unit_candidates_override")
    if unit_candidates is not None:
        if not isinstance(unit_candidates, dict):
            _err(errors, f"{section}.unit_candidates_override must be object")
        else:
            for k, v in unit_candidates.items():
                if not _is_non_empty_str(k):
                    _err(errors, f"{section}.unit_candidates_override has empty key")
                    continue
                if isinstance(v, str):
                    continue
                if not _is_list_of_str(v):
                    _err(errors, f"{section}.unit_candidates_override['{k}'] must be string or list[str]")
    tests_override = cfg.get("tests_override")
    if tests_override is not None:
        if not isinstance(tests_override, dict):
            _err(errors, f"{section}.tests_override must be object")
        else:
            for unit, test_cfg in tests_override.items():
                _validate_range_obj(f"{section}.tests_override['{unit}']", test_cfg, errors)


def _validate_tests_map(tests: Any, path: str, errors: list[str]) -> None:
    if tests is None:
        return
    if not isinstance(tests, dict):
        _err(errors, f"{path} must be object")
        return
    for unit, test_cfg in tests.items():
        _validate_range_obj(f"{path}['{unit}']", test_cfg, errors)


def _validate_candidate_rules(rules: Any, errors: list[str], path: str) -> None:
    if rules is None:
        return
    if not isinstance(rules, list):
        _err(errors, f"{path} must be list")
        return
    for idx, rule in enumerate(rules):
        rpath = f"{path}[{idx}]"
        if not isinstance(rule, dict):
            _err(errors, f"{rpath} must be object")
            continue
        fields = rule.get("fields")
        field = rule.get("field")
        if fields is not None and not _is_list_of_str(fields):
            _err(errors, f"{rpath}.fields must be list[str]")
        if field is not None and not _is_non_empty_str(field):
            _err(errors, f"{rpath}.field must be non-empty string")
        mode = rule.get("mode")
        if mode is not None and str(mode).strip().lower() not in {"any", "all"}:
            _err(errors, f"{rpath}.mode invalid '{mode}' (allowed: any|all)")
        has_matcher = False
        if _is_non_empty_str(rule.get("regex")) or _is_non_empty_str(rule.get("pattern")):
            has_matcher = True
        contains_any = rule.get("contains_any")
        contains_all = rule.get("contains_all")
        if contains_any is not None:
            if not _is_list_of_str(contains_any):
                _err(errors, f"{rpath}.contains_any must be list[str]")
            elif contains_any:
                has_matcher = True
        if contains_all is not None:
            if not _is_list_of_str(contains_all):
                _err(errors, f"{rpath}.contains_all must be list[str]")
            elif contains_all:
                has_matcher = True
        if not has_matcher:
            _err(errors, f"{rpath} must define at least one matcher (regex/pattern/contains_any/contains_all)")
        candidates = rule.get("candidates")
        if not _is_list_of_str(candidates) or not candidates:
            _err(errors, f"{rpath}.candidates must be non-empty list[str]")


def _validate_value_rules(rules: Any, errors: list[str], path: str) -> None:
    if rules is None:
        return
    if not isinstance(rules, list):
        _err(errors, f"{path} must be list")
        return
    for idx, rule in enumerate(rules):
        rpath = f"{path}[{idx}]"
        if not isinstance(rule, dict):
            _err(errors, f"{rpath} must be object")
            continue
        cond = rule.get("conditions")
        if not isinstance(cond, dict) or not cond:
            _err(errors, f"{rpath}.conditions must be non-empty object")
            continue
        for key, value in cond.items():
            if key not in _ALLOWED_VALUE_CONDITION_KEYS:
                _err(errors, f"{rpath}.conditions has invalid key '{key}'")
                continue
            if key == "non_negative":
                if not isinstance(value, bool):
                    _err(errors, f"{rpath}.conditions.non_negative must be boolean")
            else:
                try:
                    float(value)
                except Exception:
                    _err(errors, f"{rpath}.conditions.{key} must be numeric")
        candidates = rule.get("candidates")
        if not _is_list_of_str(candidates) or not candidates:
            _err(errors, f"{rpath}.candidates must be non-empty list[str]")


def validate_unit_selection_cfg(cfg: dict[str, Any] | None, errors: list[str], stage_unit_contract: dict[str, Any] | None) -> None:
    if not isinstance(cfg, dict):
        _err(errors, "unit_selection config must be object")
        return
    _validate_unknown_keys("unit_selection", cfg, _ALLOWED_UNIT_SELECTION_KEYS, errors)
    strictness = cfg.get("strictness")
    if not isinstance(strictness, dict):
        _err(errors, "unit_selection.strictness must be object")
    else:
        for key, value in strictness.items():
            if key not in _ALLOWED_UNIT_STRICTNESS_KEYS:
                _err(errors, f"unit_selection.strictness has unknown key '{key}'")
                continue
            if str(value).strip().lower() not in _ALLOWED_STRICTNESS_ACTIONS:
                _err(errors, f"unit_selection.strictness.{key} invalid '{value}' (allowed: raise|warn|skip)")
    sources = cfg.get("candidate_sources")
    if not isinstance(sources, list) or not sources:
        _err(errors, "unit_selection.candidate_sources must be non-empty list")
        source_list: list[str] = []
    else:
        source_list = [str(item).strip() for item in sources]
        for idx, source in enumerate(source_list):
            if source not in _ALLOWED_CANDIDATE_SOURCES:
                _err(errors, f"unit_selection.candidate_sources[{idx}] invalid '{source}'")
    ck_fields = cfg.get("contract_keyword_fields")
    if ck_fields is not None and not _is_list_of_str(ck_fields):
        _err(errors, "unit_selection.contract_keyword_fields must be list[str]")
    _validate_candidate_rules(cfg.get("candidate_rules"), errors, "unit_selection.candidate_rules")
    _validate_value_rules(cfg.get("value_rules"), errors, "unit_selection.value_rules")
    if "candidate_rules" in source_list and not cfg.get("candidate_rules"):
        _err(errors, "unit_selection.candidate_sources includes 'candidate_rules' but candidate_rules is empty")
    if "value_rules" in source_list and not cfg.get("value_rules"):
        _err(errors, "unit_selection.candidate_sources includes 'value_rules' but value_rules is empty")
    min_fraction = cfg.get("min_fraction")
    try:
        mf = float(min_fraction)
        if not (0.0 <= mf <= 1.0):
            _err(errors, f"unit_selection.min_fraction must be in [0,1], got {min_fraction}")
    except Exception:
        _err(errors, f"unit_selection.min_fraction must be numeric, got {min_fraction}")
    filter_candidates = cfg.get("filter_candidates_to_tests")
    if filter_candidates is not None and not isinstance(filter_candidates, bool):
        _err(errors, "unit_selection.filter_candidates_to_tests must be boolean")
    _validate_tests_map(cfg.get("tests"), "unit_selection.tests", errors)
    select_settings = cfg.get("select_settings")
    if select_settings is not None:
        if not isinstance(select_settings, dict):
            _err(errors, "unit_selection.select_settings must be object")
        else:
            abs_for_units = select_settings.get("abs_for_units")
            if abs_for_units is not None and not _is_list_of_str(abs_for_units):
                _err(errors, "unit_selection.select_settings.abs_for_units must be list[str]")
    proc_uc = cfg.get("unit_contract")
    validate_unit_contract_cfg(proc_uc, errors, "unit_selection.unit_contract")
    merged_uc = {**dict(stage_unit_contract or {}), **dict(proc_uc or {})}
    merged_cfg = dict(cfg)
    merged_cfg["unit_contract"] = merged_uc
    try:
        contract = merge_unit_contract(merged_cfg)
    except Exception as exc:
        _err(errors, f"unit_selection.unit_contract merge failed: {exc}")
        return
    if bool(contract.get("strict", False)) and not (contract.get("tests") or {}):
        _err(errors, "unit_selection strict unit_contract resolved no unit tests (check STANDARD_OUTPUT path and overrides)")


def _validate_smoothing_params(method: str, params: dict[str, Any], path: str, errors: list[str]) -> None:
    m = str(method).strip().lower()
    if m not in _ALLOWED_SMOOTHING_METHODS:
        _err(errors, f"{path}.method invalid '{method}'")
        return
    if not isinstance(params, dict):
        _err(errors, f"{path}.params must be object")
        return
    if m in {"rolling_mean", "rolling_median"}:
        if "window" in params:
            try:
                if int(params["window"]) < 1:
                    _err(errors, f"{path}.params.window must be >= 1")
            except Exception:
                _err(errors, f"{path}.params.window must be integer")
        if "min_periods" in params:
            try:
                if int(params["min_periods"]) < 1:
                    _err(errors, f"{path}.params.min_periods must be >= 1")
            except Exception:
                _err(errors, f"{path}.params.min_periods must be integer")
    if m == "ema" and "span" in params:
        try:
            if int(params["span"]) < 1:
                _err(errors, f"{path}.params.span must be >= 1")
        except Exception:
            _err(errors, f"{path}.params.span must be integer")


def validate_smoothing_cfg(cfg: dict[str, Any] | None, errors: list[str]) -> None:
    if not isinstance(cfg, dict):
        _err(errors, "smoothing_filtering config must be object")
        return
    _validate_unknown_keys("smoothing_filtering", cfg, _ALLOWED_SMOOTHING_KEYS, errors)
    default = cfg.get("default")
    if not isinstance(default, dict):
        _err(errors, "smoothing_filtering.default must be object")
        return
    _validate_smoothing_params(default.get("method", ""), default.get("params", {}), "smoothing_filtering.default", errors)
    selectors = cfg.get("selectors", [])
    if selectors is not None and not isinstance(selectors, list):
        _err(errors, "smoothing_filtering.selectors must be list")
        selectors = []
    for idx, selector in enumerate(selectors or []):
        spath = f"smoothing_filtering.selectors[{idx}]"
        if not isinstance(selector, dict):
            _err(errors, f"{spath} must be object")
            continue
        if not _is_non_empty_str(selector.get("id")):
            _err(errors, f"{spath}.id must be non-empty string")
        validate_predicate_shape(selector.get("when"), f"{spath}.when", errors)
        _validate_smoothing_params(selector.get("method", ""), selector.get("params", {}), spath, errors)


def validate_flagging_cfg(cfg: dict[str, Any] | None, errors: list[str]) -> None:
    if not isinstance(cfg, dict):
        _err(errors, "flagging config must be object")
        return
    _validate_unknown_keys("flagging", cfg, _ALLOWED_FLAGGING_KEYS, errors)
    fill_strategy = str(cfg.get("fillna_strategy", "")).strip().lower()
    if fill_strategy not in _ALLOWED_FLAG_FILL:
        _err(errors, f"flagging.fillna_strategy invalid '{cfg.get('fillna_strategy')}'")
    if fill_strategy == "constant":
        try:
            float(cfg.get("fillna_value", 0.0))
        except Exception:
            _err(errors, "flagging.fillna_value must be numeric when fillna_strategy='constant'")
    rules = cfg.get("rules")
    if not isinstance(rules, list) or not rules:
        _err(errors, "flagging.rules must be non-empty list")
        return
    for idx, rule in enumerate(rules):
        rpath = f"flagging.rules[{idx}]"
        if not isinstance(rule, dict):
            _err(errors, f"{rpath} must be object")
            continue
        rtype = str(rule.get("type", "")).strip().lower()
        if rtype not in _ALLOWED_FLAG_TYPES:
            _err(errors, f"{rpath}.type invalid '{rule.get('type')}'")
            continue
        action = str(rule.get("action", "mark_only")).strip().lower()
        if action not in _ALLOWED_FLAG_ACTIONS:
            _err(errors, f"{rpath}.action invalid '{rule.get('action')}'")
        enabled = rule.get("enabled")
        if enabled is not None and not isinstance(enabled, bool):
            _err(errors, f"{rpath}.enabled must be boolean")
        validate_predicate_shape(rule.get("apply_if"), f"{rpath}.apply_if", errors)
        params = rule.get("params", {})
        if not isinstance(params, dict):
            _err(errors, f"{rpath}.params must be object")
            continue
        if rtype == "range":
            if "lo" not in params and "hi" not in params:
                _err(errors, f"{rpath}.params must include lo or hi for range rule")
            for bound_key in ("lo", "hi"):
                if bound_key in params:
                    try:
                        float(params[bound_key])
                    except Exception:
                        _err(errors, f"{rpath}.params.{bound_key} must be numeric")
        if rtype in {"rate_change", "rate"}:
            if "max_abs_delta" not in params and "min_abs_delta" not in params:
                _err(errors, f"{rpath}.params must include max_abs_delta or min_abs_delta")
            for bound_key in ("max_abs_delta", "min_abs_delta"):
                if bound_key in params:
                    try:
                        float(params[bound_key])
                    except Exception:
                        _err(errors, f"{rpath}.params.{bound_key} must be numeric")
        if rtype in {"stale", "flatline"}:
            if "epsilon" in params:
                try:
                    float(params["epsilon"])
                except Exception:
                    _err(errors, f"{rpath}.params.epsilon must be numeric")
            if "min_run_len" in params:
                try:
                    if int(params["min_run_len"]) < 2:
                        _err(errors, f"{rpath}.params.min_run_len must be >= 2")
                except Exception:
                    _err(errors, f"{rpath}.params.min_run_len must be integer")
        if rtype == "outlier":
            method = str(params.get("method", "zscore")).strip().lower()
            if method not in _ALLOWED_OUTLIER_METHODS:
                _err(errors, f"{rpath}.params.method invalid '{params.get('method')}'")
            if "threshold" in params:
                try:
                    if float(params["threshold"]) <= 0:
                        _err(errors, f"{rpath}.params.threshold must be > 0")
                except Exception:
                    _err(errors, f"{rpath}.params.threshold must be numeric")


def validate_feature_handler_cfg(cfg: dict[str, Any] | None, errors: list[str], section_name: str = "feature_handler") -> None:
    if not isinstance(cfg, dict):
        _err(errors, f"{section_name} config must be object")
        return
    _validate_unknown_keys(section_name, cfg, _ALLOWED_FEATURE_HANDLER_KEYS, errors)
    dataset_id = cfg.get("dataset_id")
    if dataset_id is None:
        _err(errors, f"{section_name}.dataset_id is required after Stage 2 config binding")
    elif not _is_non_empty_str(dataset_id):
        _err(errors, f"{section_name}.dataset_id must be non-empty string")
    use_template_features = cfg.get("use_template_features")
    if not isinstance(use_template_features, bool):
        _err(errors, f"{section_name}.use_template_features must be boolean")
    match_fields = cfg.get("match_fields")
    if not _is_list_of_str(match_fields) or not match_fields:
        _err(errors, f"{section_name}.match_fields must be non-empty list[str]")
    list_joiner = cfg.get("list_joiner")
    if list_joiner is not None and not _is_non_empty_str(list_joiner):
        _err(errors, f"{section_name}.list_joiner must be non-empty string")
    write_features_info = cfg.get("write_features_info")
    if write_features_info is not None and not isinstance(write_features_info, bool):
        _err(errors, f"{section_name}.write_features_info must be boolean")
    policy = str(cfg.get("on_feature_error", "")).strip().lower()
    if policy not in _ALLOWED_FEATURE_ERROR_POLICY:
        _err(errors, f"{section_name}.on_feature_error invalid '{cfg.get('on_feature_error')}'")
    features = cfg.get("features")
    if features is not None and not isinstance(features, list):
        _err(errors, f"{section_name}.features must be list")
        features = []
    if not (features and len(features) > 0) and not bool(use_template_features):
        _err(errors, f"{section_name} requires non-empty features[] or use_template_features=true")
    for idx, feature in enumerate(features or []):
        fpath = f"{section_name}.features[{idx}]"
        if not isinstance(feature, dict):
            _err(errors, f"{fpath} must be object")
            continue
        if not _is_non_empty_str(feature.get("name")):
            _err(errors, f"{fpath}.name is required and must be non-empty string")
        fm_fields = feature.get("match_fields")
        if fm_fields is not None and not _is_list_of_str(fm_fields):
            _err(errors, f"{fpath}.match_fields must be list[str]")
        candidate_filter = feature.get("candidate_filter")
        if candidate_filter is not None and not isinstance(candidate_filter, dict):
            _err(errors, f"{fpath}.candidate_filter must be object when provided")
        criteria_overrides = feature.get("criteria_overrides")
        if criteria_overrides is not None and not isinstance(criteria_overrides, dict):
            _err(errors, f"{fpath}.criteria_overrides must be object when provided")


def _validate_process_list(processes: Any, errors: list[str]) -> list[dict[str, Any]]:
    if not isinstance(processes, list) or not processes:
        _err(errors, "preprocessing.processes must be a non-empty list")
        return []
    out: list[dict[str, Any]] = []
    for idx, item in enumerate(processes):
        path = f"preprocessing.processes[{idx}]"
        if not isinstance(item, dict):
            _err(errors, f"{path} must be object")
            continue
        module = item.get("module")
        config_name = item.get("config_name")
        if not _is_non_empty_str(module):
            _err(errors, f"{path}.module must be non-empty string")
        if not _is_non_empty_str(config_name):
            _err(errors, f"{path}.config_name must be non-empty string")
        trainer = item.get("trainer")
        if trainer is not None:
            if not isinstance(trainer, dict):
                _err(errors, f"{path}.trainer must be object")
            else:
                enabled = trainer.get("enabled")
                if enabled is not None and not isinstance(enabled, bool):
                    _err(errors, f"{path}.trainer.enabled must be boolean")
                if bool(trainer.get("enabled")) and not _is_non_empty_str(trainer.get("config_name")):
                    _err(errors, f"{path}.trainer.config_name must be non-empty string when trainer.enabled=true")
        out.append(item)
    return out


def validate_preprocessing_startup(stage_cfg: dict[str, Any] | None, resolved_process_cfgs: list[tuple[str, dict[str, Any]]]) -> None:
    cfg = dict(stage_cfg or {})
    errors: list[str] = []
    validate_meta_contract_cfg(cfg.get("meta_contract"), errors)
    validate_unit_contract_cfg(cfg.get("unit_contract"), errors, "preprocessing.unit_contract")
    io_cfg = cfg.get("io")
    if io_cfg is not None:
        if not isinstance(io_cfg, dict):
            _err(errors, "preprocessing.io must be object")
        else:
            period = io_cfg.get("period")
            if period is not None and not isinstance(period, dict):
                _err(errors, "preprocessing.io.period must be object")
    _validate_process_list(cfg.get("processes"), errors)
    stage_unit_contract = dict(cfg.get("unit_contract") or {})
    for module_name, proc_cfg in resolved_process_cfgs:
        name = str(module_name).strip()
        if not name:
            continue
        if name == "unit_selection":
            validate_unit_selection_cfg(proc_cfg, errors, stage_unit_contract=stage_unit_contract)
        elif name == "smoothing_filtering":
            validate_smoothing_cfg(proc_cfg, errors)
        elif name == "flagging":
            validate_flagging_cfg(proc_cfg, errors)
        elif name == "feature_handler":
            validate_feature_handler_cfg(proc_cfg, errors, section_name="feature_handler")
    if errors:
        header = f"{ERR_PREFIX}: startup configuration validation failed with {len(errors)} error(s) ===-"
        body = "\n".join(f"  - {msg}" for msg in errors)
        raise ValueError(f"{header}\n{body}")
