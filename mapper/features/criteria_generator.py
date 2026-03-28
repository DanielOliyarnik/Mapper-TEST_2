from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Callable

from .match_fields import coerce_match_fields, normalize_feature_specs, project_match_fields
from .registry import get_feature, try_import_dataset_feature


def _dataset_root(dataset: str) -> Path:
    path = Path(__file__).resolve().parent / "datasets" / dataset
    if not path.exists():
        raise FileNotFoundError(f"No dataset templates at {path}")
    return path


def _latest_template_path(dataset_root: Path) -> Path:
    templates = sorted((dataset_root / "input" / "criteria_templates").glob("template_*.json"))
    if not templates:
        raise FileNotFoundError(f"No template_*.json in {dataset_root}")
    return templates[-1]


def _resolve_feature_specs(trainer_cfg: dict[str, Any], template: dict[str, Any]) -> list[dict[str, Any]]:
    specs = normalize_feature_specs(trainer_cfg.get("features"))
    if specs:
        return specs
    if trainer_cfg.get("use_template_features"):
        return [{"name": name} for name in template]
    return []


def _resolve_match_fields(trainer_cfg: dict[str, Any]) -> list[str]:
    return coerce_match_fields(trainer_cfg.get("match_fields"))


def _apply_prefilter(rows: list[dict[str, Any]], prefilter: dict[str, Any]) -> list[dict[str, Any]]:
    if not prefilter:
        return list(rows)
    out: list[dict[str, Any]] = []
    for row in rows:
        keep = True
        for key, allowed in prefilter.items():
            allowed_set = {str(item).upper() for item in allowed}
            if str(row.get(key, "")).upper() not in allowed_set:
                keep = False
                break
        if keep:
            out.append(row)
    return out


def generate_configs_for_keys(
    dataset: str,
    keys: list[str],
    _read_fn: Callable[[str], Any],
    meta_lookup: dict[str, dict[str, Any]],
    trainer_cfg: dict[str, Any],
    force: bool = False,
) -> dict[str, Any]:
    root = _dataset_root(dataset)
    output_dir = root / "output" / "features_configs"
    output_dir.mkdir(parents=True, exist_ok=True)
    template = json.loads(_latest_template_path(root).read_text(encoding="utf-8"))
    feature_specs = _resolve_feature_specs(trainer_cfg, template)
    match_fields = _resolve_match_fields(trainer_cfg)
    rows = [{"key": key, **meta_lookup.get(key, {})} for key in keys]
    cfgs_out: dict[str, Any] = {}
    for spec in feature_specs:
        feature_name = str(spec["name"])
        try_import_dataset_feature(dataset, feature_name)
        plugin = get_feature(feature_name)
        out_path = output_dir / f"{feature_name}.json"
        if out_path.exists() and not force:
            cfgs_out[feature_name] = json.loads(out_path.read_text(encoding="utf-8"))
            continue
        filtered_rows = _apply_prefilter(rows, spec.get("prefilter") or {})
        sample_row = filtered_rows[0] if filtered_rows else {"key": None}
        static_payload = project_match_fields(sample_row, match_fields, secondary=None)
        attrs = plugin.infer_attributes(None, static_payload, spec.get("attributes") or {})
        cfg_payload = dict(template.get(feature_name, {}))
        cfg_payload.setdefault("attributes", {})
        cfg_payload["attributes"].update(attrs.get("attributes", attrs))
        out_path.write_text(json.dumps(cfg_payload, indent=2, sort_keys=True), encoding="utf-8")
        cfgs_out[feature_name] = cfg_payload
    return cfgs_out
