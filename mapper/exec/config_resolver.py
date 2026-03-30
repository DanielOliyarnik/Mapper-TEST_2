from __future__ import annotations

from dataclasses import dataclass
import json
import yaml
from pathlib import Path
from typing import Any, Iterable, Mapping

from mapper.stages.common import StageSpec


@dataclass(frozen=True)
class ResolvedRunConfig:
    run_name: str
    dataset_id: str
    output_root: Path
    requested_stages: tuple[str, ...]
    retrain_stages: tuple[str, ...]
    stage_task_names: dict[str, str]
    reporting_cfg: dict[str, Any]
    raw_cfg: dict[str, Any]


def _selection_from_args(args: Iterable[str] | None, stage_specs: dict[str, StageSpec], default_all: bool) -> tuple[str, ...]:
    if not args:
        return tuple(stage_specs) if default_all else ()
    raw = [str(arg) for arg in args]
    if raw == ["0"]:
        return tuple(stage_specs)
    by_order = {str(spec.order): name for name, spec in stage_specs.items()}
    selected: list[str] = []
    for item in raw:
        if item in by_order:
            selected.append(by_order[item])
        elif item in stage_specs:
            selected.append(item)
        else:
            raise ValueError(f"Unknown stage selection: {item}")
    return tuple(dict.fromkeys(selected))


def normalize_stage_selection(args: Iterable[str] | None, stage_specs: dict[str, StageSpec]) -> tuple[str, ...]:
    return _selection_from_args(args, stage_specs, default_all=True)


def normalize_retrain_selection(args: Iterable[str] | None, stage_specs: dict[str, StageSpec]) -> tuple[str, ...]:
    return _selection_from_args(args, stage_specs, default_all=False)


def load_run_config(path: str | Path) -> dict[str, Any]:
    cfg_path = Path(path)
    text = cfg_path.read_text(encoding="utf-8").strip()
    if not text:
        return {}
    suffix = cfg_path.suffix.lower()
    if suffix not in {".json", ".yaml", ".yml"}:
        raise TypeError(f"Unsupported config extension for {cfg_path}")
    if suffix == ".json":
        try:
            loaded = json.loads(text)
        except json.JSONDecodeError as exc:
            raise ValueError(f"Invalid JSON config: {cfg_path}") from exc
    else:
        loaded = yaml.safe_load(text)
    if not isinstance(loaded, dict):
        raise TypeError(f"Config root must be an object: {cfg_path}")
    return loaded


def resolve_dataset_id(cfg: dict[str, Any]) -> str:
    data_source = cfg.get("DATA_SOURCE", {})
    dataset_id = data_source.get("dataset") or cfg.get("dataset")
    if not dataset_id:
        raise ValueError("Run config must define DATA_SOURCE.dataset or dataset")
    return str(dataset_id)


def resolve_enabled_stages(
    cfg: dict[str, Any],
    requested: Iterable[str] | None,
    stage_specs: dict[str, StageSpec],
) -> tuple[str, ...]:
    _ = cfg
    return normalize_stage_selection(requested, stage_specs)


def resolve_stage_task_name(cfg: dict[str, Any], stage_name: str, stage_def: Any) -> str:
    stage_cfg = cfg.get(stage_name, {})
    task_name = stage_cfg.get("task") or stage_cfg.get("task_name") or stage_def.spec.default_task
    task_name = str(task_name)
    if task_name not in stage_def.spec.supported_tasks:
        raise ValueError(f"Unsupported task for {stage_name}: {task_name}")
    return task_name


def resolve_stage_task_config(cfg: dict[str, Any], stage_name: str, task_name: str) -> dict[str, Any]:
    data_source = dict(cfg.get("DATA_SOURCE", {}))
    stage_cfg = dict(data_source)
    stage_cfg.update(dict(cfg.get(stage_name, {})))
    if "dataset" not in stage_cfg and "dataset" in cfg:
        stage_cfg["dataset"] = cfg["dataset"]
    if "dataset" not in stage_cfg:
        raise ValueError(f"Run config must define dataset for stage {stage_name}")
    stage_cfg["task_name"] = task_name
    return stage_cfg


def snapshot_stage_config(cfg: Mapping[str, Any], out_path: Path) -> Path:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(dict(cfg), indent=2, sort_keys=True, default=str), encoding="utf-8")
    return out_path


def resolve_reporting_config(cfg: dict[str, Any]) -> dict[str, Any]:
    exec_cfg = cfg.get("exec", {})
    reporting_cfg = dict(exec_cfg.get("reporting", {}))
    reporting_cfg.setdefault("console", True)
    reporting_cfg.setdefault("progress", True)
    reporting_cfg.setdefault("progress_style", "single_line")
    reporting_cfg.setdefault("progress_every", 10)
    reporting_cfg.setdefault("progress_min_seconds", 1.0)
    reporting_cfg.setdefault("show_eta", True)
    return reporting_cfg


def normalize_run_config(
    cfg: dict[str, Any],
    *,
    run_name: str,
    stages: Iterable[str] | None,
    retrain: Iterable[str] | None,
    stage_specs: dict[str, StageSpec],
    stage_defs: Mapping[str, Any] | None = None,
) -> ResolvedRunConfig:
    dataset_id = resolve_dataset_id(cfg)
    output_root = Path(cfg.get("output_root", "project-data"))
    requested_stages = resolve_enabled_stages(cfg, stages, stage_specs)
    retrain_stages = normalize_retrain_selection(retrain, stage_specs)
    stage_task_names = {
        stage_name: resolve_stage_task_name(cfg, stage_name, stage_defs[stage_name])
        for stage_name in requested_stages
        if stage_defs is not None
    }
    reporting_cfg = resolve_reporting_config(cfg)
    return ResolvedRunConfig(
        run_name=run_name,
        dataset_id=dataset_id,
        output_root=output_root,
        requested_stages=requested_stages,
        retrain_stages=retrain_stages,
        stage_task_names=stage_task_names,
        reporting_cfg=reporting_cfg,
        raw_cfg=cfg,
    )
