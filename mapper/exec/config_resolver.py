from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable

from mapper.stages.common import StageSpec


@dataclass(frozen=True)
class ResolvedRunConfig:
    run_name: str
    dataset_id: str
    output_root: Path
    requested_stages: tuple[str, ...]
    retrain_stages: tuple[str, ...]
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


def normalize_run_config(
    cfg: dict[str, Any],
    *,
    run_name: str,
    stages: Iterable[str] | None,
    retrain: Iterable[str] | None,
    stage_specs: dict[str, StageSpec],
) -> ResolvedRunConfig:
    data_source = cfg.get("DATA_SOURCE", {})
    dataset_id = str(data_source.get("dataset") or cfg.get("dataset") or "unknown_dataset")
    output_root = Path(cfg.get("output_root", "project-data"))
    requested_stages = normalize_stage_selection(stages, stage_specs)
    retrain_stages = normalize_retrain_selection(retrain, stage_specs)
    return ResolvedRunConfig(
        run_name=run_name,
        dataset_id=dataset_id,
        output_root=output_root,
        requested_stages=requested_stages,
        retrain_stages=retrain_stages,
        raw_cfg=cfg,
    )
