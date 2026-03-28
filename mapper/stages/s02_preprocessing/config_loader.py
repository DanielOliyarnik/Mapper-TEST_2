from __future__ import annotations

from pathlib import Path
from typing import Any


def load_stage2_config(stage_cfg: dict[str, Any]) -> dict[str, Any]:
    config = dict(stage_cfg)
    config.setdefault(
        "process_chain",
        ["align_clean", "unit_selection", "smoothing_filtering", "flagging", "features_constructor", "static_encode"],
    )
    return config


def resolve_process_chain(stage_cfg: dict[str, Any]) -> list[str]:
    return [str(item) for item in load_stage2_config(stage_cfg).get("process_chain", [])]


def resolve_process_config_path(process_root: Path, config_name: str) -> Path:
    return process_root / "configs" / config_name
