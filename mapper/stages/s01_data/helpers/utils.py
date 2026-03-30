from __future__ import annotations

import json
from pathlib import Path
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    import pandas as pd


def normalize_col(df: "pd.DataFrame", col: str = "key") -> None:
    if col not in df.columns:
        raise KeyError(f"normalize_col: missing column {col!r}")
    values = df[col].astype("string", copy=False).str.strip()
    df.loc[:, col] = values.mask(values.eq(""), None)
    df.dropna(subset=[col], inplace=True)
    df.drop_duplicates(subset=[col], inplace=True)
    df.reset_index(drop=True, inplace=True)


def load_standard_output_config(main_cfg: dict[str, Any]) -> dict[str, Any]:
    stage_root = Path(__file__).resolve().parents[1]
    configured = str(main_cfg.get("standard_output_config") or "").strip()
    if configured:
        cfg_path = Path(configured)
        if not cfg_path.is_absolute():
            cfg_path = stage_root / cfg_path
    else:
        cfg_path = stage_root / "configs" / "STANDARD_OUTPUT.json"
    if not cfg_path.exists():
        raise FileNotFoundError(f"Stage 1 STANDARD_OUTPUT config not found: {cfg_path}")
    try:
        loaded = json.loads(cfg_path.read_text(encoding="utf-8"))
    except Exception as exc:
        raise RuntimeError(f"Failed to load Stage 1 STANDARD_OUTPUT config {cfg_path}: {exc}") from exc
    if not isinstance(loaded, dict):
        raise TypeError(f"Stage 1 STANDARD_OUTPUT config must be an object: {cfg_path}")
    return loaded
