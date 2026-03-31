from __future__ import annotations

import pandas as pd
from pathlib import Path
from typing import Any, Callable

from .utils import normalize_col


def build_brickdata(
    read_fn: Callable[..., pd.DataFrame],
    field_fn: Callable[..., dict[str, Any]],
    *,
    input_dir: Path,
    cfg: dict[str, Any],
    inventory_df: pd.DataFrame,
    out_path: Path,
    brick_cfg: dict[str, Any],
) -> pd.DataFrame:
    brick_ids = read_fn(input_dir=input_dir, cfg=cfg, inventory_df=inventory_df, brick_cfg=brick_cfg)
    required = {"key", "label", "brick_uri", "brick_class"}
    if not hasattr(brick_ids, "columns") or not required.issubset(brick_ids.columns):
        raise ValueError(f"brickdata_reader must return columns {sorted(required)}")
    normalize_col(brick_ids, "key")
    rows: list[dict[str, Any]] = []
    for row in brick_ids.to_dict(orient="records"):
        fields = field_fn(row, brick_cfg) or {}
        full_row = {"key": str(row["key"])}
        full_row.update(fields)
        rows.append(full_row)
    brick_df = pd.DataFrame.from_records(rows)
    if brick_df.empty:
        brick_df = pd.DataFrame({"key": pd.Series(dtype="string")})
    brick_df = brick_df.dropna(subset=["key"]).drop_duplicates(subset=["key"]).reset_index(drop=True)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    brick_df.to_feather(out_path)
    return brick_df
