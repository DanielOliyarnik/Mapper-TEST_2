from __future__ import annotations

import pandas as pd
from pathlib import Path
from typing import Callable

from .merge_fields import merge_fields
from .utils import load_standard_output_config


TEXT_COLUMNS = {
    "key",
    "label",
    "role",
    "location",
    "unit",
    "unit_candidates",
    "hierarchy",
    "zone_id",
    "group_id",
    "equip_id",
    "class",
    "loop_ids",
    "brick_class",
    "source_file",
    "source_group",
    "source_unit_rule",
}


def _normalize_text_column(series: pd.Series) -> pd.Series:
    return series.astype("string").str.strip().replace("", pd.NA)


def build_otherdata(
    finalize_fn: Callable[..., pd.DataFrame],
    *,
    input_dir: Path,
    cfg: dict,
    inventory_df: pd.DataFrame,
    meta_df: pd.DataFrame,
    bricks_df: pd.DataFrame,
    out_path: Path,
) -> pd.DataFrame:
    std_cfg = load_standard_output_config(cfg)
    other_cfg = std_cfg["otherdata"]
    required_columns = list(other_cfg["required_columns"])
    optional_columns = list(other_cfg["optional_columns"])

    base_df = merge_fields(cfg, inventory_df, [("meta", meta_df), ("brick", bricks_df)])
    other_df = finalize_fn(
        base_df=base_df,
        input_dir=input_dir,
        cfg=cfg,
        inventory_df=inventory_df,
        meta_df=meta_df,
        bricks_df=bricks_df,
        other_cfg=other_cfg,
    )
    if "key" not in other_df.columns:
        raise ValueError("otherdata builder output must include 'key'")
    other_df = other_df.drop_duplicates(subset=["key"]).reset_index(drop=True)

    for column in list(other_df.columns):
        if column in TEXT_COLUMNS:
            other_df[column] = _normalize_text_column(other_df[column])

    if "label" in other_df.columns:
        other_df["label"] = other_df["label"].fillna(other_df["key"].astype("string"))
    if "role" in other_df.columns:
        other_df["role"] = other_df["role"].fillna("sensor")

    for column in required_columns + optional_columns:
        if column not in other_df.columns:
            other_df[column] = pd.Series([pd.NA] * len(other_df), index=other_df.index, dtype="string")

    ordered = required_columns + optional_columns
    passthrough = [column for column in other_df.columns if column not in ordered]
    other_df = other_df.loc[:, ordered + passthrough]

    out_path.parent.mkdir(parents=True, exist_ok=True)
    other_df.to_feather(out_path)
    return other_df
