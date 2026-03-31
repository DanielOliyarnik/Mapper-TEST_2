from __future__ import annotations

import pandas as pd

from .hierarchy_builder import build_hierarchy
from .utils import load_standard_output_config


STANDARD_FIELDS = ("label", "role", "location", "unit", "unit_candidates")


def merge_fields(
    main_cfg: dict,
    inventory_df: pd.DataFrame,
    sources: list[tuple[str, pd.DataFrame]],
) -> pd.DataFrame:
    if "key" not in inventory_df.columns:
        raise ValueError("merge_fields requires inventory_df['key']")
    keys = inventory_df.loc[:, ["key"]].copy()
    merged_df = keys.copy()
    out_df = keys.copy()

    std_cfg = load_standard_output_config(main_cfg)
    precedence = std_cfg["merge_precedence"]

    for name, df in sources:
        if df is None or df.empty:
            continue
        if "key" not in df.columns:
            raise ValueError(f"merge_fields source {name!r} is missing 'key'")
        cols = [column for column in df.columns if column != "key"]
        tmp = df.loc[:, ["key"] + cols].copy()
        tmp.columns = ["key"] + [f"{column}__{name}" for column in cols]
        merged_df = merged_df.merge(tmp, on="key", how="left", copy=False)

    def _pick_col(field: str) -> pd.Series:
        ordered = [f"{field}__{source_name}" for source_name in precedence.get(field, []) if f"{field}__{source_name}" in merged_df.columns]
        candidates = [column for column in merged_df.columns if column.startswith(field + "__")]
        cols = ordered or candidates
        if not cols:
            return pd.Series([pd.NA] * len(merged_df), index=merged_df.index, dtype="string")
        result = merged_df[cols[0]].astype("string").replace("", pd.NA)
        for column in cols[1:]:
            result = result.fillna(merged_df[column].astype("string").replace("", pd.NA))
        return result

    for field_name in STANDARD_FIELDS:
        out_df[field_name] = _pick_col(field_name)

    if any(column.startswith("brick_uri__") for column in merged_df.columns):
        out_df["brick_uri"] = _pick_col("brick_uri")

    out_df["hierarchy"] = build_hierarchy(out_df, std_cfg["hierarchy"])
    return out_df
