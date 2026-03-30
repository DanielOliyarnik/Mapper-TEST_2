from __future__ import annotations

from typing import TYPE_CHECKING

from .hierarchy_builder import build_hierarchy
from .utils import load_standard_output_config

if TYPE_CHECKING:
    import pandas as pd


def merge_fields(
    main_cfg: dict,
    inventory_df: "pd.DataFrame",
    sources: list[tuple[str, "pd.DataFrame"]],
) -> "pd.DataFrame":
    import pandas as pd

    if "key" not in inventory_df.columns:
        raise ValueError("merge_fields requires inventory_df['key']")
    keys = inventory_df.loc[:, ["key"]].copy()
    merged_df = keys.copy()
    out_df = keys.copy()

    std_cfg = load_standard_output_config(main_cfg)
    precedence = std_cfg.get("merge_precedence") or {}

    for name, df in sources:
        if df is None or df.empty or "key" not in df.columns:
            continue
        cols = [column for column in df.columns if column != "key"]
        tmp = df.loc[:, ["key"] + cols].copy()
        tmp.columns = ["key"] + [f"{column}__{name}" for column in cols]
        merged_df = merged_df.merge(tmp, on="key", how="left", copy=False)

    def _pick_col(field: str) -> "pd.Series":
        ordered = [f"{field}__{source_name}" for source_name in precedence.get(field) or []]
        candidates = [column for column in merged_df.columns if column.startswith(field + "__")]
        cols = [column for column in ordered if column in merged_df.columns] or candidates
        if not cols:
            return pd.Series([""] * len(merged_df), index=merged_df.index, dtype="string")
        result = merged_df[cols[0]].astype("string")
        for column in cols[1:]:
            result = result.fillna(merged_df[column].astype("string"))
        return result.fillna("")

    for field_name in ("label", "role", "location", "unit", "unit_candidates"):
        out_df[field_name] = _pick_col(field_name)

    if any(column.startswith("brick_uri__") for column in merged_df.columns):
        out_df["brick_uri"] = _pick_col("brick_uri")

    out_df["hierarchy"] = build_hierarchy(out_df, std_cfg.get("hierarchy") or {})
    return out_df
