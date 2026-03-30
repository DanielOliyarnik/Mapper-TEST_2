from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING, Any, Callable

from .utils import normalize_col

if TYPE_CHECKING:
    import pandas as pd


def build_metadata(
    read_fn: Callable[..., "pd.DataFrame"],
    field_fn: Callable[..., dict[str, Any]],
    *,
    input_dir: Path,
    cfg: dict[str, Any],
    inventory_df: "pd.DataFrame",
    out_path: Path,
    meta_cfg: dict[str, Any],
) -> "pd.DataFrame":
    import pandas as pd

    meta_ids = read_fn(input_dir=input_dir, cfg=cfg, inventory_df=inventory_df, meta_cfg=meta_cfg)
    if not hasattr(meta_ids, "columns") or "key" not in meta_ids.columns:
        raise ValueError("metadata_reader must return a DataFrame with 'key'")
    normalize_col(meta_ids, "key")
    rows: list[dict[str, Any]] = []
    for row in meta_ids.to_dict(orient="records"):
        fields = field_fn(row, meta_cfg) or {}
        full_row = {"key": str(row["key"])}
        full_row.update(fields)
        rows.append(full_row)
    metadata_df = pd.DataFrame.from_records(rows)
    if metadata_df.empty:
        metadata_df = pd.DataFrame({"key": pd.Series(dtype="string")})
    metadata_df = metadata_df.dropna(subset=["key"]).drop_duplicates(subset=["key"]).reset_index(drop=True)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    metadata_df.to_feather(out_path)
    return metadata_df
