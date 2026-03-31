from __future__ import annotations

import pandas as pd
from pathlib import Path
from typing import Any, Callable

from .utils import normalize_col


def build_inventory(
    read_fn: Callable[[Path, dict[str, Any]], pd.DataFrame],
    *,
    input_dir: Path,
    cfg: dict[str, Any],
    out_path: Path,
) -> pd.DataFrame:
    inventory_df = read_fn(input_dir, cfg)
    if not hasattr(inventory_df, "columns"):
        raise TypeError("inventory_reader must return a DataFrame-like object")
    if "key" not in inventory_df.columns:
        raise ValueError("inventory_reader must return a 'key' column")
    normalize_col(inventory_df, "key")
    out_path.parent.mkdir(parents=True, exist_ok=True)
    inventory_df.to_feather(out_path)
    return inventory_df
