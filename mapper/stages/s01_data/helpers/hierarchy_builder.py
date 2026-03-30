from __future__ import annotations

import re
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import pandas as pd


def _compile_rx_many(pattern_list: list[str] | None) -> list[re.Pattern[str]]:
    if not pattern_list:
        return []
    return [re.compile(f"({pattern})", re.IGNORECASE) for pattern in pattern_list if pattern]


def _build_search_list(df: "pd.DataFrame", source_list: list[str]) -> "pd.Series":
    import pandas as pd

    parts = []
    for source_name in source_list or []:
        if source_name in df.columns:
            parts.append(df[source_name].astype("string", copy=False).fillna(""))
    if not parts:
        return pd.Series([""] * len(df), index=df.index, dtype="string")
    out = parts[0]
    for part in parts[1:]:
        out = (out + " " + part).str.strip()
    return out


def _first_match_any(search_list: "pd.Series", rx_list: list[re.Pattern[str]]) -> "pd.Series":
    import pandas as pd

    if not rx_list:
        return pd.Series([""] * len(search_list), index=search_list.index, dtype="string")
    out = pd.Series([""] * len(search_list), index=search_list.index, dtype="string")
    mask = pd.Series([True] * len(search_list), index=search_list.index)
    for rx in rx_list:
        matches = search_list.str.extract(rx, expand=False).fillna("").astype("string")
        valid = mask & (matches != "")
        out.loc[valid] = matches.loc[valid]
        mask &= matches == ""
        if not mask.any():
            break
    return out


def build_hierarchy(df: "pd.DataFrame", hierarchy_cfg: dict) -> "pd.Series":
    import numpy as np
    import pandas as pd

    joiner = str(hierarchy_cfg.get("joiner") or " > ")
    search_list = _build_search_list(df, list(hierarchy_cfg.get("source_order") or []))
    parent = _first_match_any(search_list, _compile_rx_many(hierarchy_cfg.get("parent")))
    child = _first_match_any(search_list, _compile_rx_many(hierarchy_cfg.get("child")))
    label = df["label"] if "label" in df.columns else pd.Series([""] * len(df), index=df.index, dtype="string")

    p = parent.fillna("")
    c = child.fillna("")
    l = label.astype("string").fillna("")
    has_p = p != ""
    include_c = (c != "") & (c != p)
    include_l = (l != "") & (l != p) & (l != c)

    hierarchy = np.where(has_p, p, "")
    hierarchy = np.where(include_c & (hierarchy != ""), hierarchy + joiner + c, np.where(include_c, c, hierarchy))
    hierarchy = np.where(include_l & (hierarchy != ""), hierarchy + joiner + l, np.where(include_l, l, hierarchy))
    return pd.Series(hierarchy, index=df.index, dtype="string")
