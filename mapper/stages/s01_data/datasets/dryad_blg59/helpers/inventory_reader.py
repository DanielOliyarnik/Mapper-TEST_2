from __future__ import annotations

import re
import pandas as pd
from pathlib import Path
from typing import Any


EXCLUDE_PATTERNS = [r"^Unnamed:", r"^cerc_templogger_", r"^zone_cerc_"]


def _exclude_key(key: str, drop_patterns: list[str], drop_cerc: bool) -> bool:
    patterns = list(drop_patterns)
    if drop_cerc:
        patterns.extend([r"^cerc_templogger_", r"^zone_cerc_"])
    return any(re.search(pattern, key, flags=re.IGNORECASE) for pattern in patterns)


def read_inventory(input_dir: Path, cfg: dict[str, Any], ingest_cfg: dict[str, Any]) -> pd.DataFrame:
    building = str(ingest_cfg["building"]).strip()
    drop_cerc = bool(ingest_cfg["drop_cerc"])
    rows: list[dict[str, str]] = []
    seen: set[str] = set()
    for spec in ingest_cfg["files"]:
        rel_csv = str(spec["path"]).strip()
        csv_path = Path(input_dir) / rel_csv
        if not csv_path.exists():
            raise FileNotFoundError(f"dryad_blg59 timeseries file not found: {csv_path}")
        time_col = str(spec["time_column"])
        drop_patterns = list(spec["drop_columns"]) if "drop_columns" in spec else list(EXCLUDE_PATTERNS)
        source_group = str(spec["source_group"]).strip()
        cols = list(pd.read_csv(csv_path, nrows=0).columns)
        for col in cols:
            if col == time_col:
                continue
            key = str(col).strip()
            if not key or _exclude_key(key, drop_patterns, drop_cerc):
                continue
            if key in seen:
                raise ValueError(f"dryad_blg59 inventory collision for key: {key}")
            seen.add(key)
            rows.append(
                {
                    "key": key,
                    "label": key,
                    "building": building,
                    "source_file": str(csv_path),
                    "source_group": source_group,
                }
            )
    if not rows:
        raise ValueError("dryad_blg59 inventory_reader found no signal columns")
    _ = cfg
    return pd.DataFrame.from_records(rows)
