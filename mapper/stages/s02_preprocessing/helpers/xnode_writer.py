from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import h5py
import pandas as pd


class PreprocWriter:
    def __init__(self, parent_path: Path) -> None:
        parent_path.mkdir(parents=True, exist_ok=True)
        self._ts_path = Path(parent_path / "xnode_ts.h5")
        self._meta_path = Path(parent_path / "xnode_meta.feather")
        self._ts: h5py.File | None = None
        self._meta: list[dict[str, Any]] = []

    @property
    def ts_path(self) -> Path:
        return self._ts_path

    @property
    def meta_path(self) -> Path:
        return self._meta_path

    def __enter__(self) -> "PreprocWriter":
        self._ts = h5py.File(str(self._ts_path), "a")
        return self

    def __exit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
        try:
            meta_df = pd.DataFrame(self._meta) if self._meta else pd.DataFrame(columns=pd.Index(["key"]))
            self._meta_path.parent.mkdir(parents=True, exist_ok=True)
            meta_df.to_feather(self._meta_path)
        finally:
            if self._ts is not None:
                self._ts.flush()
                self._ts.close()
                self._ts = None

    def write_node_meta(self, key: str, static_table: dict[str, Any]) -> None:
        row: dict[str, Any] = {"key": key}
        for name, value in (static_table or {}).items():
            if isinstance(value, (dict, list)):
                row[f"{name}_json"] = json.dumps(value, ensure_ascii=False)
            else:
                row[name] = value
        self._meta.append(row)

    def write_node_ts(
        self,
        key: str,
        series: pd.Series,
        flag: pd.Series | None,
        chunk_len: int = 8192,
    ) -> None:
        if self._ts is None:
            raise RuntimeError("PreprocWriter must be used as a context manager")
        if len(series) <= 0:
            raise ValueError(f"Need a non-empty series for key '{key}'")

        t_series = series.index.view("int64")
        v_series = series.values.astype("float32", copy=False)
        f_series = None if flag is None else flag.reindex(series.index).fillna(0).astype("int8").values
        chunks = (min(len(series), chunk_len),)

        group = self._ts.require_group(f"nodes/{key}")
        for name in ("time", "value", "flag"):
            if name in group:
                del group[name]
        group.create_dataset("time", data=t_series, compression="gzip", compression_opts=4, shuffle=True, chunks=chunks)
        group.create_dataset("value", data=v_series, compression="gzip", compression_opts=4, shuffle=True, chunks=chunks)
        if f_series is not None:
            group.create_dataset("flag", data=f_series, compression="gzip", compression_opts=4, shuffle=True, chunks=chunks)


def build_node_ledger(ts_store: Path, meta_store: Path, written_keys: list[str], out_path: Path) -> Path:
    keys = (
        pd.Series(written_keys, dtype="string")
        .dropna()
        .drop_duplicates()
        .sort_values()
        .reset_index(drop=True)
    )
    ledger_df = pd.DataFrame(
        {
            "key": keys,
            "xnode_ts_store": str(ts_store),
            "xnode_meta_store": str(meta_store),
        }
    )
    out_path.parent.mkdir(parents=True, exist_ok=True)
    ledger_df.to_feather(out_path)
    return out_path
