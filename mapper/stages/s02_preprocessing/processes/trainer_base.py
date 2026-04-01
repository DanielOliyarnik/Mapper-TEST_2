from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any, Callable, Iterable

import pandas as pd


class TrainerBase(ABC):
    def __init__(self, cfg: dict[str, Any] | None, reporter: Any | None = None, progress: Any | None = None) -> None:
        self.cfg = dict(cfg or {})
        self.reporter = reporter
        self.progress = progress
        self._progress: dict[str, dict[str, bool]] = {}

    def bind_runtime(self, *, reporter: Any | None = None, progress: Any | None = None) -> None:
        self.reporter = reporter
        self.progress = progress

    def _emit(self, message: str) -> None:
        if self.reporter is not None:
            self.reporter.info(message)
            return
        print(message)

    def prog_tick(self, name: str, index: int, total: int, step: int | None = None) -> None:
        if total <= 0:
            return
        state = self._progress.setdefault(name, {"started": False, "done": False})
        prefix = f"[trainer][{name}]"
        if not state["started"]:
            self._emit(f"{prefix} start")
            state["started"] = True
        step_size = int(step or max(1, total // 20))
        if index == 1 or index % step_size == 0 or index >= total:
            pct = (index * 100) // max(1, total)
            self._emit(f"{prefix} {index}/{total} ({pct}%)")
        if index >= total and not state["done"]:
            self._emit(f"{prefix} done")
            state["done"] = True

    def build_read_fn(self, name: str, default_read_fn: Callable[[str], pd.Series | None]) -> Callable[[str], pd.Series | None]:
        align_cfg = self.cfg.get("align_for_train", False)
        if not align_cfg:
            return default_read_fn
        from .align_clean.process import Process as AlignCleanProcess

        align_proc = AlignCleanProcess(cfg=align_cfg, reporter=self.reporter, progress=self.progress)

        def _read_aligned(key: str) -> pd.Series | None:
            raw_series = default_read_fn(key)
            if raw_series is None or raw_series.empty:
                return None
            out = align_proc.apply({"series": raw_series})
            if out is None:
                return None
            series = out.get("series")
            if not isinstance(series, pd.Series) or series.empty:
                return None
            return series

        return _read_aligned

    @abstractmethod
    def fit(
        self,
        _read_fn: Callable[[str], pd.Series | None],
        keys: Iterable[str],
        meta_lookup: dict[str, dict[str, Any]],
        prev_state: dict[str, Any] | None = None,
        prev_hparams: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        raise NotImplementedError
