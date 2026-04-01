from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any

Payload = dict[str, Any]


class ProcessBase(ABC):
    def __init__(self, cfg: dict[str, Any] | None, reporter: Any | None = None, progress: Any | None = None) -> None:
        self.cfg = dict(cfg or {})
        self.reporter = reporter
        self.progress = progress
        self._state: dict[str, Any] | None = None
        self._progress: dict[str, dict[str, bool]] = {}

    def bind_runtime(self, *, reporter: Any | None = None, progress: Any | None = None) -> None:
        self.reporter = reporter
        self.progress = progress

    def set_state(self, state: dict[str, Any] | None) -> None:
        self._state = state

    def _emit(self, message: str) -> None:
        if self.reporter is not None:
            self.reporter.info(message)
            return
        print(message)

    def prog_tick(self, name: str, index: int, total: int, step: int | None = None) -> None:
        if total <= 0:
            return
        state = self._progress.setdefault(name, {"started": False, "done": False})
        prefix = f"[preprocess][{name}]"
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

    @abstractmethod
    def apply(self, proc_payload: Payload) -> Payload | None:
        raise NotImplementedError
