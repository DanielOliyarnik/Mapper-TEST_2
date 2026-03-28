from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any


class TrainerBase(ABC):
    name: str

    def __init__(self, reporter: Any | None = None, progress: Any | None = None) -> None:
        self.reporter = reporter
        self.progress = progress

    def prog_tick(self, name: str, index: int, total: int, step: int | None = None) -> None:
        if total <= 0:
            return
        step_size = int(step or max(1, total // 20))
        if index == 1:
            self._emit(f"[trainer][{name}] start")
        if index == 1 or index % step_size == 0 or index >= total:
            pct = (index * 100) // max(1, total)
            self._emit(f"[trainer][{name}] {index}/{total} ({pct}%)")
            if self.progress is not None:
                if getattr(self.progress, "state", None) is None:
                    self.progress.start(f"trainer {name}", total=total, unit="step")
                self.progress.update(index, total=total, extra={"trainer": name})
        if index >= total:
            self._emit(f"[trainer][{name}] done")

    def _emit(self, message: str) -> None:
        if self.reporter is not None:
            self.reporter.info(message)
            return
        print(message)

    @abstractmethod
    def fit_trainer(self, *args: Any, **kwargs: Any) -> Any:
        raise NotImplementedError
