from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any


class ProcessBase(ABC):
    name: str

    @abstractmethod
    def run_process(self, *args: Any, **kwargs: Any) -> Any:
        raise NotImplementedError
