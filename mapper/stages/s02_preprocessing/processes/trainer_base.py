from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any


class TrainerBase(ABC):
    name: str

    @abstractmethod
    def fit_trainer(self, *args: Any, **kwargs: Any) -> Any:
        raise NotImplementedError
