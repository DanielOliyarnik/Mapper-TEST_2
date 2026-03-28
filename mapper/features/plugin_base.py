from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class FeatureResult:
    matched: bool
    details: dict[str, Any]


class FeaturePlugin(ABC):
    name: str

    @abstractmethod
    def infer_attributes(self, series: Any, static: dict[str, Any], params: dict[str, Any]) -> dict[str, Any]:
        raise NotImplementedError

    @abstractmethod
    def match(self, series: Any, static: dict[str, Any], config: dict[str, Any]) -> FeatureResult:
        raise NotImplementedError


def label_in_list(search: str, search_list: list[str]) -> bool:
    search_norm = (search or "").upper()
    return any((item or "").upper() in search_norm for item in (search_list or []))
