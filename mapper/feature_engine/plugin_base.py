from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any, Callable


@dataclass(frozen=True)
class FeatureResult:
    matched: bool
    details: dict[str, Any]


class FeaturePlugin(ABC):
    name: str

    @abstractmethod
    def build_criteria(
        self,
        feature_def: dict[str, Any],
        template_cfg: dict[str, Any],
        criteria_overrides: dict[str, Any],
    ) -> dict[str, Any]:
        raise NotImplementedError

    def calibrate_criteria(
        self,
        criteria: dict[str, Any],
        candidate_rows: list[dict[str, Any]],
        read_fn: Callable[[str], Any] | None,
    ) -> dict[str, Any]:
        return criteria

    def match(self, series: Any, static: dict[str, Any], criteria: dict[str, Any]) -> FeatureResult:
        from .feature_match import match_resolved_criteria

        return match_resolved_criteria(series, static, criteria)


def label_in_list(search: str, search_list: list[str]) -> bool:
    search_norm = (search or "").upper()
    return any((item or "").upper() in search_norm for item in (search_list or []))
