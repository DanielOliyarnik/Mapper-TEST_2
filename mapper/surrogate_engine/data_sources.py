from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Mapping


@dataclass(frozen=True)
class HistoricalSource:
    values: Mapping[str, Any]


@dataclass(frozen=True)
class CustomSource:
    values: Mapping[str, Any]


@dataclass(frozen=True)
class HybridSource:
    history_source: HistoricalSource
    forecast_values: Mapping[str, Any]
