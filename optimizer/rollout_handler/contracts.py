from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Mapping


@dataclass(frozen=True)
class ForecastPlan:
    horizon_steps: int
    channels: Mapping[str, list[float]]


@dataclass(frozen=True)
class CandidateActionPlan:
    setpoints: Mapping[str, list[float]]


@dataclass(frozen=True)
class OptimizerStateSlice:
    timestamp: str
    observed_state: Mapping[str, Any]


@dataclass(frozen=True)
class OptimizerRolloutRequest:
    state: OptimizerStateSlice
    forecast: ForecastPlan
    candidate: CandidateActionPlan
