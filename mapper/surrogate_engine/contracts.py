from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Mapping


@dataclass(frozen=True)
class LoadedSurrogateRuntime:
    zone_name: str
    root: Path
    model_paths: Mapping[str, Path] = field(default_factory=dict)


@dataclass(frozen=True)
class SingleRolloutRequest:
    runtime: LoadedSurrogateRuntime
    horizon_steps: int
    initial_state: Mapping[str, Any]
    exogenous_plan: Mapping[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class ZoneRolloutRequest:
    runtimes: tuple[LoadedSurrogateRuntime, ...]
    horizon_steps: int
    initial_state: Mapping[str, Any]
    exogenous_plan: Mapping[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class RolloutResult:
    status: str
    predictions: Mapping[str, Any]
    summary: Mapping[str, Any] = field(default_factory=dict)
