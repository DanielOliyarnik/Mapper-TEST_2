from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Mapping


@dataclass(frozen=True)
class TrialSpec:
    trial_id: str
    stage_overrides: Mapping[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class TrialRequest:
    experiment_name: str
    base_cfg: Mapping[str, Any]
    trial_spec: TrialSpec


@dataclass(frozen=True)
class TrialResult:
    trial_id: str
    status: str
    objectives: Mapping[str, float] = field(default_factory=dict)
    metadata: Mapping[str, Any] = field(default_factory=dict)
