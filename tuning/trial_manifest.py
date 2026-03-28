from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any, Mapping

from .contracts import TrialRequest, TrialResult


@dataclass
class TrialManifest:
    experiment_name: str
    trial_id: str
    status: str
    objectives: Mapping[str, float]
    metadata: Mapping[str, Any]


def build_trial_manifest(request: TrialRequest, result: TrialResult) -> dict[str, Any]:
    return asdict(TrialManifest(
        experiment_name=request.experiment_name,
        trial_id=result.trial_id,
        status=result.status,
        objectives=result.objectives,
        metadata=result.metadata,
    ))
