from __future__ import annotations

from .contracts import RolloutResult, SingleRolloutRequest


def run_single_rollout(request: SingleRolloutRequest) -> RolloutResult:
    predictions = {"zone": request.runtime.zone_name, "horizon_steps": request.horizon_steps}
    summary = {"runtime_root": str(request.runtime.root), "status": "scaffold"}
    return RolloutResult(status="success", predictions=predictions, summary=summary)
