from __future__ import annotations

from .contracts import RolloutResult, ZoneRolloutRequest
from .merge import merge_overlapping_outputs
from .single_rollout import run_single_rollout


def run_zone_rollout(request: ZoneRolloutRequest) -> RolloutResult:
    results = [
        run_single_rollout(
            request=type("SingleRequest", (), {
                "runtime": runtime,
                "horizon_steps": request.horizon_steps,
                "initial_state": request.initial_state,
                "exogenous_plan": request.exogenous_plan,
            })()
        )
        for runtime in request.runtimes
    ]
    merged = merge_overlapping_outputs(results)
    return RolloutResult(status="success", predictions={"zones": [runtime.zone_name for runtime in request.runtimes]}, summary=merged)
