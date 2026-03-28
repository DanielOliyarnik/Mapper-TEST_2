from __future__ import annotations

from mapper.surrogate_engine.contracts import LoadedSurrogateRuntime, ZoneRolloutRequest
from mapper.surrogate_engine.zone_rollout import run_zone_rollout

from .contracts import OptimizerRolloutRequest


def call_surrogate_engine(runtime: LoadedSurrogateRuntime, request: OptimizerRolloutRequest):
    zone_request = ZoneRolloutRequest(
        runtimes=(runtime,),
        horizon_steps=request.forecast.horizon_steps,
        initial_state=request.state.observed_state,
        exogenous_plan=request.forecast.channels,
    )
    return run_zone_rollout(zone_request)
