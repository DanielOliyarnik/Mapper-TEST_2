from __future__ import annotations

from mapper.surrogate_engine.runtime_loader import load_runtime_from_artifacts
from mapper.surrogate_engine.zone_rollout import run_zone_rollout
from mapper.surrogate_engine.contracts import ZoneRolloutRequest


def coordinate_zone_rollout(zone_root, *, horizon_steps: int = 1, initial_state: dict | None = None, exogenous_plan: dict | None = None):
    runtime = load_runtime_from_artifacts(zone_root)
    request = ZoneRolloutRequest(
        runtimes=(runtime,),
        horizon_steps=horizon_steps,
        initial_state=initial_state or {},
        exogenous_plan=exogenous_plan or {},
    )
    return run_zone_rollout(request)
