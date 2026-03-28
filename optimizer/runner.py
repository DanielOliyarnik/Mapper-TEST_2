from __future__ import annotations

from mapper.surrogate_engine.runtime_loader import load_runtime_from_artifacts

from .constraints.rules.feasibility import check_feasibility
from .mpc.candidate_generation import generate_candidates
from .objectives.comfort import score_comfort
from .rollout_handler.engine_bridge import call_surrogate_engine
from .rollout_handler.result_adapter import adapt_rollout_result


def run_optimizer_step(runtime_root: str, rollout_request, *, bounds: dict | None = None, max_delta: float = 1.0) -> dict[str, object]:
    runtime = load_runtime_from_artifacts(runtime_root)
    best: dict[str, object] | None = None
    for candidate_request in generate_candidates(rollout_request):
        candidate_dict = getattr(candidate_request.candidate, "setpoints", {})
        if bounds and not check_feasibility(candidate_dict, bounds=bounds, max_delta=max_delta):
            continue
        rollout_result = call_surrogate_engine(runtime, candidate_request)
        adapted = adapt_rollout_result(rollout_result)
        score = score_comfort(adapted["summary"])
        if best is None or score < best["score"]:
            best = {"score": score, "result": adapted}
    return best or {"score": None, "result": {}}
