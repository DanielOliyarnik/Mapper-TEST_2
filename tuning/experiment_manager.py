from __future__ import annotations

from typing import Any

from .budget import TrialBudget
from .contracts import TrialRequest, TrialSpec
from .search_backends.random_search import propose_trial
from .search_space import normalize_search_space
from .trial_manager import run_trial


def run_experiment(experiment_name: str, base_cfg: dict[str, Any], raw_search_space: dict[str, Any], budget: TrialBudget) -> list[dict[str, Any]]:
    search_space = normalize_search_space(raw_search_space)
    results: list[dict[str, Any]] = []
    num_failed = 0
    num_started = 0
    while budget.allow_trial(num_started, num_failed):
        overrides = propose_trial(search_space, seed=num_started)
        trial_request = TrialRequest(
            experiment_name=experiment_name,
            base_cfg=base_cfg,
            trial_spec=TrialSpec(trial_id=f"trial_{num_started:04d}", stage_overrides=overrides),
        )
        result = run_trial(trial_request, pipeline_summary={"num_overrides": float(len(overrides))})
        results.append({"trial_id": result.trial_id, "status": result.status, "objectives": result.objectives})
        num_started += 1
        if result.status != "success":
            num_failed += 1
    return results
