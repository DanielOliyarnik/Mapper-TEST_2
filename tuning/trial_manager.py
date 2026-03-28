from __future__ import annotations

from .contracts import TrialRequest, TrialResult
from .objective_extractor import extract_objectives


def run_trial(request: TrialRequest, pipeline_summary: dict | None = None) -> TrialResult:
    summary = pipeline_summary or {"status_code": 0.0}
    objectives = extract_objectives(summary)
    return TrialResult(
        trial_id=request.trial_spec.trial_id,
        status="success",
        objectives=objectives,
        metadata={"experiment_name": request.experiment_name},
    )
