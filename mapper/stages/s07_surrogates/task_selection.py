from __future__ import annotations

from mapper.stages.common import StageTaskRequest


def build_training_request(request: StageTaskRequest) -> dict[str, object]:
    return {
        "zone": request.config.get("zone"),
        "task_name": request.task_name,
        "progress_style": "two_line",
    }


def build_rollout_request(request: StageTaskRequest) -> dict[str, object]:
    return {
        "zone": request.config.get("zone"),
        "horizon_steps": int(request.config.get("horizon_steps", 1)),
        "progress_style": "two_line",
    }
