from __future__ import annotations

from mapper.stages.common import ArtifactBundle, StageTaskRequest, StageTaskResult, StageWarning

from .rollout.run_rollout_all import run_rollout_all
from .rollout.run_rollout_one import run_rollout_one
from .rollout.run_rollout_zone import run_rollout_zone
from .task_selection import build_rollout_request, build_training_request


def _train_result(request: StageTaskRequest) -> StageTaskResult:
    reporter = request.reporter
    if reporter is not None:
        reporter.info("train_mode", zone=request.config.get("zone"), task=request.task_name)
    return StageTaskResult(
        status="not_implemented",
        artifact_bundle=ArtifactBundle(),
        warnings=(StageWarning(code="not_implemented", message="Stage 7 training scaffold only"),),
        extras=build_training_request(request),
    )


def _rollout_result(request: StageTaskRequest, mode: str) -> StageTaskResult:
    reporter = request.reporter
    if reporter is not None:
        reporter.info("rollout_mode", mode=mode, zone=request.config.get("zone"))
    payload = build_rollout_request(request)
    extras = {"mode": mode, **payload}
    return StageTaskResult(
        status="not_implemented",
        artifact_bundle=ArtifactBundle(),
        warnings=(StageWarning(code="not_implemented", message="Stage 7 rollout scaffold only"),),
        extras=extras,
    )


class TrainZoneTask:
    name = "train_zone"
    def run(self, request: StageTaskRequest) -> StageTaskResult:
        return _train_result(request)


class TrainAllTask:
    name = "train_all"
    def run(self, request: StageTaskRequest) -> StageTaskResult:
        return _train_result(request)


class RolloutOneTask:
    name = "rollout_one"
    def run(self, request: StageTaskRequest) -> StageTaskResult:
        _ = run_rollout_one
        return _rollout_result(request, self.name)


class RolloutZoneTask:
    name = "rollout_zone"
    def run(self, request: StageTaskRequest) -> StageTaskResult:
        _ = run_rollout_zone
        return _rollout_result(request, self.name)


class RolloutAllTask:
    name = "rollout_all"
    def run(self, request: StageTaskRequest) -> StageTaskResult:
        _ = run_rollout_all
        return _rollout_result(request, self.name)


TASKS = {
    "train_zone": TrainZoneTask(),
    "train_all": TrainAllTask(),
    "rollout_one": RolloutOneTask(),
    "rollout_zone": RolloutZoneTask(),
    "rollout_all": RolloutAllTask(),
}
