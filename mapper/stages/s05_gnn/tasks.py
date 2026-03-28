from __future__ import annotations

from mapper.stages.common import StageTaskRequest, StageTaskResult

from .gnn_task import run_gnn_training


class TrainGnnTask:
    name = "train"

    def run(self, request: StageTaskRequest) -> StageTaskResult:
        return run_gnn_training(request)


TASKS = {"train": TrainGnnTask()}
