from __future__ import annotations

from mapper.stages.common import StageTaskRequest, StageTaskResult

from .train_export import run_embedding_training


class TrainExportTask:
    name = "train_export"

    def run(self, request: StageTaskRequest) -> StageTaskResult:
        return run_embedding_training(request)


TASKS = {"train_export": TrainExportTask()}
