from __future__ import annotations

from typing import Protocol

from .artifacts import TaskArtifactContract
from .types import StageSpec, StageTaskRequest, StageTaskResult


class StageTask(Protocol):
    name: str

    def run(self, request: StageTaskRequest) -> StageTaskResult:
        ...


class StageDefinition(Protocol):
    spec: StageSpec

    def get_task(self, task_name: str) -> StageTask:
        ...

    def list_tasks(self) -> dict[str, StageTask]:
        ...

    def get_artifact_contract(self, task_name: str) -> TaskArtifactContract:
        ...
