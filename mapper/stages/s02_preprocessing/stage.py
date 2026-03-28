from __future__ import annotations

from mapper.stages.common import StageSpec

from .artifacts import CONTRACTS
from .tasks import TASKS

SPEC = StageSpec(
    name="s02_preprocessing",
    order=2,
    default_task="build",
    supported_tasks=("build",),
    required_upstream=("s01_data",),
    description="Build canonical aligned node inputs for downstream stages.",
)


class Stage:
    spec = SPEC

    def get_task(self, task_name: str):
        return TASKS[task_name]

    def list_tasks(self):
        return dict(TASKS)

    def get_artifact_contract(self, task_name: str):
        return CONTRACTS[task_name]
