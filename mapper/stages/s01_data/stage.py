from __future__ import annotations

from mapper.stages.common import StageSpec

from .artifacts import CONTRACTS
from .tasks import TASKS

SPEC = StageSpec(
    name="s01_data",
    order=1,
    default_task="build",
    supported_tasks=("build",),
    required_upstream=(),
    description="Ingest raw dataset into canonical stores and ledger.",
)


class Stage:
    spec = SPEC

    def get_task(self, task_name: str):
        return TASKS[task_name]

    def list_tasks(self):
        return dict(TASKS)

    def get_artifact_contract(self, task_name: str):
        return CONTRACTS[task_name]
