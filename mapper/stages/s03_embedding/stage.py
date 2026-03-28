from __future__ import annotations

from mapper.stages.common import StageSpec

from .artifacts import CONTRACTS
from .tasks import TASKS

SPEC = StageSpec(
    name="s03_embedding",
    order=3,
    default_task="train_export",
    supported_tasks=("train_export",),
    required_upstream=("s02_preprocessing",),
    description="Train embedding model and export per-node embeddings.",
)


class Stage:
    spec = SPEC

    def get_task(self, task_name: str):
        return TASKS[task_name]

    def list_tasks(self):
        return dict(TASKS)

    def get_artifact_contract(self, task_name: str):
        return CONTRACTS[task_name]
