from __future__ import annotations

from mapper.stages.common import StageSpec

from .artifacts import CONTRACTS
from .tasks import TASKS

SPEC = StageSpec(
    name="s05_gnn",
    order=5,
    default_task="train",
    supported_tasks=("train",),
    required_upstream=("s04_similarity",),
    description="Train GNN over similarity graph outputs.",
)


class Stage:
    spec = SPEC

    def get_task(self, task_name: str):
        return TASKS[task_name]

    def list_tasks(self):
        return dict(TASKS)

    def get_artifact_contract(self, task_name: str):
        return CONTRACTS[task_name]
