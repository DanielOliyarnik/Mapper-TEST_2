from __future__ import annotations

from mapper.stages.common import StageSpec

from .artifacts import CONTRACTS
from .tasks import TASKS

SPEC = StageSpec(
    name="s07_surrogates",
    order=7,
    default_task="train_zone",
    supported_tasks=("train_zone", "train_all", "rollout_one", "rollout_zone", "rollout_all"),
    required_upstream=("s02_preprocessing", "s05_gnn"),
    description="Train and run surrogate models for zone-level rollout experiments.",
)


class Stage:
    spec = SPEC

    def get_task(self, task_name: str):
        return TASKS[task_name]

    def list_tasks(self):
        return dict(TASKS)

    def get_artifact_contract(self, task_name: str):
        return CONTRACTS[task_name]
