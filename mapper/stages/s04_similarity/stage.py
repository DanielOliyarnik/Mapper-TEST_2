from __future__ import annotations

from mapper.stages.common import StageSpec

from .artifacts import CONTRACTS
from .tasks import TASKS

SPEC = StageSpec(
    name="s04_similarity",
    order=4,
    default_task="build_graph",
    supported_tasks=("build_graph",),
    required_upstream=("s02_preprocessing", "s03_embedding"),
    description="Build similarity graph from embeddings and node metadata.",
)


class Stage:
    spec = SPEC

    def get_task(self, task_name: str):
        return TASKS[task_name]

    def list_tasks(self):
        return dict(TASKS)

    def get_artifact_contract(self, task_name: str):
        return CONTRACTS[task_name]
