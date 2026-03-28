from __future__ import annotations

from mapper.stages.common import StageTaskRequest, StageTaskResult

from .similarity_task import build_similarity_graph


class BuildGraphTask:
    name = "build_graph"

    def run(self, request: StageTaskRequest) -> StageTaskResult:
        return build_similarity_graph(request)


TASKS = {"build_graph": BuildGraphTask()}
