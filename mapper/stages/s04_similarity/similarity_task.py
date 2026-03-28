from __future__ import annotations

from mapper.stages.common import ArtifactBundle, StageTaskRequest, StageTaskResult, StageWarning


def build_similarity_graph(request: StageTaskRequest) -> StageTaskResult:
    return StageTaskResult(
        status="not_implemented",
        artifact_bundle=ArtifactBundle(),
        warnings=(StageWarning(code="not_implemented", message="Stage 4 similarity scaffold only"),),
        extras={"input_keys": sorted(request.input_refs)},
    )
