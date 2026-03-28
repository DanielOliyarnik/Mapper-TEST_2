from __future__ import annotations

from mapper.stages.common import ArtifactBundle, StageTaskRequest, StageTaskResult, StageWarning


def build_similarity_graph(request: StageTaskRequest) -> StageTaskResult:
    reporter = request.reporter
    if reporter is not None:
        reporter.info(
            "graph_build",
            input_count=len(request.input_refs),
            schema_path=request.config.get("schema_path"),
        )
    return StageTaskResult(
        status="not_implemented",
        artifact_bundle=ArtifactBundle(),
        warnings=(StageWarning(code="not_implemented", message="Stage 4 similarity scaffold only"),),
        extras={"input_keys": sorted(request.input_refs)},
    )
