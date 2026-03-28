from __future__ import annotations

from mapper.stages.common import ArtifactBundle, StageTaskRequest, StageTaskResult, StageWarning


def export_embeddings(request: StageTaskRequest) -> dict[str, object]:
    return {"output_root": str(request.data_dir), "input_keys": sorted(request.input_refs)}


def run_embedding_training(request: StageTaskRequest) -> StageTaskResult:
    return StageTaskResult(
        status="not_implemented",
        artifact_bundle=ArtifactBundle(),
        warnings=(StageWarning(code="not_implemented", message="Stage 3 embedding scaffold only"),),
        extras=export_embeddings(request),
    )
