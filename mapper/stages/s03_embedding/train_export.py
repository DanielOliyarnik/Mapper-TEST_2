from __future__ import annotations

from mapper.stages.common import ArtifactBundle, StageTaskRequest, StageTaskResult, StageWarning


def export_embeddings(request: StageTaskRequest) -> dict[str, object]:
    return {"output_root": str(request.data_dir), "input_keys": sorted(request.input_refs)}


def run_embedding_training(request: StageTaskRequest) -> StageTaskResult:
    reporter = request.reporter
    if reporter is not None:
        reporter.info(
            "config",
            dataset=request.dataset_id,
            input_count=len(request.input_refs),
            epochs=request.config.get("epochs"),
            batch_size=request.config.get("batch_size"),
        )
    return StageTaskResult(
        status="not_implemented",
        artifact_bundle=ArtifactBundle(),
        warnings=(StageWarning(code="not_implemented", message="Stage 3 embedding scaffold only"),),
        extras=export_embeddings(request),
    )
