from __future__ import annotations

from mapper.stages.common import ArtifactBundle, StageTaskRequest, StageTaskResult, StageWarning


def write_gnn_outputs(request: StageTaskRequest) -> dict[str, object]:
    return {"work_dir": str(request.data_dir), "input_keys": sorted(request.input_refs)}


def run_gnn_training(request: StageTaskRequest) -> StageTaskResult:
    reporter = request.reporter
    if reporter is not None:
        reporter.info(
            "training_config",
            epochs=request.config.get("epochs"),
            lr=request.config.get("lr"),
            input_count=len(request.input_refs),
        )
    return StageTaskResult(
        status="not_implemented",
        artifact_bundle=ArtifactBundle(),
        warnings=(StageWarning(code="not_implemented", message="Stage 5 GNN scaffold only"),),
        extras=write_gnn_outputs(request),
    )
