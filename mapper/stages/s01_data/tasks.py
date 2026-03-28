from __future__ import annotations

from mapper.stages.common import ArtifactBundle, StageTaskRequest, StageTaskResult, StageWarning

from .dataset_loader import load_dataset


def build_stage_data(request: StageTaskRequest) -> StageTaskResult:
    dataset_name = str(request.config.get("dataset") or request.dataset_id)
    reporter = request.reporter
    if reporter is not None:
        reporter.step_start("dataset_loader", dataset=dataset_name)
    dataset = load_dataset(dataset_name, dict(request.config), reporter=reporter)
    if reporter is not None:
        reporter.step_done("dataset_loader", dataset=dataset_name, dataset_class=dataset.__class__.__name__)
    return StageTaskResult(
        status="not_implemented",
        artifact_bundle=ArtifactBundle(),
        warnings=(StageWarning(code="not_implemented", message="Stage 1 ingest scaffold only"),),
        extras={"dataset_name": dataset_name, "dataset_class": dataset.__class__.__name__},
    )


class BuildTask:
    name = "build"

    def run(self, request: StageTaskRequest) -> StageTaskResult:
        return build_stage_data(request)


TASKS = {"build": BuildTask()}
