from __future__ import annotations

from mapper.stages.common import ArtifactBundle, StageTaskRequest, StageTaskResult, StageWarning

from .config_loader import load_stage2_config, resolve_process_chain
from .process_registry import list_process_names


def run_preprocessing_pipeline(request: StageTaskRequest) -> StageTaskResult:
    stage_cfg = load_stage2_config(dict(request.config))
    reporter = request.reporter
    if reporter is not None:
        reporter.step_start("config_loader", config_name=stage_cfg.get("config_name", "inline"))
    process_chain = resolve_process_chain(stage_cfg)
    available = list_process_names()
    if reporter is not None:
        reporter.step_done("config_loader", processes=len(process_chain))
        reporter.info("process_chain", process_chain=",".join(process_chain))
    return StageTaskResult(
        status="not_implemented",
        artifact_bundle=ArtifactBundle(),
        warnings=(StageWarning(code="not_implemented", message="Stage 2 preprocessing scaffold only"),),
        extras={
            "process_chain": process_chain,
            "available_processes": available,
            "input_keys": sorted(request.input_refs),
        },
    )


class BuildTask:
    name = "build"

    def run(self, request: StageTaskRequest) -> StageTaskResult:
        return run_preprocessing_pipeline(request)


TASKS = {"build": BuildTask()}
