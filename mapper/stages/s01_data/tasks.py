from __future__ import annotations

import time
from pathlib import Path

from mapper.stages.common import ArtifactBundle, ArtifactRef, StageTaskRequest, StageTaskResult

from .dataset_loader import load_dataset


def _resolve_input_dir(request: StageTaskRequest) -> Path:
    configured = str(request.config.get("input_root") or request.config.get("input_path") or "").strip()
    if not configured:
        raise ValueError("Stage 1 requires input_path or input_root in the resolved stage config")
    path = Path(configured)
    return path if path.is_absolute() else (Path.cwd() / path)


def _build_output_paths(data_dir: Path) -> dict[str, Path]:
    return {
        "inventory": data_dir / "inventory.feather",
        "raw_store": data_dir / "raw_store.h5",
        "metadata": data_dir / "metadata.feather",
        "otherdata": data_dir / "otherdata.feather",
        "brickdata": data_dir / "brickdata.feather",
        "ledger": data_dir / "ledger.feather",
    }


def _build_artifact_bundle(paths: dict[str, Path]) -> ArtifactBundle:
    return ArtifactBundle(
        primary=(
            ArtifactRef(key="inventory", path=paths["inventory"], kind="file", role="primary"),
            ArtifactRef(key="raw_store", path=paths["raw_store"], kind="file", role="primary"),
            ArtifactRef(key="metadata", path=paths["metadata"], kind="file", role="primary"),
            ArtifactRef(key="otherdata", path=paths["otherdata"], kind="file", role="primary"),
            ArtifactRef(key="brickdata", path=paths["brickdata"], kind="file", role="primary"),
            ArtifactRef(key="ledger", path=paths["ledger"], kind="file", role="primary"),
        )
    )


def _report_step_start(request: StageTaskRequest, step_name: str) -> None:
    if request.reporter is not None:
        request.reporter.step_start(step_name, compact=True)


def _report_step_done(request: StageTaskRequest, step_name: str, **fields) -> None:
    if request.reporter is not None:
        request.reporter.step_done(step_name, compact=True, **fields)


def build_stage_data(request: StageTaskRequest) -> StageTaskResult:
    dataset_name = str(request.config.get("dataset") or request.dataset_id).strip()
    input_dir = _resolve_input_dir(request)
    paths = _build_output_paths(request.data_dir)
    stage_cfg = dict(request.config)
    stage_cfg.setdefault("dataset", dataset_name)
    dataset = load_dataset(dataset_name, stage_cfg, reporter=request.reporter)

    t0 = time.perf_counter()
    _report_step_start(request, "inventory")
    inventory_df = dataset.build_inventory(input_dir=input_dir, cfg=stage_cfg, out_path=paths["inventory"])
    inventory_elapsed = time.perf_counter() - t0
    _report_step_done(request, "inventory", rows=len(inventory_df), path=paths["inventory"], elapsed=f"{inventory_elapsed:.2f}s")

    t0 = time.perf_counter()
    _report_step_start(request, "ingest")
    ingested_series = dataset.ingest_data(
        input_dir=input_dir,
        cfg=stage_cfg,
        inventory_df=inventory_df.reset_index(drop=True),
        h5_path=paths["raw_store"],
        max_workers=int(stage_cfg["io"]["max_workers"]),
        chunk_len=int(stage_cfg["io"]["chunk_len"]),
    )
    ingest_elapsed = time.perf_counter() - t0
    _report_step_done(request, "ingest", series=ingested_series, path=paths["raw_store"], elapsed=f"{ingest_elapsed:.2f}s")

    t0 = time.perf_counter()
    _report_step_start(request, "metadata")
    metadata_df = dataset.build_metadata(input_dir=input_dir, cfg=stage_cfg, inventory_df=inventory_df, out_path=paths["metadata"])
    metadata_elapsed = time.perf_counter() - t0
    _report_step_done(request, "metadata", rows=len(metadata_df), path=paths["metadata"], elapsed=f"{metadata_elapsed:.2f}s")

    t0 = time.perf_counter()
    _report_step_start(request, "brickdata")
    brickdata_df = dataset.build_brickdata(input_dir=input_dir, cfg=stage_cfg, inventory_df=inventory_df, out_path=paths["brickdata"])
    brickdata_elapsed = time.perf_counter() - t0
    _report_step_done(request, "brickdata", rows=len(brickdata_df), path=paths["brickdata"], elapsed=f"{brickdata_elapsed:.2f}s")

    t0 = time.perf_counter()
    _report_step_start(request, "ledger")
    ledger_df = dataset.build_ledger(
        inventory_df=inventory_df,
        meta_df=metadata_df,
        bricks_df=brickdata_df,
        inventory_store_path=paths["inventory"],
        ts_store_path=paths["raw_store"],
        meta_store_path=paths["metadata"],
        bricks_store_path=paths["brickdata"],
        out_path=paths["ledger"],
        validate=True,
    )
    ledger_elapsed = time.perf_counter() - t0
    _report_step_done(request, "ledger", rows=len(ledger_df), path=paths["ledger"], elapsed=f"{ledger_elapsed:.2f}s")

    metrics = {
        "inventory_rows": len(inventory_df),
        "ingested_series": int(ingested_series),
        "metadata_rows": len(metadata_df),
        "brickdata_rows": len(brickdata_df),
        "ledger_rows": len(ledger_df),
        "inventory_elapsed_seconds": round(inventory_elapsed, 6),
        "ingest_elapsed_seconds": round(ingest_elapsed, 6),
        "metadata_elapsed_seconds": round(metadata_elapsed, 6),
        "brickdata_elapsed_seconds": round(brickdata_elapsed, 6),
        "ledger_elapsed_seconds": round(ledger_elapsed, 6),
    }

    return StageTaskResult(
        status="success",
        artifact_bundle=_build_artifact_bundle(paths),
        metrics=metrics,
        extras={
            "dataset_name": dataset_name,
            "dataset_class": dataset.__class__.__name__,
            "input_dir": str(input_dir),
        },
    )


class BuildTask:
    name = "build"

    def run(self, request: StageTaskRequest) -> StageTaskResult:
        return build_stage_data(request)


TASKS = {"build": BuildTask()}
