from __future__ import annotations

import time
from pathlib import Path
from typing import Any

import pandas as pd

from mapper.stages.common import ArtifactBundle, ArtifactRef, StageTaskRequest, StageTaskResult

from .config_loader import build_shared_context, load_config, load_meta_contract_cfg, resolve_process_configs
from .helpers.config_validation import validate_preprocessing_startup
from .helpers.io import build_meta_lookup, extract_period, load_ledger, open_ts_store, read_series_for_key, read_store_table, resolve_store_paths
from .helpers.meta_contract import discover_store_columns, load_meta_contract, resolve_fields
from .helpers.xnode_writer import PreprocWriter, build_node_ledger
from .process_registry import build_process_pipeline, build_trainers


def _build_output_paths(data_dir: Path) -> dict[str, Path]:
    return {
        "xnode_ts": data_dir / "xnode_ts.h5",
        "xnode_meta": data_dir / "xnode_meta.feather",
        "xnode_ledger": data_dir / "xnode_ledger.feather",
    }


def _build_artifact_bundle(paths: dict[str, Path]) -> ArtifactBundle:
    return ArtifactBundle(
        primary=(
            ArtifactRef(key="xnode_ts", path=paths["xnode_ts"], kind="file", role="primary"),
            ArtifactRef(key="xnode_meta", path=paths["xnode_meta"], kind="file", role="primary"),
            ArtifactRef(key="xnode_ledger", path=paths["xnode_ledger"], kind="file", role="primary"),
        )
    )


def _report_step_start(request: StageTaskRequest, step_name: str) -> None:
    if request.reporter is not None:
        request.reporter.step_start(step_name, compact=True)


def _report_step_done(request: StageTaskRequest, step_name: str, **fields: Any) -> None:
    if request.reporter is not None:
        request.reporter.step_done(step_name, compact=True, **fields)


def load_upstream_inputs(request: StageTaskRequest, cfg: dict[str, Any]) -> dict[str, Any]:
    if "ledger" not in request.input_refs:
        raise KeyError("Stage 2 requires upstream input artifact 'ledger'")
    ledger_ref = request.input_refs["ledger"]
    ledger_df = load_ledger(ledger_ref.path)
    inv_path, ts_path, meta_stores, meta_order = resolve_store_paths(ledger_df)
    store_frames = {name: read_store_table(path) for name, path in meta_stores.items()}

    meta_contract = load_meta_contract(load_meta_contract_cfg(cfg))
    discovered = discover_store_columns(store_frames)
    resolved = resolve_fields(meta_contract, discovered)
    if meta_contract.get("strict", True) and resolved["missing_required"]:
        raise ValueError(f"Missing required upstream meta fields: {resolved['missing_required']}")

    meta_lookup = build_meta_lookup(
        store_frames=store_frames,
        lookup_order=meta_order,
        selected_fields=resolved["selected_fields"],
        contract=meta_contract,
    )

    return {
        "ledger_ref": ledger_ref,
        "ledger_df": ledger_df,
        "inv_path": inv_path,
        "ts_path": ts_path,
        "meta_stores": meta_stores,
        "meta_order": meta_order,
        "store_frames": store_frames,
        "meta_contract": meta_contract,
        "meta_fields": resolved["selected_fields"],
        "missing_required": resolved["missing_required"],
        "meta_lookup": meta_lookup,
    }


def run_trainers(
    request: StageTaskRequest,
    cfg: dict[str, Any],
    upstream: dict[str, Any],
    keys: list[str],
    start_ts: pd.Timestamp,
    end_ts: pd.Timestamp,
) -> tuple[dict[str, dict[str, Any]], dict[str, Any]]:
    trainers = build_trainers(cfg, request.dataset_id, reporter=request.reporter, progress=None)
    enabled = [(name, trainer) for name, trainer in trainers if trainer is not None]
    if not enabled:
        return {}, {"enabled_trainers": 0, "trained_trainers": 0}

    model_map: dict[str, dict[str, Any]] = {}
    t0 = time.perf_counter()
    _report_step_start(request, "trainers")

    if request.progress is not None:
        request.progress.start("trainers", total=len(enabled), unit="trainer")

    with open_ts_store(upstream["ts_path"]) as ts_hdf5:
        def _read_series(key: str) -> pd.Series | None:
            return read_series_for_key(ts_hdf5, key, start_ts, end_ts)

        for index, (name, trainer) in enumerate(enabled, 1):
            assert trainer is not None
            state = trainer.fit(
                _read_fn=_read_series,
                keys=keys,
                meta_lookup=upstream["meta_lookup"],
                prev_state=None,
                prev_hparams=None,
            ) or {}
            model_map[name] = state
            if request.progress is not None:
                request.progress.update(index, total=len(enabled), extra={"trainer": name}, force=index == len(enabled))

    if request.progress is not None:
        request.progress.finish(extra={"trained": len(model_map)})

    elapsed = time.perf_counter() - t0
    _report_step_done(request, "trainers", trained=len(model_map), elapsed=f"{elapsed:.2f}s")
    return model_map, {
        "enabled_trainers": len(enabled),
        "trained_trainers": len(model_map),
        "trainer_elapsed_seconds": round(elapsed, 6),
    }


def run_process_pipeline(
    request: StageTaskRequest,
    cfg: dict[str, Any],
    upstream: dict[str, Any],
    model_map: dict[str, dict[str, Any]],
    paths: dict[str, Path],
    start_ts: pd.Timestamp,
    end_ts: pd.Timestamp,
) -> dict[str, Any]:
    process_list = build_process_pipeline(cfg, request.dataset_id, reporter=request.reporter, progress=None)
    for name, proc in process_list:
        proc.set_state(model_map.get(name))
    shared_context = build_shared_context(cfg, process_list)
    keys = upstream["ledger_df"]["key"].astype("string").dropna().drop_duplicates().tolist()
    total = len(keys)
    proc_step = max(1, total // 20) if total > 0 else 1
    written_keys: list[str] = []
    skipped_no_data = 0
    skipped_after_process = 0

    t0 = time.perf_counter()
    _report_step_start(request, "preprocess")
    if request.progress is not None:
        request.progress.start("preprocess", total=total, unit="key")

    with open_ts_store(upstream["ts_path"]) as ts_hdf5, PreprocWriter(request.data_dir) as writer:
        for index, key in enumerate(keys, 1):
            series = read_series_for_key(ts_hdf5, str(key), start_ts, end_ts)
            if series is None or series.empty:
                skipped_no_data += 1
                if request.progress is not None:
                    request.progress.update(index, total=total, extra={"written": len(written_keys), "skipped": skipped_no_data + skipped_after_process})
                continue

            payload: dict[str, Any] = {
                "key": str(key),
                "series": series,
                "flag": None,
                "static": {},
                "meta": upstream["meta_lookup"].get(str(key), {}),
                "meta_contract_fields": upstream["meta_fields"],
                "runtime_context": shared_context,
                "unit_contract": shared_context.get("unit_contract"),
            }

            current: dict[str, Any] | None = payload
            for name, proc in process_list:
                if current is None:
                    break
                current = proc.apply(current)
                proc.prog_tick(name, index, total, proc_step)

            if current is None:
                skipped_after_process += 1
                if request.progress is not None:
                    request.progress.update(index, total=total, extra={"written": len(written_keys), "skipped": skipped_no_data + skipped_after_process})
                continue

            out_series = current.get("series")
            if not isinstance(out_series, pd.Series) or out_series.empty:
                skipped_after_process += 1
                if request.progress is not None:
                    request.progress.update(index, total=total, extra={"written": len(written_keys), "skipped": skipped_no_data + skipped_after_process})
                continue

            flag_series = current.get("flag") if isinstance(current.get("flag"), pd.Series) else None
            writer.write_node_meta(str(key), dict(current.get("static") or {}))
            writer.write_node_ts(str(key), out_series.astype("float32", copy=False), flag_series.astype("int8") if flag_series is not None else None)
            written_keys.append(str(key))

            if request.progress is not None:
                request.progress.update(index, total=total, extra={"written": len(written_keys), "skipped": skipped_no_data + skipped_after_process})

        ledger_path = build_node_ledger(writer.ts_path, writer.meta_path, written_keys, paths["xnode_ledger"])

    if request.progress is not None:
        request.progress.finish(extra={"written": len(written_keys), "skipped": skipped_no_data + skipped_after_process})

    elapsed = time.perf_counter() - t0
    _report_step_done(request, "preprocess", written=len(written_keys), skipped=skipped_no_data + skipped_after_process, elapsed=f"{elapsed:.2f}s")
    return {
        "xnode_ts_path": paths["xnode_ts"],
        "xnode_meta_path": paths["xnode_meta"],
        "xnode_ledger_path": ledger_path,
        "written_keys": written_keys,
        "preprocess_elapsed_seconds": round(elapsed, 6),
        "skipped_no_data": skipped_no_data,
        "skipped_after_process": skipped_after_process,
    }


def build_preprocessing(request: StageTaskRequest) -> StageTaskResult:
    paths = _build_output_paths(request.data_dir)

    t0 = time.perf_counter()
    _report_step_start(request, "config")
    cfg = load_config(dict(request.config))
    resolved_process_cfgs = resolve_process_configs(cfg, request.dataset_id)
    validate_preprocessing_startup(cfg, resolved_process_cfgs)
    config_elapsed = time.perf_counter() - t0
    _report_step_done(request, "config", processes=len(resolved_process_cfgs), elapsed=f"{config_elapsed:.2f}s")

    upstream = load_upstream_inputs(request, cfg)
    start_ts, end_ts = extract_period(cfg)
    keys = upstream["ledger_df"]["key"].astype("string").dropna().drop_duplicates().tolist()

    model_map, trainer_metrics = run_trainers(request, cfg, upstream, keys, start_ts, end_ts)
    output_info = run_process_pipeline(request, cfg, upstream, model_map, paths, start_ts, end_ts)

    t0 = time.perf_counter()
    _report_step_start(request, "outputs")
    artifact_bundle = _build_artifact_bundle(paths)
    output_elapsed = time.perf_counter() - t0
    _report_step_done(request, "outputs", artifacts=3, elapsed=f"{output_elapsed:.2f}s")

    metrics = {
        "config_elapsed_seconds": round(config_elapsed, 6),
        "upstream_keys": len(keys),
        "meta_fields": len(upstream["meta_fields"]),
        "missing_required_meta_fields": len(upstream["missing_required"]),
        "xnode_rows": len(output_info["written_keys"]),
        "skipped_no_data": output_info["skipped_no_data"],
        "skipped_after_process": output_info["skipped_after_process"],
        **trainer_metrics,
        "preprocess_elapsed_seconds": output_info["preprocess_elapsed_seconds"],
        "outputs_elapsed_seconds": round(output_elapsed, 6),
    }

    return StageTaskResult(
        status="success",
        artifact_bundle=artifact_bundle,
        metrics=metrics,
        extras={
            "meta_order": list(upstream["meta_order"]),
            "meta_fields": list(upstream["meta_fields"]),
            "input_ledger": str(upstream["ledger_ref"].path),
            "ts_store": str(upstream["ts_path"]),
        },
    )


class BuildTask:
    name = "build"

    def run(self, request: StageTaskRequest) -> StageTaskResult:
        return build_preprocessing(request)


TASKS = {"build": BuildTask()}
