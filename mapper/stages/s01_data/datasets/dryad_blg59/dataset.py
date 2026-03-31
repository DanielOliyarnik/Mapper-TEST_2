from __future__ import annotations

import re
import json
import pandas as pd
from pathlib import Path
from typing import Any

from ..dataset_base import DatasetBase

from ...helpers.brickdata_builder import build_brickdata
from ...helpers.ingest_engine import ingest_records
from ...helpers.inventory_builder import build_inventory
from ...helpers.ledger_builder import build_ledger
from ...helpers.metadata_builder import build_metadata
from ...helpers.otherdata_builder import build_otherdata as build_otherdata_table

from .helpers.brickdata_fields import get_brick_fields
from .helpers.brickdata_reader import read_brick_ttl
from .helpers.csv_reader import build_csv_ingest_records, read_csv_record
from .helpers.id_assigner_adapter import assign_standard_ids
from .helpers.inventory_reader import read_inventory
from .helpers.metadata_fields import get_metadata_fields
from .helpers.metadata_reader import read_metadata_inferred


class Dataset(DatasetBase):
    dataset_name = "dryad_blg59"

    def __init__(self, cfg: dict[str, Any]) -> None:
        super().__init__(cfg)
        self._cfg_cache: dict[str, Any] | None = None

    def _resolve_dataset_config_path(self, cfg: dict[str, Any], cfg_key: str, default_name: str) -> Path:
        configured = str(cfg.get(cfg_key) or "").strip()
        if configured:
            path = Path(configured)
            return path if path.is_absolute() else (Path.cwd() / path)
        return Path(__file__).resolve().parent / "configs" / default_name

    def _require_keys(self, obj: dict[str, Any], required: list[str], name: str) -> None:
        for key in required:
            if key not in obj:
                raise ValueError(f"dryad_blg59 invalid {name}: missing key {key!r}")

    def _load_dataset_configs(self, cfg: dict[str, Any]) -> dict[str, Any]:
        if self._cfg_cache is not None:
            return self._cfg_cache

        ingest_path = self._resolve_dataset_config_path(cfg, "ingest_config", "ingest_config.json")
        id_rules_path = self._resolve_dataset_config_path(cfg, "id_rules_config", "id_rules.json")
        metadata_path = self._resolve_dataset_config_path(cfg, "metadata_config", "metadata_config.json")
        brick_path = self._resolve_dataset_config_path(cfg, "brickdata_config", "brickdata_config.json")

        for path in (ingest_path, id_rules_path, metadata_path, brick_path):
            if not path.exists():
                raise FileNotFoundError(f"dryad_blg59 missing required config: {path}")

        ingest_cfg = json.loads(ingest_path.read_text(encoding="utf-8"))
        id_rules_cfg = json.loads(id_rules_path.read_text(encoding="utf-8"))
        metadata_cfg = json.loads(metadata_path.read_text(encoding="utf-8"))
        brick_cfg = json.loads(brick_path.read_text(encoding="utf-8"))

        self._require_keys(ingest_cfg, ["files", "building", "value_dtype", "drop_cerc"], "ingest_config")
        for idx, spec in enumerate(ingest_cfg["files"]):
            self._require_keys(spec, ["path", "time_column", "datetime_format", "timezone", "source_group"], f"ingest_config.files[{idx}]")
        self._require_keys(id_rules_cfg, ["key_normalization", "zone_id", "group_id", "equip_id", "class", "location", "loop_ids", "strictness", "hierarchy"], "id_rules")
        self._require_keys(metadata_cfg, ["authoritative_semantics", "allowed_fields", "source_unit_rules", "role_rules", "drop_key_patterns"], "metadata_config")
        self._require_keys(brick_cfg, ["ttl_path", "predicates", "role_overrides", "role_rules", "unit_candidates_tokens", "default_role", "point_alias_rules"], "brickdata_config")
        if bool(metadata_cfg["authoritative_semantics"]):
            raise ValueError("dryad_blg59 requires metadata_config.authoritative_semantics=false")

        self._require_keys(id_rules_cfg["zone_id"], ["sources", "fallback"], "id_rules.zone_id")
        self._require_keys(id_rules_cfg["group_id"], ["derive_from", "policy"], "id_rules.group_id")
        self._require_keys(id_rules_cfg["location"], ["default_from", "sources"], "id_rules.location")
        self._require_keys(id_rules_cfg["loop_ids"], ["delimiter", "rules", "fallback"], "id_rules.loop_ids")
        self._require_keys(id_rules_cfg["hierarchy"], ["default_mode", "control_chain", "context_chain", "joiner", "list_delimiter", "mode_rules"], "id_rules.hierarchy")
        if str(id_rules_cfg["group_id"]["derive_from"]) != "zone_id":
            raise ValueError("dryad_blg59 id_rules.group_id.derive_from must be 'zone_id'")

        self._cfg_cache = {
            "ingest": ingest_cfg,
            "id_rules": id_rules_cfg,
            "metadata": metadata_cfg,
            "brick": brick_cfg,
        }
        return self._cfg_cache

    def _parse_loop_ids(self, value: Any, delimiter: str) -> list[str]:
        if value is None:
            return []
        try:
            if pd.isna(value):
                return []
        except Exception:
            pass
        if isinstance(value, list):
            raw = [str(item).strip() for item in value]
        else:
            text = str(value).strip()
            if not text:
                return []
            raw = [token.strip() for token in text.split(delimiter)]
        out: list[str] = []
        seen: set[str] = set()
        for token in raw:
            if token and token not in seen:
                seen.add(token)
                out.append(token)
        return out

    def _join_unique(self, values: list[str], delimiter: str) -> str:
        out: list[str] = []
        seen: set[str] = set()
        for value in values:
            token = str(value).strip()
            if token and token not in seen:
                seen.add(token)
                out.append(token)
        return delimiter.join(out)

    def _build_hierarchy(self, other_df: pd.DataFrame, id_rules: dict[str, Any]) -> pd.Series:
        hierarchy_cfg = id_rules["hierarchy"]
        joiner = str(hierarchy_cfg["joiner"])
        loop_delim = str(id_rules["loop_ids"]["delimiter"])
        list_delim = str(hierarchy_cfg["list_delimiter"])
        control_cfg = dict(hierarchy_cfg["control_chain"])
        context_cfg = dict(hierarchy_cfg["context_chain"])
        mode_rules = list(hierarchy_cfg["mode_rules"])
        default_mode = str(hierarchy_cfg["default_mode"])

        ctrl_loop_prefix = str(control_cfg["loop_prefix"])
        ctrl_setpoint_prefix = str(control_cfg["setpoint_prefix"])
        ctrl_setpoint_anchor_prefix = str(control_cfg["setpoint_anchor_prefix"])
        ctrl_actuator_prefix = str(control_cfg["actuator_prefix"])
        ctrl_actuator_anchor_prefix = str(control_cfg["actuator_anchor_prefix"])
        ctrl_sensor_anchor_prefix = str(control_cfg["sensor_anchor_prefix"])
        ctrl_sensor_prefix = str(control_cfg["sensor_prefix"])
        unknown_loop = str(control_cfg["unknown_loop"])
        unknown_setpoint = str(control_cfg["unknown_setpoint"])
        unknown_actuator = str(control_cfg["unknown_actuator"])
        unknown_sensor = str(control_cfg["unknown_sensor"])

        ctx_loop_prefix = str(context_cfg["loop_prefix"])
        ctx_role_prefix = str(context_cfg["role_prefix"])
        ctx_point_prefix = str(context_cfg["point_prefix"])
        ctx_include_loop = bool(context_cfg["include_loop"])
        ctx_include_role = bool(context_cfg["include_role"])

        compiled_mode_rules: list[tuple[re.Pattern[str], str]] = []
        for rule in mode_rules:
            pattern = str(rule["zone_pattern"]).strip()
            mode = str(rule["mode"]).strip()
            if pattern and mode:
                compiled_mode_rules.append((re.compile(pattern, flags=re.IGNORECASE), mode))

        key_s = other_df["key"].astype("string").fillna("UNK")
        role_s = other_df["role"].astype("string").str.lower().fillna("sensor")
        zone_s = other_df["zone_id"].astype("string").fillna("UNK")
        loops_s = other_df["loop_ids"] if "loop_ids" in other_df.columns else pd.Series([pd.NA] * len(other_df), index=other_df.index)
        parsed_loops = [self._parse_loop_ids(value, loop_delim) for value in loops_s.tolist()]

        setpoints_by_zone_loop: dict[tuple[str, str], list[str]] = {}
        actuators_by_zone_loop: dict[tuple[str, str], list[str]] = {}
        sensors_by_zone_loop: dict[tuple[str, str], list[str]] = {}
        for idx in range(len(other_df)):
            key = str(key_s.iat[idx])
            role = str(role_s.iat[idx])
            zone = str(zone_s.iat[idx])
            loops = parsed_loops[idx] or [unknown_loop]
            for loop in loops:
                bucket = (zone, loop)
                if role == "setpoint":
                    setpoints_by_zone_loop.setdefault(bucket, [])
                    if key not in setpoints_by_zone_loop[bucket]:
                        setpoints_by_zone_loop[bucket].append(key)
                elif role == "actuator":
                    actuators_by_zone_loop.setdefault(bucket, [])
                    if key not in actuators_by_zone_loop[bucket]:
                        actuators_by_zone_loop[bucket].append(key)
                else:
                    sensors_by_zone_loop.setdefault(bucket, [])
                    if key not in sensors_by_zone_loop[bucket]:
                        sensors_by_zone_loop[bucket].append(key)

        hierarchy_values: list[str] = []
        for idx in range(len(other_df)):
            key = str(key_s.iat[idx])
            role = str(role_s.iat[idx])
            zone = str(zone_s.iat[idx])
            raw_loops = parsed_loops[idx]
            mode = default_mode
            for rx, candidate in compiled_mode_rules:
                if rx.search(zone):
                    mode = candidate
                    break
            if mode == "control_chain":
                loops = raw_loops or [unknown_loop]
                loop_token = self._join_unique(loops, list_delim)
                setpoint_refs: list[str] = []
                actuator_refs: list[str] = []
                sensor_refs: list[str] = []
                for loop in loops:
                    bucket = (zone, loop)
                    setpoint_refs.extend(setpoints_by_zone_loop.get(bucket, []))
                    actuator_refs.extend(actuators_by_zone_loop.get(bucket, []))
                    sensor_refs.extend(sensors_by_zone_loop.get(bucket, []))
                setpoint_token = self._join_unique(setpoint_refs, list_delim) or unknown_setpoint
                actuator_token = self._join_unique(actuator_refs, list_delim) or unknown_actuator
                sensor_token = self._join_unique(sensor_refs, list_delim) or unknown_sensor
                if role == "setpoint":
                    hierarchy_values.append(
                        f"{zone}{joiner}{ctrl_loop_prefix}{loop_token}{joiner}{ctrl_setpoint_prefix}{key}"
                        f"{joiner}{ctrl_actuator_anchor_prefix}{actuator_token}{joiner}{ctrl_sensor_anchor_prefix}{sensor_token}"
                    )
                elif role == "actuator":
                    hierarchy_values.append(
                        f"{zone}{joiner}{ctrl_loop_prefix}{loop_token}{joiner}{ctrl_setpoint_anchor_prefix}{setpoint_token}"
                        f"{joiner}{ctrl_actuator_prefix}{key}{joiner}{ctrl_sensor_anchor_prefix}{sensor_token}"
                    )
                else:
                    hierarchy_values.append(
                        f"{zone}{joiner}{ctrl_loop_prefix}{loop_token}{joiner}{ctrl_setpoint_anchor_prefix}{setpoint_token}"
                        f"{joiner}{ctrl_actuator_anchor_prefix}{actuator_token}{joiner}{ctrl_sensor_prefix}{key}"
                    )
                continue

            loop_token = self._join_unique(raw_loops, list_delim)
            parts = [zone]
            if ctx_include_loop and loop_token:
                parts.append(f"{ctx_loop_prefix}{loop_token}")
            if ctx_include_role:
                parts.append(f"{ctx_role_prefix}{role}")
            parts.append(f"{ctx_point_prefix}{key}")
            hierarchy_values.append(joiner.join(parts))

        return pd.Series(hierarchy_values, index=other_df.index, dtype="string")

    def _count_dropped_cerc_columns(self, input_dir: Path, ingest_cfg: dict[str, Any]) -> int:
        dropped_cerc = 0
        for spec in ingest_cfg["files"]:
            csv_path = input_dir / str(spec["path"])
            if not csv_path.exists():
                continue
            cols = list(pd.read_csv(csv_path, nrows=0).columns)
            dropped_cerc += len(
                [
                    column
                    for column in cols
                    if isinstance(column, str) and (column.startswith("cerc_templogger_") or column.startswith("zone_cerc_"))
                ]
            )
        return dropped_cerc

    def _build_otherdata_fields(
        self,
        *,
        base_df: pd.DataFrame,
        input_dir: Path,
        cfg: dict[str, Any],
        inventory_df: pd.DataFrame,
        meta_df: pd.DataFrame,
        bricks_df: pd.DataFrame,
        other_cfg: dict[str, Any],
    ) -> pd.DataFrame:
        ds_cfg = self._load_dataset_configs(cfg)
        id_rules = ds_cfg["id_rules"]
        ingest_cfg = ds_cfg["ingest"]
        _ = other_cfg

        source_cols = [
            "key",
            "zone_hint",
            "location_hint",
            "equip_hint",
            "owner_id",
            "owner_class",
            "brick_class",
            "brick_class_local",
            "class_tag_hint",
            "location",
            "source_group",
        ]
        use_cols = [column for column in source_cols if column in bricks_df.columns]
        if "source_group" in meta_df.columns:
            meta_source = meta_df.loc[:, [column for column in ["key", "source_group"] if column in meta_df.columns]].drop_duplicates(subset=["key"])
            join_df = bricks_df.loc[:, use_cols].copy() if use_cols else bricks_df.loc[:, ["key"]].copy()
            join_df = join_df.merge(meta_source, on="key", how="outer")
        else:
            join_df = bricks_df.loc[:, use_cols].copy() if use_cols else bricks_df.loc[:, ["key"]].copy()

        id_df = assign_standard_ids(key_df=inventory_df.loc[:, ["key"]], source_df=join_df, id_rules=id_rules)
        other_df = base_df.merge(id_df, on="key", how="left")

        meta_passthrough_cols = [column for column in ["source_file", "source_group", "source_unit_rule"] if column in meta_df.columns]
        if meta_passthrough_cols:
            other_df = other_df.merge(
                meta_df.loc[:, ["key"] + meta_passthrough_cols].drop_duplicates(subset=["key"]),
                on="key",
                how="left",
                suffixes=("", "__meta"),
            )
            for column in meta_passthrough_cols:
                meta_column = f"{column}__meta"
                if meta_column in other_df.columns:
                    if column in other_df.columns:
                        other_df[column] = other_df[column].astype("string").replace("", pd.NA).fillna(
                            other_df[meta_column].astype("string").replace("", pd.NA)
                        )
                    else:
                        other_df[column] = other_df[meta_column]
                    other_df = other_df.drop(columns=[meta_column])

        if "brick_class" in bricks_df.columns:
            brick_cls = bricks_df.loc[:, ["key", "brick_class"]].drop_duplicates(subset=["key"])
            other_df = other_df.merge(brick_cls, on="key", how="left", suffixes=("", "__brick"))
            if "brick_class__brick" in other_df.columns:
                other_df["brick_class"] = other_df["brick_class"].astype("string").replace("", pd.NA).fillna(
                    other_df["brick_class__brick"].astype("string").replace("", pd.NA)
                )
                other_df = other_df.drop(columns=["brick_class__brick"])

        if "location_default" in other_df.columns:
            other_df["location"] = other_df["location"].astype("string").replace("", pd.NA).fillna(
                other_df["location_default"].astype("string").replace("", pd.NA)
            )
            other_df = other_df.drop(columns=["location_default"])

        if "group_id" not in other_df.columns or "zone_id" not in other_df.columns:
            raise ValueError("dryad_blg59 otherdata missing required IDs zone_id/group_id")
        other_df["group_id"] = other_df["group_id"].astype("string").replace("", pd.NA).fillna(other_df["zone_id"].astype("string"))
        other_df["hierarchy"] = self._build_hierarchy(other_df=other_df, id_rules=id_rules)

        if other_df["zone_id"].isna().any() or other_df["group_id"].isna().any():
            raise ValueError("dryad_blg59 required IDs zone_id/group_id contain nulls")

        dropped_cerc = self._count_dropped_cerc_columns(input_dir=input_dir, ingest_cfg=ingest_cfg)
        counts = (
            other_df.loc[:, ["group_id", "role"]]
            .assign(
                role=lambda df: df["role"].astype("string").fillna("unknown"),
                group_id=lambda df: df["group_id"].astype("string").fillna("UNK"),
            )
            .groupby(["group_id", "role"], dropna=False)
            .size()
            .reset_index(name="count")
            .to_dict(orient="records")
        )
        self.record_metrics(
            dryad_retained_keys=int(len(other_df)),
            dryad_dropped_cerc_columns=int(dropped_cerc),
            dryad_group_role_counts=json.dumps(counts, sort_keys=True),
        )
        return other_df

    def build_inventory(self, input_dir: Path, cfg: dict[str, Any], out_path: Path):
        ds_cfg = self._load_dataset_configs(cfg)
        return build_inventory(
            read_fn=lambda root, stage_cfg: read_inventory(root, stage_cfg, ds_cfg["ingest"]),
            input_dir=input_dir,
            cfg=cfg,
            out_path=out_path,
        )

    def ingest_data(
        self,
        input_dir: Path,
        cfg: dict[str, Any],
        inventory_df: pd.DataFrame,
        h5_path: Path,
        max_workers: int = 8,
        chunk_len: int = 8192,
    ) -> int:
        ds_cfg = self._load_dataset_configs(cfg)
        records = build_csv_ingest_records(input_dir=input_dir, ingest_cfg=ds_cfg["ingest"], inventory_df=inventory_df)
        return ingest_records(
            read_fn=read_csv_record,
            records=records,
            h5_path=h5_path,
            max_workers=max_workers,
            chunk_len=chunk_len,
            reporter=self.reporter,
            progress=self.progress,
            progress_label="ingest",
            progress_unit="files",
        )

    def build_metadata(self, input_dir: Path, cfg: dict[str, Any], inventory_df: pd.DataFrame, out_path: Path):
        ds_cfg = self._load_dataset_configs(cfg)
        return build_metadata(
            read_fn=read_metadata_inferred,
            field_fn=get_metadata_fields,
            input_dir=input_dir,
            cfg=cfg,
            inventory_df=inventory_df,
            out_path=out_path,
            meta_cfg=ds_cfg["metadata"],
        )

    def build_brickdata(self, input_dir: Path, cfg: dict[str, Any], inventory_df: pd.DataFrame, out_path: Path):
        ds_cfg = self._load_dataset_configs(cfg)
        return build_brickdata(
            read_fn=read_brick_ttl,
            field_fn=get_brick_fields,
            input_dir=input_dir,
            cfg=cfg,
            inventory_df=inventory_df,
            out_path=out_path,
            brick_cfg=ds_cfg["brick"],
        )

    def build_otherdata(
        self,
        input_dir: Path,
        cfg: dict[str, Any],
        inventory_df: pd.DataFrame,
        meta_df: pd.DataFrame,
        bricks_df: pd.DataFrame,
        out_path: Path,
    ) -> pd.DataFrame:
        return build_otherdata_table(
            self._build_otherdata_fields,
            input_dir=input_dir,
            cfg=cfg,
            inventory_df=inventory_df,
            meta_df=meta_df,
            bricks_df=bricks_df,
            out_path=out_path,
        )

    def build_ledger(
        self,
        inventory_df: pd.DataFrame,
        meta_df: pd.DataFrame,
        bricks_df: pd.DataFrame,
        other_df: pd.DataFrame,
        inventory_store_path: Path,
        ts_store_path: Path,
        meta_store_path: Path,
        bricks_store_path: Path,
        other_store_path: Path,
        out_path: Path,
        validate: bool = True,
    ) -> pd.DataFrame:
        return build_ledger(
            inventory_df=inventory_df,
            metadata_df=meta_df,
            brickdata_df=bricks_df,
            otherdata_df=other_df,
            inventory_store_path=inventory_store_path,
            ts_store_path=ts_store_path,
            meta_store_path=meta_store_path,
            bricks_store_path=bricks_store_path,
            other_store_path=other_store_path,
            out_path=out_path,
            validate=validate,
        )
