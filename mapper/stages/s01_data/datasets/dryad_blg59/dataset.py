from __future__ import annotations

import json
from pathlib import Path
from typing import TYPE_CHECKING, Any

from ..dataset_base import DatasetBase

from ...helpers.brickdata_builder import build_brickdata
from ...helpers.inventory_builder import build_inventory
from ...helpers.ledger_builder import build_ledger
from ...helpers.merge_fields import merge_fields
from ...helpers.metadata_builder import build_metadata

from .helpers.brickdata_fields import get_brick_fields
from .helpers.brickdata_reader import read_brick_ttl
from .helpers.csv_reader import ingest_csv_files
from .helpers.id_assigner_adapter import assign_standard_ids
from .helpers.inventory_reader import read_inventory
from .helpers.metadata_fields import get_metadata_fields
from .helpers.metadata_reader import read_metadata_inferred

if TYPE_CHECKING:
    import pandas as pd


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
        for idx, spec in enumerate(list(ingest_cfg.get("files") or [])):
            self._require_keys(spec, ["path", "time_column", "timezone", "source_group"], f"ingest_config.files[{idx}]")
        self._require_keys(id_rules_cfg, ["key_normalization", "zone_id", "group_id", "equip_id", "class", "location", "loop_ids", "strictness", "hierarchy"], "id_rules")
        self._require_keys(metadata_cfg, ["authoritative_semantics", "allowed_fields", "source_unit_rules", "role_rules", "drop_key_patterns"], "metadata_config")
        self._require_keys(brick_cfg, ["ttl_path", "predicates", "role_overrides", "role_rules", "unit_candidates_tokens", "default_role", "point_alias_rules"], "brickdata_config")
        if bool(metadata_cfg.get("authoritative_semantics", True)):
            raise ValueError("dryad_blg59 requires metadata_config.authoritative_semantics=false")
        self._require_keys(id_rules_cfg["zone_id"], ["sources", "fallback"], "id_rules.zone_id")
        self._require_keys(id_rules_cfg["group_id"], ["derive_from", "policy"], "id_rules.group_id")
        self._require_keys(id_rules_cfg["location"], ["default_from", "sources"], "id_rules.location")
        self._require_keys(id_rules_cfg.get("loop_ids", {}), ["delimiter", "rules", "fallback"], "id_rules.loop_ids")
        self._require_keys(id_rules_cfg.get("hierarchy", {}), ["default_mode", "control_chain", "context_chain"], "id_rules.hierarchy")
        if str(id_rules_cfg["group_id"].get("derive_from") or "") != "zone_id":
            raise ValueError("dryad_blg59 id_rules.group_id.derive_from must be 'zone_id'")

        self._cfg_cache = {
            "ingest": ingest_cfg,
            "id_rules": id_rules_cfg,
            "metadata": metadata_cfg,
            "brick": brick_cfg,
        }
        return self._cfg_cache

    def _parse_loop_ids(self, value: Any, delimiter: str) -> list[str]:
        try:
            import pandas as pd
        except ModuleNotFoundError:
            pd = None

        if value is None:
            return []
        if pd is not None:
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

    def _build_hierarchy(self, other_df: "pd.DataFrame", id_rules: dict[str, Any]):
        import pandas as pd
        import re

        hierarchy_cfg = id_rules.get("hierarchy") or {}
        joiner = str(hierarchy_cfg.get("joiner") or " > ")
        loop_delim = str((id_rules.get("loop_ids") or {}).get("delimiter") or "|")
        list_delim = str(hierarchy_cfg.get("list_delimiter") or "|")
        control_cfg = dict(hierarchy_cfg.get("control_chain") or {})
        context_cfg = dict(hierarchy_cfg.get("context_chain") or {})
        mode_rules = list(hierarchy_cfg.get("mode_rules") or [])
        default_mode = str(hierarchy_cfg.get("default_mode") or "context_chain")

        ctrl_loop_prefix = str(control_cfg.get("loop_prefix") or "loop:")
        ctrl_setpoint_prefix = str(control_cfg.get("setpoint_prefix") or "setpoint:")
        ctrl_setpoint_anchor_prefix = str(control_cfg.get("setpoint_anchor_prefix") or "setpoint_anchor:")
        ctrl_actuator_prefix = str(control_cfg.get("actuator_prefix") or "actuator:")
        ctrl_actuator_anchor_prefix = str(control_cfg.get("actuator_anchor_prefix") or "actuator_anchor:")
        ctrl_sensor_anchor_prefix = str(control_cfg.get("sensor_anchor_prefix") or "sensor_anchor:")
        ctrl_sensor_prefix = str(control_cfg.get("sensor_prefix") or "sensor:")
        unknown_loop = str(control_cfg.get("unknown_loop") or "UNMAPPED_LOOP")
        unknown_setpoint = str(control_cfg.get("unknown_setpoint") or "UNMAPPED_SETPOINT")
        unknown_actuator = str(control_cfg.get("unknown_actuator") or "UNMAPPED_ACTUATOR")
        unknown_sensor = str(control_cfg.get("unknown_sensor") or "UNMAPPED_SENSOR")

        ctx_loop_prefix = str(context_cfg.get("loop_prefix") or "loop:")
        ctx_role_prefix = str(context_cfg.get("role_prefix") or "role:")
        ctx_point_prefix = str(context_cfg.get("point_prefix") or "point:")
        ctx_include_loop = bool(context_cfg.get("include_loop", True))
        ctx_include_role = bool(context_cfg.get("include_role", True))

        compiled_mode_rules: list[tuple[re.Pattern[str], str]] = []
        for rule in mode_rules:
            pattern = str(rule.get("zone_pattern") or "").strip()
            mode = str(rule.get("mode") or "").strip()
            if pattern and mode:
                compiled_mode_rules.append((re.compile(pattern, flags=re.IGNORECASE), mode))

        key_s = other_df["key"].astype("string").fillna("UNK")
        role_s = other_df["role"].astype("string").str.lower().fillna("sensor")
        zone_s = other_df["zone_id"].astype("string").fillna("UNK")
        loops_s = other_df.get("loop_ids", pd.Series([pd.NA] * len(other_df), index=other_df.index))
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

    def _print_stage1_summary(self, other_df: "pd.DataFrame", *, dropped_cerc: int) -> None:
        view = other_df.loc[:, ["key", "role", "zone_id", "group_id", "loop_ids", "hierarchy"]].copy()
        view["role"] = view["role"].astype("string").fillna("unknown")
        view["zone_id"] = view["zone_id"].astype("string").fillna("UNK")
        view["group_id"] = view["group_id"].astype("string").fillna("UNK")
        print(f"[dryad_blg59] retained keys: {len(view)}")
        print(f"[dryad_blg59] dropped cerc keys: {dropped_cerc}")
        print("[dryad_blg59] role counts by group_id")
        counts = view.groupby(["group_id", "role"], dropna=False).size().reset_index(name="count")
        for row in counts.to_dict(orient="records"):
            print(f"  group={row['group_id']:<8} role={row['role']:<9} count={int(row['count'])}")

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
        inventory_df,
        h5_path: Path,
        max_workers: int = 8,
        chunk_len: int = 8192,
    ) -> int:
        del max_workers, chunk_len
        ds_cfg = self._load_dataset_configs(cfg)
        if "key" not in inventory_df.columns:
            raise ValueError("dryad_blg59 ingest_data requires inventory_df['key']")
        return ingest_csv_files(
            input_dir=input_dir,
            ingest_cfg=ds_cfg["ingest"],
            inventory_df=inventory_df,
            h5_path=h5_path,
            chunk_len=chunk_len,
        )

    def build_metadata(self, input_dir: Path, cfg: dict[str, Any], inventory_df, out_path: Path):
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

    def build_brickdata(self, input_dir: Path, cfg: dict[str, Any], inventory_df, out_path: Path):
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

    def build_ledger(
        self,
        inventory_df,
        meta_df,
        bricks_df,
        inventory_store_path: Path,
        ts_store_path: Path,
        meta_store_path: Path,
        bricks_store_path: Path,
        out_path: Path,
        validate: bool = True,
    ):
        import pandas as pd

        ds_cfg = self._load_dataset_configs(self.cfg)
        id_rules = ds_cfg["id_rules"]
        ingest_cfg = ds_cfg["ingest"]

        other_df = merge_fields(self.cfg, inventory_df, [("meta", meta_df), ("brick", bricks_df)])

        evidence_cols = [
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
        use_cols = [column for column in evidence_cols if column in bricks_df.columns]
        if "source_group" in meta_df.columns:
            meta_evidence = meta_df.loc[:, [column for column in ["key", "source_group"] if column in meta_df.columns]].drop_duplicates(subset=["key"])
            evidence_df = (bricks_df.loc[:, use_cols].copy() if use_cols else bricks_df.loc[:, ["key"]].copy())
            evidence_df = evidence_df.merge(meta_evidence, on="key", how="outer")
        else:
            evidence_df = bricks_df.loc[:, use_cols].copy() if use_cols else bricks_df.loc[:, ["key"]].copy()

        id_df = assign_standard_ids(key_df=inventory_df.loc[:, ["key"]], evidence_df=evidence_df, id_rules=id_rules)
        other_df = other_df.merge(id_df, on="key", how="left")

        if "brick_class" in bricks_df.columns:
            brick_cls = bricks_df.loc[:, ["key", "brick_class"]].drop_duplicates(subset=["key"])
            other_df = other_df.merge(brick_cls, on="key", how="left", suffixes=("", "_brick"))
            if "brick_class_brick" in other_df.columns:
                other_df["brick_class"] = (
                    other_df.get("brick_class", pd.Series([pd.NA] * len(other_df), dtype="string"))
                    .astype("string")
                    .replace("", pd.NA)
                    .fillna(other_df["brick_class_brick"].astype("string").replace("", pd.NA))
                )
                other_df = other_df.drop(columns=["brick_class_brick"], errors="ignore")

        for column in ["label", "role", "location", "unit", "unit_candidates", "hierarchy", "source_file", "source_group"]:
            if column in other_df.columns:
                other_df[column] = other_df[column].astype("string").str.strip().replace("", pd.NA)

        other_df["label"] = other_df["label"].fillna(other_df["key"].astype("string"))
        other_df["role"] = other_df["role"].fillna("sensor")
        if "location" not in other_df.columns:
            other_df["location"] = pd.Series([pd.NA] * len(other_df), dtype="string")
        other_df["location"] = other_df["location"].fillna(other_df["location_default"])
        other_df = other_df.drop(columns=["location_default"], errors="ignore")
        other_df["group_id"] = other_df["group_id"].astype("string").replace("", pd.NA).fillna(other_df["zone_id"].astype("string"))
        other_df["hierarchy"] = self._build_hierarchy(other_df=other_df, id_rules=id_rules)
        other_df = other_df.drop(columns=["brick_uri"], errors="ignore")

        if "zone_id" not in other_df.columns or "group_id" not in other_df.columns:
            raise ValueError("dryad_blg59 otherdata missing required IDs zone_id/group_id")
        if other_df["zone_id"].isna().any() or other_df["group_id"].isna().any():
            raise ValueError("dryad_blg59 required IDs zone_id/group_id contain nulls")

        required = ["key", "label", "role", "location", "unit", "unit_candidates", "hierarchy", "zone_id", "group_id"]
        optional = ["equip_id", "class", "loop_ids", "brick_class", "source_file", "source_group", "source_unit_rule"]
        for column in required:
            if column not in other_df.columns:
                other_df[column] = pd.NA
        for column in optional:
            if column not in other_df.columns:
                other_df[column] = pd.NA
        ordered = required + optional
        passthrough = [column for column in other_df.columns if column not in ordered]
        other_df = other_df.loc[:, ordered + passthrough]

        other_store_path = out_path.parent / "otherdata.feather"
        other_df.to_feather(other_store_path)

        dropped_cerc = 0
        input_root = Path(self.cfg.get("input_path") or self.cfg.get("input_root") or "")
        for spec in list(ingest_cfg.get("files") or []):
            csv_path = input_root / str(spec.get("path") or "")
            if csv_path.exists():
                cols = list(pd.read_csv(csv_path, nrows=0).columns)
                dropped_cerc += len(
                    [
                        column
                        for column in cols
                        if isinstance(column, str) and (column.startswith("cerc_templogger_") or column.startswith("zone_cerc_"))
                    ]
                )
        self._print_stage1_summary(other_df, dropped_cerc=dropped_cerc)

        return build_ledger(
            inventory_df=inventory_df,
            inventory_store_path=inventory_store_path,
            ts_store_path=ts_store_path,
            otherdata_list=[
                ("metadata", meta_df, meta_store_path),
                ("brickdata", bricks_df, bricks_store_path),
                ("other", other_df, other_store_path),
            ],
            out_path=out_path,
            validate=validate,
        )


DryadBlg59Dataset = Dataset
