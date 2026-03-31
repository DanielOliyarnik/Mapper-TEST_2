from __future__ import annotations

import re
import pandas as pd
from pathlib import Path
from typing import Any
from rdflib import Graph, RDF, URIRef


EXCLUDE_PATTERNS = [r"^cerc_templogger_", r"^zone_cerc_"]


def _local_name(uri: Any) -> str:
    text = str(uri)
    if "#" in text:
        return text.rsplit("#", 1)[-1]
    return text.rsplit("/", 1)[-1]


def _predicates_by_local_name(graph: Graph, names: list[str]) -> set[URIRef]:
    wanted = {str(name) for name in names}
    out: set[URIRef] = set()
    for predicate in graph.predicates(None, None):
        if _local_name(predicate) in wanted:
            out.add(URIRef(str(predicate)))
    return out


def _first_type_local(graph: Graph, subject_local: str) -> str | None:
    for subject, _, obj in graph.triples((None, RDF.type, None)):
        if _local_name(subject) == subject_local:
            return _local_name(obj)
    return None


def _alias_for(local: str, alias_map: dict[str, str]) -> str:
    if local in alias_map:
        return alias_map[local]
    out = local
    out = out.replace("_oa_damper", "_oadmpr_pct")
    out = out.replace("_oa_fr", "_oa_flow_tn")
    out = out.replace("_fltrd_gnd_plenum_press_tn", "_fltrd_gnd_lvl_plenum_press_tn")
    out = out.replace("occ_forth_south", "occ_fourth_south")
    return out


def _exclude_key(key: str) -> bool:
    return any(re.search(pattern, key, flags=re.IGNORECASE) for pattern in EXCLUDE_PATTERNS)


def _zone_from_owner(owner_id: str | None, parent_of: dict[str, str], feeds_map: dict[str, list[str]], type_by_local: dict[str, str]) -> str | None:
    if not owner_id:
        return None
    node = owner_id
    seen: set[str] = set()
    for _ in range(12):
        if not node or node in seen:
            break
        seen.add(node)
        if re.match(r"^zone_[0-9]+$", node, flags=re.IGNORECASE):
            return node
        if type_by_local.get(node) == "Zone":
            return node
        for target in feeds_map.get(node, []):
            if re.match(r"^zone_[0-9]+$", target, flags=re.IGNORECASE):
                return target
        node = parent_of.get(node)
    return None


def read_brick_ttl(*, input_dir: Path, cfg: dict[str, Any], inventory_df, brick_cfg: dict[str, Any]):
    ttl_path = Path(input_dir) / str(brick_cfg["ttl_path"]).strip()
    if not ttl_path.exists():
        raise FileNotFoundError(f"dryad_blg59 TTL not found: {ttl_path}")
    alias_map = {
        str(item["brick"]).strip(): str(item["csv"]).strip()
        for item in brick_cfg["point_alias_rules"]
        if str(item["brick"]).strip() and str(item["csv"]).strip()
    }
    graph = Graph()
    graph.parse(str(ttl_path), format="turtle")
    type_by_local = {}
    for subject, _, obj in graph.triples((None, RDF.type, None)):
        type_by_local[_local_name(subject)] = _local_name(obj)
    predicates_cfg = brick_cfg["predicates"]
    has_point_preds = _predicates_by_local_name(graph, list(predicates_cfg["has_point"]))
    has_part_preds = _predicates_by_local_name(graph, list(predicates_cfg["has_part"]))
    feeds_preds = _predicates_by_local_name(graph, list(predicates_cfg["feeds"]))
    is_fed_by_preds = _predicates_by_local_name(graph, list(predicates_cfg["is_fed_by"]))

    parent_of: dict[str, str] = {}
    for predicate in has_part_preds:
        for subject, _, obj in graph.triples((None, predicate, None)):
            parent_of[_local_name(obj)] = _local_name(subject)

    feeds_map: dict[str, list[str]] = {}
    for predicate in feeds_preds:
        for subject, _, obj in graph.triples((None, predicate, None)):
            feeds_map.setdefault(_local_name(subject), []).append(_local_name(obj))
    for predicate in is_fed_by_preds:
        for subject, _, obj in graph.triples((None, predicate, None)):
            feeds_map.setdefault(_local_name(obj), []).append(_local_name(subject))

    key_set = set(inventory_df["key"].astype("string").dropna().tolist())
    rows: list[dict[str, Any]] = []
    for predicate in has_point_preds:
        for owner, _, point in graph.triples((None, predicate, None)):
            local = _local_name(point)
            key = local if local in key_set else _alias_for(local, alias_map)
            if key not in key_set or _exclude_key(key):
                continue
            owner_id = _local_name(owner)
            point_class_local = _first_type_local(graph, local)
            zone_hint = _zone_from_owner(owner_id, parent_of, feeds_map, type_by_local)
            rows.append(
                {
                    "key": key,
                    "label": local,
                    "brick_uri": str(point),
                    "brick_class": f"brick:{point_class_local}" if point_class_local else None,
                    "brick_class_local": point_class_local,
                    "owner_id": owner_id,
                    "owner_class": type_by_local.get(owner_id),
                    "equip_hint": owner_id,
                    "zone_hint": zone_hint,
                    "location_hint": zone_hint,
                    "class_tag_hint": point_class_local,
                    "brick_exact_match": key == local,
                    "brick_alias_match": key != local,
                }
            )
    _ = cfg
    out = pd.DataFrame.from_records(rows)
    if out.empty:
        return pd.DataFrame(columns=["key", "label", "brick_uri", "brick_class"])
    return out.drop_duplicates(subset=["key"]).reset_index(drop=True)
