from __future__ import annotations

import re
import pandas as pd
from typing import Any


def _norm_col(series: pd.Series) -> pd.Series:
    out = series.astype("string", copy=False)
    out = out.str.strip()
    return out.mask(out == "", pd.NA)


def _normalize_keys(df: pd.DataFrame, policy: dict[str, Any]) -> pd.Series:
    key = _norm_col(df["key"])
    case = str(policy["case"]).lower() if "case" in policy else "preserve"
    if case == "upper":
        key = key.str.upper()
    elif case == "lower":
        key = key.str.lower()
    return key


def _extract_regex(source_df: pd.DataFrame, source: dict[str, Any]) -> pd.Series:
    column = str(source["column"]) if "column" in source else "key"
    pattern = str(source["pattern"]).strip()
    if not pattern:
        return pd.Series([pd.NA] * len(source_df), index=source_df.index, dtype="string")
    src = _norm_col(source_df[column]) if column in source_df.columns else pd.Series([pd.NA] * len(source_df), index=source_df.index, dtype="string")
    try:
        extracted = src.str.extract(pattern, expand=True)
    except ValueError as exc:
        if "no capture groups" not in str(exc):
            raise
        extracted = src.str.extract(f"({pattern})", expand=True)
    if isinstance(extracted, pd.DataFrame):
        if extracted.shape[1] == 0:
            cap = pd.Series([pd.NA] * len(source_df), index=source_df.index, dtype="string")
        else:
            group_name = source.get("group")
            if group_name and group_name in extracted.columns:
                cap = extracted[group_name]
            elif "zone_code" in extracted.columns:
                cap = extracted["zone_code"]
            else:
                cap = extracted.iloc[:, 0]
    else:
        cap = extracted
    cap = _norm_col(cap)
    if "map" in source and isinstance(source["map"], dict) and source["map"]:
        cap = cap.map(source["map"]).fillna(cap)
    if "template" in source and isinstance(source["template"], str) and source["template"]:
        cap = cap.apply(lambda value: source["template"].format(capture=value) if pd.notna(value) else pd.NA)
    if bool(source.get("to_upper", False)):
        cap = cap.str.upper()
    return cap


def _extract_regex_template(source_df: pd.DataFrame, source: dict[str, Any]) -> pd.Series:
    column = str(source["column"]) if "column" in source else "key"
    pattern = str(source["pattern"]).strip()
    template = str(source["template"]).strip()
    if not pattern or not template:
        return pd.Series([pd.NA] * len(source_df), index=source_df.index, dtype="string")
    rx = re.compile(pattern, flags=re.IGNORECASE)
    src = _norm_col(source_df[column]) if column in source_df.columns else pd.Series([pd.NA] * len(source_df), index=source_df.index, dtype="string")

    def _render(value: Any):
        if pd.isna(value):
            return pd.NA
        text = str(value)
        match = rx.search(text)
        if not match:
            return pd.NA
        groups = dict(match.groupdict())
        if match.groups():
            groups.setdefault("capture", match.group(1))
            for idx, group in enumerate(match.groups(), start=1):
                groups.setdefault(f"g{idx}", group)
        groups.setdefault("value", text)
        try:
            rendered = template.format(**groups)
        except Exception:
            rendered = groups.get("capture") or groups.get("value")
        rendered_s = str(rendered).strip()
        return rendered_s if rendered_s else pd.NA

    out = src.apply(_render).astype("string")
    if bool(source.get("to_upper", False)):
        out = out.str.upper()
    return out


def _first_from_sources(base_df: pd.DataFrame, sources: list[dict[str, Any]]) -> pd.Series:
    out = pd.Series([pd.NA] * len(base_df), index=base_df.index, dtype="string")
    for source in sources:
        source_type = str(source["type"]).strip().lower()
        if source_type == "column":
            column = str(source["column"])
            candidate = _norm_col(base_df[column]) if column in base_df.columns else pd.Series([pd.NA] * len(base_df), index=base_df.index, dtype="string")
        elif source_type == "regex":
            candidate = _extract_regex(base_df, source)
        elif source_type == "regex_template":
            candidate = _extract_regex_template(base_df, source)
        elif source_type == "constant":
            value = str(source["value"]).strip() if "value" in source else ""
            if not value:
                continue
            candidate = pd.Series([value] * len(base_df), index=base_df.index, dtype="string")
        else:
            continue
        out = out.fillna(candidate)
    return out


def _match_any_sources(base_df: pd.DataFrame, sources: list[dict[str, Any]]) -> pd.Series:
    out = pd.Series([False] * len(base_df), index=base_df.index, dtype="bool")
    for source in sources:
        source_type = str(source["type"]).strip().lower()
        if source_type == "column":
            column = str(source["column"])
            candidate = _norm_col(base_df[column]) if column in base_df.columns else pd.Series([pd.NA] * len(base_df), index=base_df.index, dtype="string")
            matched = candidate.notna()
        elif source_type == "regex":
            matched = _extract_regex(base_df, source).notna()
        elif source_type == "regex_template":
            matched = _extract_regex_template(base_df, source).notna()
        elif source_type == "constant":
            value = str(source["value"]).strip() if "value" in source else ""
            matched = pd.Series([bool(value)] * len(base_df), index=base_df.index, dtype="bool")
        else:
            continue
        out = out | matched
    return out


def _assign_loop_ids(merged: pd.DataFrame, zone_id: pd.Series, id_rules: dict[str, Any]) -> pd.Series:
    loop_cfg = id_rules["loop_ids"]
    rules = list(loop_cfg["rules"])
    delimiter = str(loop_cfg["delimiter"])
    out_lists: list[list[str]] = [[] for _ in range(len(merged))]
    declared_ids: list[str] = []

    def _to_loop_list(raw: Any) -> list[str]:
        if raw is None:
            return []
        if isinstance(raw, list):
            values = raw
        else:
            text = str(raw).strip()
            if not text:
                return []
            values = [token.strip() for token in text.split(delimiter)]
        out: list[str] = []
        seen: set[str] = set()
        for value in values:
            token = str(value).strip()
            if not token or token in seen:
                continue
            seen.add(token)
            out.append(token)
        return out

    for rule in rules:
        loop_id = str(rule["id"]).strip() if "id" in rule and rule["id"] is not None else ""
        set_value = rule["set"] if "set" in rule else None
        if set_value is None and loop_id:
            set_value = loop_id
        set_loops = _to_loop_list(set_value)
        if loop_id:
            declared_ids.append(loop_id)
        for loop in set_loops:
            if loop and loop not in declared_ids:
                declared_ids.append(loop)
        src = list(rule["match_any"]) if "match_any" in rule else []
        merged_loop = merged.copy()
        merged_loop["_zone_id"] = zone_id.astype("string")
        matched = _match_any_sources(merged_loop, src) if src else pd.Series([True] * len(merged_loop), index=merged_loop.index, dtype="bool")
        allow = set(str(value) for value in rule.get("zone_allowlist", []))
        deny = set(str(value) for value in rule.get("zone_denylist", []))
        if allow:
            matched = matched & zone_id.astype("string").isin(allow)
        if deny:
            matched = matched & ~zone_id.astype("string").isin(deny)
        mode = str(rule["mode"]).strip().lower() if "mode" in rule else ("clear" if set_value is None else "append")
        for idx, ok in enumerate(matched.tolist()):
            if not ok:
                continue
            if mode == "clear":
                out_lists[idx] = []
            elif mode in {"replace", "set"}:
                out_lists[idx] = list(set_loops)
            else:
                for loop in set_loops:
                    if loop not in out_lists[idx]:
                        out_lists[idx].append(loop)

    fallback = loop_cfg["fallback"]
    mode = str(fallback["mode"]).lower()
    exclude_zone_ids = set(str(value) for value in fallback.get("exclude_zone_ids", []))
    if mode in {"zone_all", "all_zone_loops"} and declared_ids:
        for idx, loops in enumerate(out_lists):
            if loops:
                continue
            zone_token = str(zone_id.iloc[idx]) if idx < len(zone_id) and pd.notna(zone_id.iloc[idx]) else ""
            if not zone_token or zone_token in exclude_zone_ids:
                continue
            out_lists[idx] = list(dict.fromkeys(declared_ids))

    out_vals: list[str | Any] = []
    for loops in out_lists:
        out_vals.append(delimiter.join(loops) if loops else pd.NA)
    return pd.Series(out_vals, index=merged.index, dtype="string")


def _apply_regex_map(source: pd.Series, regex_rules: list[dict[str, Any]], default: str | None) -> pd.Series:
    out = pd.Series([pd.NA] * len(source), index=source.index, dtype="string")
    src = source.astype("string")
    for rule in regex_rules:
        pattern = str(rule["pattern"]).strip()
        if not pattern:
            continue
        value = rule.get("value")
        template = str(rule["template"]).strip() if "template" in rule and rule["template"] is not None else ""
        mask = src.str.contains(pattern, regex=True, case=False, na=False)
        unresolved = out.isna() & mask
        if not unresolved.any():
            continue
        if template:
            try:
                extracted = src.loc[unresolved].str.extract(pattern, flags=re.IGNORECASE, expand=True)
            except ValueError as exc:
                if "no capture groups" not in str(exc):
                    raise
                extracted = src.loc[unresolved].str.extract(f"({pattern})", flags=re.IGNORECASE, expand=True)
            if isinstance(extracted, pd.DataFrame):
                cap = extracted.iloc[:, 0] if extracted.shape[1] else pd.Series(index=src.loc[unresolved].index, dtype="string")
            else:
                cap = extracted
            mapped = cap.astype("string").apply(lambda token: template.format(capture=token) if pd.notna(token) and str(token).strip() else pd.NA)
            out.loc[unresolved] = mapped
        else:
            token = str(value).strip() if value is not None else ""
            if token:
                out.loc[unresolved] = token
    if default is not None:
        out = out.fillna(str(default))
    return out


def assign_standard_ids(*, key_df, source_df, id_rules: dict[str, Any]):
    base = key_df.loc[:, ["key"]].copy()
    source = source_df.copy() if source_df is not None else pd.DataFrame(columns=["key"])
    if "key" not in source.columns:
        source = pd.DataFrame(columns=["key"])
    merged = base.merge(source, on="key", how="left", copy=False)
    merged["key"] = _normalize_keys(merged, id_rules["key_normalization"])

    location_cfg = id_rules["location"]
    location_seed = _first_from_sources(merged, list(location_cfg["sources"]))
    merged["location_seed"] = location_seed

    zone_cfg = id_rules["zone_id"]
    zone_id = _first_from_sources(merged, list(zone_cfg["sources"]))
    fallback = zone_cfg["fallback"]
    if str(fallback["mode"]).lower() in {"constant", "global", "global_bucket"}:
        zone_id = zone_id.fillna(str(fallback["value"]))
    strictness = str(id_rules["strictness"]["missing_zone_id"]).lower()
    missing = zone_id.isna()
    if missing.any():
        if strictness == "fail":
            raise ValueError("ID assignment failed: missing zone_id for one or more keys")
        if strictness == "drop":
            keep = ~missing
            merged = merged.loc[keep].reset_index(drop=True)
            zone_id = zone_id.loc[keep].reset_index(drop=True)
        elif strictness == "warn_and_global":
            zone_id = zone_id.fillna(str(fallback["value"]))

    group_cfg = id_rules["group_id"]
    policy = str(group_cfg["policy"]).lower()
    group_id = zone_id.copy()
    if policy == "map" and "map" in group_cfg and isinstance(group_cfg["map"], dict):
        group_id = group_id.map(group_cfg["map"]).fillna(group_id)
    elif policy == "regex_map":
        default = group_cfg.get("default")
        group_id = _apply_regex_map(source=zone_id, regex_rules=list(group_cfg["regex_map"]), default=str(default) if default is not None else None)
        if bool(group_cfg.get("fallback_to_zone_id", True)):
            group_id = group_id.fillna(zone_id)

    equip_cfg = id_rules["equip_id"]
    equip_id = pd.Series([pd.NA] * len(merged), index=merged.index, dtype="string")
    if bool(equip_cfg.get("enabled", True)):
        equip_id = _first_from_sources(merged, list(equip_cfg["sources"]))

    class_cfg = id_rules["class"]
    class_col = pd.Series([pd.NA] * len(merged), index=merged.index, dtype="string")
    if bool(class_cfg.get("enabled", True)):
        delimiter = str(class_cfg["delimiter"])
        components = list(class_cfg["components"])
        tmp = merged.copy()
        tmp["zone_id"] = zone_id
        tmp["group_id"] = group_id
        tmp["equip_id"] = equip_id
        extras = list(class_cfg.get("extra_tags", []))
        for idx, source in enumerate(extras):
            name = f"_extra_{idx}"
            if isinstance(source, str):
                tmp[name] = _norm_col(tmp[source]) if source in tmp.columns else pd.Series([pd.NA] * len(tmp), index=tmp.index)
            elif isinstance(source, dict):
                tmp[name] = _first_from_sources(tmp, [source])
            components.append(name)
        values = []
        for _, row in tmp.iterrows():
            parts = []
            for component in components:
                value = row.get(component, pd.NA)
                if pd.isna(value) or str(value).strip() == "":
                    continue
                parts.append(str(value))
            values.append(delimiter.join(parts) if parts else pd.NA)
        class_col = pd.Series(values, index=tmp.index, dtype="string")

    location_default = location_seed.copy()
    if str(location_cfg["default_from"]) == "zone_id":
        location_default = location_default.fillna(zone_id)

    loop_ids = _assign_loop_ids(merged=merged, zone_id=zone_id, id_rules=id_rules)
    out = pd.DataFrame(
        {
            "key": merged["key"],
            "zone_id": zone_id,
            "group_id": group_id,
            "equip_id": equip_id,
            "class": class_col,
            "location_default": location_default,
            "loop_ids": loop_ids,
        }
    )
    return out.drop_duplicates(subset=["key"]).reset_index(drop=True)
