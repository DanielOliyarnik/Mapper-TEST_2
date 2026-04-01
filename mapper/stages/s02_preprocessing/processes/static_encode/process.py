from __future__ import annotations

from typing import Any

from ..process_base import Payload, ProcessBase


class Process(ProcessBase):
    def apply(self, proc_payload: Payload) -> Payload:
        meta = proc_payload["meta"]
        contract_fields = list(proc_payload.get("meta_contract_fields") or [])
        static_table = proc_payload["static"]

        static_table = self._build_static_table(meta, static_table, contract_fields)
        proc_payload["static"] = static_table

        enc_cfg = self.cfg.get("encode") or {}
        enc_type = str(enc_cfg.get("type") or "").lower()
        if enc_type == "onehot":
            enc_roles = list(enc_cfg.get("roles") or [])
            enc_units = list(enc_cfg.get("units") or [])
            enc_feats = list(enc_cfg.get("features") or [])

            role_val = str(static_table.get("role", "")).lower()
            unit_val = str(static_table.get("unit", "")).upper()

            role_vec = [1.0 if role_val == role else 0.0 for role in enc_roles]
            unit_vec = [1.0 if unit_val == unit else 0.0 for unit in enc_units]
            feat_vec = [1.0 if bool(static_table.get(feature, False)) else 0.0 for feature in enc_feats]

            static_vec = role_vec + unit_vec + feat_vec
            if not static_vec:
                raise ValueError(f"-=== (static_encode): Static vector was empty; meta: {meta} ===-")
            static_table["static_vec"] = [float(value) for value in static_vec]
            proc_payload["static"] = static_table

        return proc_payload

    def _build_static_table(self, meta_table: dict[str, Any], static_table: dict[str, Any], contract_fields: list[str]) -> dict[str, Any]:
        cfg_fields = list(self.cfg.get("fields") or [])
        fields: list[str] = []
        seen = set()
        for field in cfg_fields + list(contract_fields):
            token = str(field).strip()
            if not token or token in seen:
                continue
            seen.add(token)
            fields.append(token)
        for field in fields:
            value = meta_table.get(field)
            if value is not None and value != "":
                static_table[field] = value
        return static_table
