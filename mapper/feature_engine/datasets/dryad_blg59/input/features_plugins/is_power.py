from __future__ import annotations

from copy import deepcopy
from typing import Any

from mapper.feature_engine.plugin_base import FeaturePlugin
from mapper.feature_engine.plugin_registry import register


class IsPower(FeaturePlugin):
    name = "is_power"

    def build_criteria(
        self,
        feature_def: dict[str, Any],
        template_cfg: dict[str, Any],
        criteria_overrides: dict[str, Any],
    ) -> dict[str, Any]:
        criteria = deepcopy(template_cfg)
        attributes = dict(criteria.get("attributes") or {})
        override_attributes = dict(criteria_overrides.get("attributes") or {})
        attributes.update(override_attributes)
        electric_terms = list(attributes.get("electric_terms") or ["KW", "KWH", "W"])
        criteria.update({key: value for key, value in criteria_overrides.items() if key != "attributes"})
        criteria["name"] = self.name
        criteria.setdefault("logic", "any")
        criteria.setdefault(
            "metadata_rules",
            [
                {"field": "unit", "op": "in", "values": electric_terms},
                {"field": "unit_candidates", "op": "contains_any", "values": electric_terms},
            ],
        )
        criteria.setdefault("timeseries_rules", [])
        criteria["attributes"] = {**attributes, "electric_terms": electric_terms}
        return criteria


register("is_power", IsPower())
