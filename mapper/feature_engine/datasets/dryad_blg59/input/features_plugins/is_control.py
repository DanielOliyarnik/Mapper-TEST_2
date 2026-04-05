from __future__ import annotations

from copy import deepcopy
from typing import Any

from mapper.feature_engine.plugin_base import FeaturePlugin
from mapper.feature_engine.plugin_registry import register


class IsControl(FeaturePlugin):
    name = "is_control"

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
        control_terms = list(attributes.get("control_terms") or ["CMD", "SET"])
        criteria.update({key: value for key, value in criteria_overrides.items() if key != "attributes"})
        criteria["name"] = self.name
        criteria.setdefault("logic", "all")
        criteria.setdefault("metadata_rules", [{"field": "label", "op": "contains_any", "values": control_terms}])
        criteria.setdefault("timeseries_rules", [])
        criteria["attributes"] = {**attributes, "control_terms": control_terms}
        return criteria


register("is_control", IsControl())
