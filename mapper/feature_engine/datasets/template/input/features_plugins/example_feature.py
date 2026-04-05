from __future__ import annotations

from typing import Any

from mapper.feature_engine.plugin_base import FeaturePlugin
from mapper.feature_engine.plugin_registry import register


class ExampleFeature(FeaturePlugin):
    name = "example_feature"

    def build_criteria(
        self,
        feature_def: dict[str, Any],
        template_cfg: dict[str, Any],
        criteria_overrides: dict[str, Any],
    ) -> dict[str, Any]:
        criteria = dict(template_cfg)
        criteria.update(criteria_overrides)
        criteria.setdefault("name", self.name)
        criteria.setdefault("logic", "all")
        criteria.setdefault("metadata_rules", [])
        criteria.setdefault("timeseries_rules", [])
        criteria.setdefault("attributes", {})
        return criteria


register("example_feature", ExampleFeature())
