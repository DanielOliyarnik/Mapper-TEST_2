from __future__ import annotations

from mapper.features.plugin_base import FeaturePlugin, FeatureResult
from mapper.features.registry import register


class ExampleFeature(FeaturePlugin):
    name = "example_feature"

    def infer_attributes(self, series, static: dict[str, object], params: dict[str, object]) -> dict[str, object]:
        return {"attributes": {"static_keys": sorted(static)}}

    def match(self, series, static: dict[str, object], config: dict[str, object]) -> FeatureResult:
        return FeatureResult(matched=bool(static), details={"reason": "scaffold"})


register("example_feature", ExampleFeature())
