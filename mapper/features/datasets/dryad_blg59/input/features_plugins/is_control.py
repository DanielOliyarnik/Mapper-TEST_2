from __future__ import annotations

from mapper.features.plugin_base import FeaturePlugin, FeatureResult, label_in_list
from mapper.features.registry import register


class IsControl(FeaturePlugin):
    name = "is_control"

    def infer_attributes(self, series, static: dict[str, object], params: dict[str, object]) -> dict[str, object]:
        return {"attributes": {"control_terms": params.get("control_terms", ["CMD", "SET"])}}

    def match(self, series, static: dict[str, object], config: dict[str, object]) -> FeatureResult:
        label = str(static.get("label") or "")
        terms = list(config.get("control_terms", ["CMD", "SET"]))
        return FeatureResult(matched=label_in_list(label, terms), details={"label": label, "terms": terms})


register("is_control", IsControl())
