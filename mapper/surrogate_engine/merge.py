from __future__ import annotations

from .contracts import RolloutResult


def merge_overlapping_outputs(results: list[RolloutResult]) -> dict[str, object]:
    return {"zone_count": len(results), "statuses": [item.status for item in results]}
