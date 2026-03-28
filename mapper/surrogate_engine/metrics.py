from __future__ import annotations

from .contracts import RolloutResult


def build_rollout_summary(result: RolloutResult) -> dict[str, object]:
    return {"status": result.status, **dict(result.summary)}
