from __future__ import annotations


def score_comfort(rollout_summary: dict) -> float:
    return float(rollout_summary.get("comfort_penalty", 0.0))
