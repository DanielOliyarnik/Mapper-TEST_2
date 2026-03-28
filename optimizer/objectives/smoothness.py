from __future__ import annotations


def score_smoothness(candidate_summary: dict) -> float:
    return float(candidate_summary.get("smoothness_penalty", 0.0))
