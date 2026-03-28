from __future__ import annotations


def score_effort(candidate_summary: dict) -> float:
    return float(candidate_summary.get("effort_penalty", 0.0))
