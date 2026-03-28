from __future__ import annotations


def adapt_rollout_result(result) -> dict[str, object]:
    return {"predictions": dict(result.predictions), "summary": dict(result.summary)}
