from __future__ import annotations


def check_rate_limits(candidate: dict, max_delta: float) -> bool:
    for values in candidate.values():
        deltas = [abs(values[idx] - values[idx - 1]) for idx in range(1, len(values))]
        if any(delta > max_delta for delta in deltas):
            return False
    return True
