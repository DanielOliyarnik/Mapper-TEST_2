from __future__ import annotations


def check_bounds(candidate: dict, bounds: dict) -> bool:
    for key, values in candidate.items():
        lo, hi = bounds.get(key, (None, None))
        for value in values:
            if lo is not None and value < lo:
                return False
            if hi is not None and value > hi:
                return False
    return True


def clip_to_bounds(candidate: dict, bounds: dict) -> dict:
    clipped: dict = {}
    for key, values in candidate.items():
        lo, hi = bounds.get(key, (None, None))
        clipped[key] = [
            max(lo, min(hi, value)) if lo is not None and hi is not None else value
            for value in values
        ]
    return clipped
