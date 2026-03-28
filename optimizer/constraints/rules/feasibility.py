from __future__ import annotations

from .bounds import check_bounds
from .rate_limits import check_rate_limits


def check_feasibility(candidate: dict, *, bounds: dict, max_delta: float) -> bool:
    return check_bounds(candidate, bounds) and check_rate_limits(candidate, max_delta)
