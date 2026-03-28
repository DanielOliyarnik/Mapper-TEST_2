from __future__ import annotations

from typing import Any, Mapping


def resolve_exogenous_step(exogenous_plan: Mapping[str, Any], step_idx: int) -> dict[str, Any]:
    resolved: dict[str, Any] = {}
    for key, value in exogenous_plan.items():
        if isinstance(value, list):
            resolved[key] = value[min(step_idx, len(value) - 1)] if value else None
        else:
            resolved[key] = value
    return resolved
