from __future__ import annotations

from typing import Any


def normalize_search_space(raw_space: dict[str, Any]) -> dict[str, dict[str, Any]]:
    normalized: dict[str, dict[str, Any]] = {}
    for key, value in raw_space.items():
        if isinstance(value, dict):
            normalized[key] = dict(value)
        else:
            normalized[key] = {"choices": list(value)}
    return normalized
