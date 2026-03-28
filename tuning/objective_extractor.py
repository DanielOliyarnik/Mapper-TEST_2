from __future__ import annotations

from typing import Any


def extract_objectives(summary: dict[str, Any]) -> dict[str, float]:
    extracted: dict[str, float] = {}
    for key, value in summary.items():
        if isinstance(value, (int, float)):
            extracted[key] = float(value)
    return extracted
