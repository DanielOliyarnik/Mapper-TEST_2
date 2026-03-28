from __future__ import annotations

from typing import Any


def build_historical_window(history_values: list[Any], window_size: int) -> list[Any]:
    if window_size <= 0:
        return []
    return list(history_values[-window_size:])
