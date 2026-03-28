from __future__ import annotations

from typing import Any

from .evaluator_registry import get_evaluator


def run_evaluator(name: str, *args: Any, **kwargs: Any) -> Any:
    evaluator_fn = get_evaluator(name)
    return evaluator_fn(*args, **kwargs)
