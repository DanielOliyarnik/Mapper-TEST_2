from __future__ import annotations

from typing import Any, Callable

_EVALUATORS: dict[str, Callable[..., Any]] = {}


def register_evaluator(name: str, evaluator_fn: Callable[..., Any]) -> None:
    _EVALUATORS[name] = evaluator_fn


def get_evaluator(name: str) -> Callable[..., Any]:
    if name not in _EVALUATORS:
        raise KeyError(f"Unknown evaluator: {name}")
    return _EVALUATORS[name]


def load_evaluator(name: str) -> Callable[..., Any]:
    return get_evaluator(name)


def list_evaluators() -> list[str]:
    return sorted(_EVALUATORS)
