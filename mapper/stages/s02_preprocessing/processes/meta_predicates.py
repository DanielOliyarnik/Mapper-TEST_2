from __future__ import annotations

import re
from typing import Any


def _ctx_value(context: dict[str, Any], field: str) -> Any:
    if "." in field:
        head, tail = field.split(".", 1)
        top = context.get(head)
        if isinstance(top, dict):
            return top.get(tail)
    return context.get(field)


def _to_list(value: Any) -> list[Any]:
    if isinstance(value, list):
        return value
    if value is None:
        return []
    return [value]


def _as_text(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, list):
        return "|".join(str(v) for v in value)
    return str(value)


def eval_condition(context: dict[str, Any], cond: dict[str, Any]) -> bool:
    field = str(cond.get("field") or "").strip()
    if not field:
        return False
    op = str(cond.get("op") or "eq").strip().lower()
    value = _ctx_value(context, field)

    if op == "exists":
        return value is not None and _as_text(value).strip() != ""
    if op == "truthy":
        return bool(value)

    expected = cond.get("value")
    expected_list = cond.get("values")

    text = _as_text(value)
    text_up = text.upper()

    if op == "eq":
        return text_up == _as_text(expected).upper()
    if op == "ne":
        return text_up != _as_text(expected).upper()
    if op == "contains":
        return _as_text(expected).upper() in text_up
    if op == "regex":
        flags = re.IGNORECASE if bool(cond.get("ignore_case", True)) else 0
        pattern = str(expected or "")
        if not pattern:
            return False
        return bool(re.search(pattern, text, flags=flags))
    if op == "in":
        choices = {_as_text(v).upper() for v in _to_list(expected_list if expected_list is not None else expected)}
        return text_up in choices
    if op == "not_in":
        choices = {_as_text(v).upper() for v in _to_list(expected_list if expected_list is not None else expected)}
        return text_up not in choices

    return False


def eval_predicate(context: dict[str, Any], pred: Any) -> bool:
    if pred is None:
        return True
    if isinstance(pred, list):
        return all(eval_predicate(context, item) for item in pred)
    if not isinstance(pred, dict):
        return False
    if "all" in pred:
        return all(eval_predicate(context, item) for item in (pred.get("all") or []))
    if "any" in pred:
        return any(eval_predicate(context, item) for item in (pred.get("any") or []))
    if "not" in pred:
        return not eval_predicate(context, pred.get("not"))
    return eval_condition(context, pred)
