from __future__ import annotations

from .contracts import ForecastPlan


def adapt_forecast_plan(raw_plan: dict) -> ForecastPlan:
    return ForecastPlan(horizon_steps=int(raw_plan.get("horizon_steps", 1)), channels=dict(raw_plan.get("channels", {})))
