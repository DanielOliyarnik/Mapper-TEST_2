from __future__ import annotations

from .contracts import OptimizerStateSlice


def adapt_runtime_state(observed_state: dict, timestamp: str) -> OptimizerStateSlice:
    return OptimizerStateSlice(timestamp=timestamp, observed_state=dict(observed_state))
