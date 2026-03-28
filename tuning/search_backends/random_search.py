from __future__ import annotations

import random
from typing import Any


def propose_trial(search_space: dict[str, dict[str, Any]], *, seed: int | None = None) -> dict[str, Any]:
    rng = random.Random(seed)
    proposal: dict[str, Any] = {}
    for key, spec in search_space.items():
        choices = spec.get("choices")
        if choices:
            proposal[key] = rng.choice(list(choices))
        else:
            proposal[key] = spec.get("default")
    return proposal
