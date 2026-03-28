from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class TrialBudget:
    max_trials: int
    max_failures: int = 0

    def allow_trial(self, num_started: int, num_failed: int) -> bool:
        return num_started < self.max_trials and num_failed <= self.max_failures
