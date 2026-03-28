from __future__ import annotations

from ..trainer_base import TrainerBase


def fit_trainer(*args, **kwargs):
    raise NotImplementedError("smoothing_filtering.fit_trainer is not implemented in the first scaffold")


class Trainer(TrainerBase):
    name = "smoothing_filtering"

    def fit_trainer(self, *args, **kwargs):
        return fit_trainer(*args, **kwargs)
