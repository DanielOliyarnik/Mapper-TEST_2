from __future__ import annotations

from ..trainer_base import TrainerBase


def fit_trainer(*args, **kwargs):
    raise NotImplementedError("features_constructor.fit_trainer is not implemented in the first scaffold")


class Trainer(TrainerBase):
    name = "features_constructor"

    def fit_trainer(self, *args, **kwargs):
        return fit_trainer(*args, **kwargs)
