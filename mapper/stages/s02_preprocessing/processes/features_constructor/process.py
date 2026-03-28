from __future__ import annotations

from ..process_base import ProcessBase


def run_process(*args, **kwargs):
    raise NotImplementedError("features_constructor.run_process is not implemented in the first scaffold")


class Process(ProcessBase):
    name = "features_constructor"

    def run_process(self, *args, **kwargs):
        return run_process(*args, **kwargs)
