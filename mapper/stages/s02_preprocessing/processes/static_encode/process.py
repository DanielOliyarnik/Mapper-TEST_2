from __future__ import annotations

from ..process_base import ProcessBase


def run_process(*args, **kwargs):
    raise NotImplementedError("static_encode.run_process is not implemented in the first scaffold")


class Process(ProcessBase):
    name = "static_encode"

    def run_process(self, *args, **kwargs):
        return run_process(*args, **kwargs)
