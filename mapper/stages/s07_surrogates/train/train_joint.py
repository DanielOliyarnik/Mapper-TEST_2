from __future__ import annotations


def run_joint_training(*args, **kwargs):
    raise NotImplementedError("Stage 7 joint training is not implemented in the first scaffold")


def describe_training_progress() -> dict[str, object]:
    return {
        "style": "two_line",
        "basis": "stage7_reference",
    }
