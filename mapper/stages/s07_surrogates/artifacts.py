from __future__ import annotations

from mapper.stages.common import ArtifactSpec, TaskArtifactContract

TRAIN_CONTRACT = TaskArtifactContract(
    stage_name="s07_surrogates",
    task_name="train_zone",
    required_artifacts=(
        ArtifactSpec("zone_model_root", "directory", True, True, "Zone training output root"),
    ),
)
ROLL_OUT_CONTRACT = TaskArtifactContract(
    stage_name="s07_surrogates",
    task_name="rollout_one",
    required_artifacts=(
        ArtifactSpec("rollout_summary", "file", True, False, "rollout summary json"),
    ),
)
CONTRACTS = {
    "train_zone": TRAIN_CONTRACT,
    "train_all": TRAIN_CONTRACT,
    "rollout_one": ROLL_OUT_CONTRACT,
    "rollout_zone": ROLL_OUT_CONTRACT,
    "rollout_all": ROLL_OUT_CONTRACT,
}
