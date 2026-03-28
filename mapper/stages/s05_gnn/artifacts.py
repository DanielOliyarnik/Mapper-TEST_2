from __future__ import annotations

from mapper.stages.common import ArtifactSpec, TaskArtifactContract

TRAIN_CONTRACT = TaskArtifactContract(
    stage_name="s05_gnn",
    task_name="train",
    required_artifacts=(
        ArtifactSpec("h_node", "file", True, False, "h_node.npy"),
        ArtifactSpec("node_keys", "file", True, False, "node_keys.txt"),
    ),
    optional_artifacts=(
        ArtifactSpec("attn_final", "file", False, False, "attn_final.npz"),
    ),
)

CONTRACTS = {"train": TRAIN_CONTRACT}
