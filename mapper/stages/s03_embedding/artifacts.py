from __future__ import annotations

from mapper.stages.common import ArtifactSpec, TaskArtifactContract

TRAIN_EXPORT_CONTRACT = TaskArtifactContract(
    stage_name="s03_embedding",
    task_name="train_export",
    required_artifacts=(
        ArtifactSpec("embeddings_dir", "directory", True, True, "Embedding npz directory"),
        ArtifactSpec("checkpoints_dir", "directory", True, True, "Checkpoint directory"),
        ArtifactSpec("run_json", "file", True, False, "Run metadata"),
    ),
)

CONTRACTS = {"train_export": TRAIN_EXPORT_CONTRACT}
