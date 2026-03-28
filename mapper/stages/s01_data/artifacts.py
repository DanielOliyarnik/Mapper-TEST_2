from __future__ import annotations

from mapper.stages.common import ArtifactSpec, TaskArtifactContract

BUILD_CONTRACT = TaskArtifactContract(
    stage_name="s01_data",
    task_name="build",
    required_artifacts=(
        ArtifactSpec("inventory", "file", True, False, "inventory.feather"),
        ArtifactSpec("raw_store", "file", True, False, "raw_store.h5"),
        ArtifactSpec("metadata", "file", True, False, "metadata.feather"),
        ArtifactSpec("brickdata", "file", True, False, "brickdata.feather"),
        ArtifactSpec("ledger", "file", True, False, "ledger.feather"),
    ),
)

CONTRACTS = {"build": BUILD_CONTRACT}
