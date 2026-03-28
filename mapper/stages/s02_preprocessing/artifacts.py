from __future__ import annotations

from mapper.stages.common import ArtifactSpec, TaskArtifactContract

BUILD_CONTRACT = TaskArtifactContract(
    stage_name="s02_preprocessing",
    task_name="build",
    required_artifacts=(
        ArtifactSpec("xnode_ts", "file", True, False, "xnode_ts.h5"),
        ArtifactSpec("xnode_meta", "file", True, False, "xnode_meta.feather"),
        ArtifactSpec("xnode_ledger", "file", True, False, "xnode_ledger.feather"),
    ),
)

CONTRACTS = {"build": BUILD_CONTRACT}
