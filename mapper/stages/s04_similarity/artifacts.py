from __future__ import annotations

from mapper.stages.common import ArtifactSpec, TaskArtifactContract

BUILD_GRAPH_CONTRACT = TaskArtifactContract(
    stage_name="s04_similarity",
    task_name="build_graph",
    required_artifacts=(
        ArtifactSpec("vectors", "file", True, False, "vectors.npz"),
        ArtifactSpec("edges", "file", True, False, "edges_combined.npz"),
        ArtifactSpec("meta_filtered", "file", True, False, "_meta_filtered.feather"),
    ),
    optional_artifacts=(
        ArtifactSpec("reports", "directory", False, True, "report outputs"),
    ),
)

CONTRACTS = {"build_graph": BUILD_GRAPH_CONTRACT}
