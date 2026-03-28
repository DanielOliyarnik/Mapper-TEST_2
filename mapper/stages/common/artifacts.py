from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from .types import ArtifactBundle, ArtifactRef


@dataclass(frozen=True)
class ArtifactSpec:
    key: str
    kind: str
    required: bool
    is_directory: bool
    description: str = ""


@dataclass(frozen=True)
class TaskArtifactContract:
    stage_name: str
    task_name: str
    required_artifacts: tuple[ArtifactSpec, ...]
    optional_artifacts: tuple[ArtifactSpec, ...] = ()


def _ref_exists(ref: ArtifactRef) -> bool:
    path = Path(ref.path)
    return path.is_dir() if ref.kind == "directory" else path.is_file()


def validate_artifact_bundle(bundle: ArtifactBundle, contract: TaskArtifactContract) -> None:
    refs = {ref.key: ref for ref in bundle.all_refs()}
    for spec in contract.required_artifacts:
        if spec.key not in refs:
            raise ValueError(f"{contract.stage_name}.{contract.task_name}: missing artifact {spec.key!r}")
        if spec.required and not _ref_exists(refs[spec.key]):
            raise FileNotFoundError(f"Expected artifact path does not exist: {refs[spec.key].path}")


def require_artifacts(bundle: ArtifactBundle, *keys: str) -> dict[str, ArtifactRef]:
    refs = {ref.key: ref for ref in bundle.all_refs()}
    missing = [key for key in keys if key not in refs]
    if missing:
        raise KeyError(f"Missing artifacts: {', '.join(missing)}")
    return {key: refs[key] for key in keys}
