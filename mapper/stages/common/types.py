from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Mapping


@dataclass(frozen=True)
class StageSpec:
    name: str
    order: int
    default_task: str
    supported_tasks: tuple[str, ...]
    required_upstream: tuple[str, ...] = ()
    description: str = ""


@dataclass(frozen=True)
class InputRef:
    name: str
    source_stage: str
    artifact_key: str
    path: Path
    manifest_path: Path
    resolution_mode: str
    required: bool = True


@dataclass(frozen=True)
class ArtifactRef:
    key: str
    path: Path
    kind: str
    role: str
    required: bool = True


@dataclass(frozen=True)
class ArtifactBundle:
    primary: tuple[ArtifactRef, ...] = ()
    secondary: tuple[ArtifactRef, ...] = ()

    def all_refs(self) -> tuple[ArtifactRef, ...]:
        return self.primary + self.secondary


@dataclass(frozen=True)
class StageWarning:
    code: str
    message: str


@dataclass
class StageTaskRequest:
    stage_name: str
    task_name: str
    dataset_id: str
    run_name: str
    timestamp: str
    cache_dir: Path
    data_dir: Path
    config: Mapping[str, Any]
    input_refs: Mapping[str, InputRef]
    upstream_manifest_refs: tuple[Path, ...] = ()


@dataclass
class StageTaskResult:
    status: str
    artifact_bundle: ArtifactBundle
    metrics: Mapping[str, Any] = field(default_factory=dict)
    warnings: tuple[StageWarning, ...] = ()
    extras: Mapping[str, Any] = field(default_factory=dict)
