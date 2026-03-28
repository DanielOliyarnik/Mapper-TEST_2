from __future__ import annotations

from pathlib import Path

from .contracts import LoadedSurrogateRuntime


def load_runtime_from_artifacts(zone_root: str | Path, zone_name: str | None = None) -> LoadedSurrogateRuntime:
    root = Path(zone_root)
    name = zone_name or root.name
    model_paths = {
        file_path.stem: file_path
        for file_path in root.glob("*.pt")
    }
    return LoadedSurrogateRuntime(zone_name=name, root=root, model_paths=model_paths)
