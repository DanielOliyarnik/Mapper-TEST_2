from __future__ import annotations

from pathlib import Path


def resolve_stage7_paths(work_dir: str | Path) -> dict[str, Path]:
    root = Path(work_dir)
    return {"root": root, "models": root / "models", "rollouts": root / "rollouts"}
