from __future__ import annotations

import argparse
import json
import time
from pathlib import Path
from typing import Any

from mapper.exec.pipeline_runner import run_pipeline


def load_cfg_path(path: str | Path) -> dict[str, Any]:
    cfg_path = Path(path)
    text = cfg_path.read_text(encoding="utf-8").strip()
    if not text:
        return {}
    if cfg_path.suffix.lower() in {".json", ".yaml", ".yml"}:
        try:
            loaded = json.loads(text)
        except json.JSONDecodeError as exc:
            raise NotImplementedError(
                f"Only JSON-compatible config syntax is supported in the first scaffold: {cfg_path}"
            ) from exc
        if not isinstance(loaded, dict):
            raise TypeError(f"Config root must be an object: {cfg_path}")
        return loaded
    raise TypeError(f"Unsupported config extension for {cfg_path}")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--cfg", default="configs/baseline.yaml")
    parser.add_argument("--run", default="baseline", help="Run directory name")
    parser.add_argument("--stages", nargs="*", default=None, help="Stage names or numbers; 0 means all")
    parser.add_argument("--retrain", nargs="*", default=[], help="Stage names or numbers; 0 means all")
    args = parser.parse_args()

    cfg = load_cfg_path(args.cfg)
    t0 = time.time()
    summary = run_pipeline(cfg, run_name=args.run, stages=args.stages, retrain=args.retrain)
    elapsed = time.time() - t0
    stage_summary = ", ".join(f"{item['stage_name']}={item['status']}" for item in summary["stages"])
    print(f"Finished Mapper Run: {stage_summary} in {elapsed:,.1f}s")


if __name__ == "__main__":
    main()
