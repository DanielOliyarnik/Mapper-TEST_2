from __future__ import annotations

import argparse
import time

from mapper.exec.config_resolver import load_run_config
from mapper.exec.pipeline_runner import run_pipeline


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--cfg", default="configs/baseline.yaml")
    parser.add_argument("--run", default="baseline", help="Run directory name")
    parser.add_argument("--stages", nargs="*", default=None, help="Stage names or numbers; 0 means all")
    parser.add_argument("--retrain", nargs="*", default=[], help="Stage names or numbers; 0 means all")
    args = parser.parse_args()

    cfg = load_run_config(args.cfg)
    t0 = time.time()
    summary = run_pipeline(cfg, run_name=args.run, stages=args.stages, retrain=args.retrain)
    elapsed = time.time() - t0
    stage_summary = ", ".join(f"{item['stage_name']}={item['status']}" for item in summary["stages"])
    print(f"Finished Mapper Run: {stage_summary} in {elapsed:,.1f}s")


if __name__ == "__main__":
    main()
