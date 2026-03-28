from __future__ import annotations

import argparse

from .train_export import run_embedding_training


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--data-dir", required=False)
    parser.add_argument("--out-root", required=False)
    parser.parse_args()
    raise NotImplementedError("Stage 3 CLI is developer-facing only in the first scaffold")


if __name__ == "__main__":
    main()
