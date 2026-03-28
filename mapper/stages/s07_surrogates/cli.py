from __future__ import annotations

import argparse


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", required=False)
    parser.parse_args()
    raise NotImplementedError("Stage 7 CLI is developer-facing only in the first scaffold")


if __name__ == "__main__":
    main()
