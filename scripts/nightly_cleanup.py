"""Scheduled retention cleanup runner for DataForge."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
BACKEND_DIR = ROOT / "backend"
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from app.services.cleanup import run_cleanup  # noqa: E402


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run DataForge retention cleanup.")
    parser.add_argument("--keep-last-runs", type=int, default=20, help="Number of most recent runs to keep")
    parser.add_argument("--keep-raw-files", type=int, default=200, help="Number of newest raw files to keep")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    payload = run_cleanup(keep_last_runs=args.keep_last_runs, keep_raw_files=args.keep_raw_files)
    print(json.dumps(payload, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
