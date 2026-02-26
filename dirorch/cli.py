from __future__ import annotations

import argparse
import logging
from pathlib import Path

from .models import CliOptions


def parse_args() -> CliOptions:
    parser = argparse.ArgumentParser(
        description="Run directory-based workflow orchestration"
    )
    parser.add_argument(
        "workflow",
        type=Path,
        help="Workflow path, or name resolved from $XDG_CONFIG_DIR/dirorch/workflows/<name>.yml (fallback: ~/.config/dirorch/workflows/<name>.yml)",
    )
    parser.add_argument(
        "--root",
        type=Path,
        default=Path.cwd(),
        help="Root directory for workflow state directories (default: current directory)",
    )
    parser.add_argument(
        "--retries",
        type=int,
        default=None,
        help="Retries for hooks (overrides YAML retries; retries count excludes first attempt)",
    )
    parser.add_argument(
        "--state-file",
        default=".dirorch_runtime.json",
        help="Runtime state file name under --root",
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=("DEBUG", "INFO", "WARNING", "ERROR"),
        help="Logging verbosity",
    )
    args = parser.parse_args()
    if args.retries is not None and args.retries < 0:
        raise SystemExit("--retries must be 0 or greater")
    return CliOptions(
        workflow=args.workflow,
        root=args.root,
        retries_override=args.retries,
        state_file=args.state_file,
        log_level=args.log_level,
    )


def configure_logging(level: str) -> logging.Logger:
    logging.basicConfig(
        level=getattr(logging, level),
        format="%(asctime)s %(levelname)s %(message)s",
    )
    return logging.getLogger("dirorch")
