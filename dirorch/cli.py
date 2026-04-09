from __future__ import annotations

import argparse
import logging
from pathlib import Path

from .constants import DEFAULT_WEB_HOST, DEFAULT_WEB_PORT
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
    parser.add_argument(
        "--watch",
        action="store_true",
        help="Keep waiting for entity layout changes and rerun the workflow when they happen",
    )
    parser.add_argument(
        "--web",
        action="store_true",
        help="Enable the HTTP API server alongside workflow execution",
    )
    parser.add_argument(
        "--web-log",
        action="store_true",
        help="Enable HTTP access logging for the API server",
    )
    parser.add_argument(
        "--web-host",
        default=DEFAULT_WEB_HOST,
        help=f"Host interface for the HTTP API server (default: {DEFAULT_WEB_HOST})",
    )
    parser.add_argument(
        "--web-port",
        type=int,
        default=DEFAULT_WEB_PORT,
        help=f"Port for the HTTP API server (default: {DEFAULT_WEB_PORT})",
    )
    args = parser.parse_args()
    if args.retries is not None and args.retries < 0:
        raise SystemExit("--retries must be 0 or greater")
    if args.web_port < 1 or args.web_port > 65535:
        raise SystemExit("--web-port must be between 1 and 65535")
    return CliOptions(
        workflow=args.workflow,
        root=args.root,
        retries_override=args.retries,
        state_file=args.state_file,
        log_level=args.log_level,
        watch=args.watch,
        web=args.web,
        web_log=args.web_log,
        web_host=args.web_host,
        web_port=args.web_port,
    )


def configure_logging(level: str) -> logging.Logger:
    logging.basicConfig(
        level=getattr(logging, level),
        format="%(asctime)s %(levelname)s %(message)s",
    )
    return logging.getLogger("dirorch")
