from __future__ import annotations

import asyncio

from dirorch import CliOptions, FAILED_STATE, WorkflowError, load_workflow, run
from dirorch.cli import parse_args


def main() -> None:
    options = parse_args()
    try:
        asyncio.run(run(options))
    except WorkflowError as exc:
        raise SystemExit(str(exc)) from exc


__all__ = [
    "CliOptions",
    "FAILED_STATE",
    "WorkflowError",
    "load_workflow",
    "main",
    "run",
]


if __name__ == "__main__":
    main()
