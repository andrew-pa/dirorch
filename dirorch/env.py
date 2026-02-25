from __future__ import annotations

import os
import re
from pathlib import Path

from .models import WorkflowConfig


def build_hook_env(config: WorkflowConfig, root: Path) -> dict[str, str]:
    """Build process + workflow environment visible to all hooks."""

    env = dict(os.environ)
    env.update(config.environment)
    for phase in config.phases:
        for state in phase.states:
            key = f"DIR_{_sanitize_token(phase.name)}_{_sanitize_token(state)}"
            env[key] = str((root / phase.name / state).resolve())
    return env


def _sanitize_token(raw: str) -> str:
    upper = raw.upper()
    return re.sub(r"[^A-Z0-9]", "_", upper)
