from __future__ import annotations

import os
import re
from pathlib import Path

from .errors import WorkflowError
from .models import WorkflowConfig
from .template_engine import TemplateRenderError, TemplateRenderer


def build_hook_env(config: WorkflowConfig, root: Path) -> dict[str, str]:
    """Build process + workflow environment visible to all hooks."""
    return build_hook_env_from_defined(build_defined_hook_env(config, root))


def build_hook_env_from_defined(defined_env: dict[str, str]) -> dict[str, str]:
    """Merge inherited process env with Dirorch-defined environment."""
    env = dict(os.environ)
    env.update(defined_env)
    return env


def build_defined_hook_env(config: WorkflowConfig, root: Path) -> dict[str, str]:
    """Build only Dirorch-defined hook environment variables."""
    dir_env: dict[str, str] = {}
    for phase in config.phases:
        for state in phase.states:
            key = f"DIR_{_sanitize_token(phase.name)}_{_sanitize_token(state)}"
            dir_env[key] = str((root / phase.name / state).resolve())

    rendered_env = _render_workflow_env(config.environment, dir_env, root)
    return rendered_env | dir_env


def _render_workflow_env(
    raw_env: dict[str, str], dir_env: dict[str, str], root: Path
) -> dict[str, str]:
    renderer = TemplateRenderer(root)
    rendered: dict[str, str] = {}
    remaining = dict(raw_env)
    errors: dict[str, str] = {}

    while remaining:
        progressed = False
        for key in list(remaining):
            template = remaining[key]
            context = dir_env | rendered
            try:
                rendered[key] = renderer.render(template, context)
            except TemplateRenderError as exc:
                errors[key] = str(exc)
                continue
            del remaining[key]
            progressed = True
        if progressed:
            continue

        first_key = next(iter(remaining))
        raise WorkflowError(
            f"Environment variable '{first_key}' template failed: {errors[first_key]}"
        )

    return rendered


def _sanitize_token(raw: str) -> str:
    upper = raw.upper()
    return re.sub(r"[^A-Z0-9]", "_", upper)
