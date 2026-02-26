from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

from jinja2 import StrictUndefined, TemplateError
from jinja2.sandbox import SandboxedEnvironment


class TemplateRenderError(RuntimeError):
    """Raised when rendering stdin templates fails."""


@dataclass
class TemplateRenderer:
    root: Path

    def __post_init__(self) -> None:
        self._engine = SandboxedEnvironment(
            autoescape=False,
            keep_trailing_newline=True,
            undefined=StrictUndefined,
        )

    def render(self, template: str, env_vars: dict[str, str]) -> str:
        context = {
            **env_vars,
            "env": env_vars,
            "read_file": self._read_file,
            "include_file": self._read_file,
        }
        try:
            compiled = self._engine.from_string(template)
            return compiled.render(context)
        except TemplateError as exc:
            raise TemplateRenderError(str(exc)) from exc

    def _read_file(self, raw_path: Any) -> str:
        if not isinstance(raw_path, str) or not raw_path:
            raise TemplateRenderError("read_file/include_file path must be a non-empty string")

        path = Path(raw_path).expanduser()
        if not path.is_absolute():
            path = self.root / path
        try:
            return path.read_text(encoding="utf-8")
        except OSError as exc:
            raise TemplateRenderError(f"unable to read file '{path}': {exc}") from exc
