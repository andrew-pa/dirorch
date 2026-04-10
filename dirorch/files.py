from __future__ import annotations

import json
from pathlib import Path, PurePosixPath

from .constants import ENTITY_LOGS_DIR_NAME
from .errors import ConflictError, ForbiddenError, NotFoundError, ValidationError
from .models import WorkflowConfig


class FileStore:
    """Provides safe root-scoped file CRUD for non-entity workflow files."""

    def __init__(
        self,
        root: Path,
        config: WorkflowConfig,
        state_file_name: str,
        locks_file_name: str,
    ) -> None:
        self._root = root.resolve()
        self._reserved_top_level = {phase.name for phase in config.phases}
        self._reserved_top_level.add(ENTITY_LOGS_DIR_NAME)
        self._reserved_files = {state_file_name, locks_file_name}

    def read(self, relative_path: str) -> tuple[Path, str]:
        path = self._resolve_path(relative_path)
        if not path.exists() or not path.is_file():
            raise NotFoundError(f"File '{relative_path}' was not found")
        try:
            return path, path.read_text(encoding="utf-8")
        except OSError as exc:
            raise ValidationError(f"Unable to read file '{relative_path}': {exc}") from exc

    def create(self, relative_path: str, content: str, format_name: str) -> tuple[Path, str]:
        path = self._resolve_path(relative_path)
        if path.exists():
            raise ConflictError(f"File '{relative_path}' already exists")
        self._validate_content(format_name, content, relative_path)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(content, encoding="utf-8")
        return path, content

    def update(self, relative_path: str, content: str, format_name: str) -> tuple[Path, str]:
        path = self._resolve_path(relative_path)
        if not path.exists() or not path.is_file():
            raise NotFoundError(f"File '{relative_path}' was not found")
        self._validate_content(format_name, content, relative_path)
        path.write_text(content, encoding="utf-8")
        return path, content

    def delete(self, relative_path: str) -> None:
        path = self._resolve_path(relative_path)
        if not path.exists() or not path.is_file():
            raise NotFoundError(f"File '{relative_path}' was not found")
        path.unlink()

    def _resolve_path(self, relative_path: str) -> Path:
        if not relative_path:
            raise ValidationError("Path must not be empty")

        raw_path = PurePosixPath(relative_path)
        if raw_path.is_absolute() or ".." in raw_path.parts:
            raise ForbiddenError(f"Path '{relative_path}' is not allowed")
        if raw_path.parts[0] in self._reserved_top_level or relative_path in self._reserved_files:
            raise ForbiddenError(f"Path '{relative_path}' is reserved for workflow internals")

        path = (self._root / Path(*raw_path.parts)).resolve()
        if self._root not in path.parents and path != self._root:
            raise ForbiddenError(f"Path '{relative_path}' escapes the workflow root")
        return path

    def _validate_content(self, format_name: str, content: str, relative_path: str) -> None:
        if format_name not in {"text", "json"}:
            raise ValidationError("format must be 'text' or 'json'")
        if format_name == "json":
            try:
                json.loads(content)
            except json.JSONDecodeError as exc:
                raise ValidationError(
                    f"Invalid JSON for '{relative_path}': {exc.msg}"
                ) from exc
