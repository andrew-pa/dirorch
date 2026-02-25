from __future__ import annotations

import json
from pathlib import Path

from .errors import WorkflowError


class RuntimeStateStore:
    """Persists the currently active phase so runs can resume."""

    def __init__(self, root: Path, state_file_name: str) -> None:
        self._state_path = root / state_file_name

    def load_current_phase(self) -> str | None:
        if not self._state_path.exists():
            return None
        try:
            payload = json.loads(self._state_path.read_text(encoding="utf-8"))
        except (json.JSONDecodeError, OSError) as exc:
            raise WorkflowError(
                f"Unable to read state file {self._state_path}: {exc}"
            ) from exc

        current = payload.get("current_phase")
        if current is None:
            return None
        if not isinstance(current, str):
            raise WorkflowError(
                f"Invalid state file {self._state_path}: 'current_phase' must be a string"
            )
        return current

    def save_current_phase(self, current_phase: str) -> None:
        payload = {"current_phase": current_phase}
        self._state_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
