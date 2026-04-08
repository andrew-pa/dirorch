from __future__ import annotations

import json
from pathlib import Path

from .errors import WorkflowError


class EntityLockStore:
    """Persists entity lock state independent of runtime execution snapshot."""

    def __init__(self, root: Path, file_name: str) -> None:
        self._path = root / file_name
        self._locks: set[str] | None = None

    def is_locked(self, entity_id: str) -> bool:
        return entity_id in self._load()

    def list_locks(self) -> set[str]:
        return set(self._load())

    def set_locked(self, entity_id: str, locked: bool) -> bool:
        locks = self._load()
        changed = False
        if locked:
            if entity_id not in locks:
                locks.add(entity_id)
                changed = True
        elif entity_id in locks:
            locks.remove(entity_id)
            changed = True
        if changed:
            self._save(locks)
        return locked

    def clear(self, entity_id: str) -> None:
        self.set_locked(entity_id, False)

    def _load(self) -> set[str]:
        if self._locks is not None:
            return self._locks

        if not self._path.exists():
            self._locks = set()
            return self._locks

        try:
            payload = json.loads(self._path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            raise WorkflowError(
                f"Unable to read lock file {self._path}: {exc}"
            ) from exc

        if not isinstance(payload, list) or not all(
            isinstance(item, str) and item for item in payload
        ):
            raise WorkflowError(
                f"Invalid lock file {self._path}: expected list of non-empty strings"
            )

        self._locks = set(payload)
        return self._locks

    def _save(self, locks: set[str]) -> None:
        self._path.write_text(
            json.dumps(sorted(locks), indent=2),
            encoding="utf-8",
        )
