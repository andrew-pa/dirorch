from __future__ import annotations


class WorkflowError(RuntimeError):
    """Raised when a workflow definition or runtime state is invalid."""
