from __future__ import annotations


class WorkflowError(RuntimeError):
    """Raised when a workflow definition or runtime state is invalid."""


class DirorchServiceError(RuntimeError):
    """Raised when a service operation cannot be completed."""


class ValidationError(DirorchServiceError):
    """Raised when user input is syntactically or semantically invalid."""


class NotFoundError(DirorchServiceError):
    """Raised when a requested resource does not exist."""


class ConflictError(DirorchServiceError):
    """Raised when an operation conflicts with current state."""


class ForbiddenError(DirorchServiceError):
    """Raised when access to a resource is not allowed."""
