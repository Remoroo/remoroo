"""Step-executor package. Importing it registers the shipped kinds (supervised + base-to-
base); an authored cell can `register()` a custom kind. The engine consumes the registry,
never a hardcoded `if kind == ...`.
"""
from __future__ import annotations

from . import base_to_base, supervised  # noqa: F401 — import for the registration side effect
from .base import (
    StepContext,
    StepError,
    family_of,
    known_kinds,
    make_executor,
    register,
)

__all__ = ["StepContext", "StepError", "make_executor", "register",
           "known_kinds", "family_of"]
