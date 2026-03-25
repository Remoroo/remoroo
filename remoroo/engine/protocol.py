# CLI/worker must not depend on remoroo_brain (often a different host / package set).
from remoroo_core.protocol import ExecutionRequest, ExecutionResult, SCHEMA_VERSION

__all__ = ["ExecutionRequest", "ExecutionResult", "SCHEMA_VERSION"]
