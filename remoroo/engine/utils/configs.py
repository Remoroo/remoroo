"""
Execution layer configuration values.

Contains file system, path exclusion, venv, and instrumentation pipeline configs.
"""
from typing import Dict, Any

# ============================================================================
# VENV CONFIGURATION
# ============================================================================

# Always create venv even if no requirements files are detected
DEFAULT_ALWAYS_CREATE_VENV: bool = True

# ============================================================================
# REPOSITORY EXCLUSIONS CONFIGURATION
# ============================================================================

# Default paths to deny in repository operations
DEFAULT_DENY_PATHS: list = [
    ".git/",
    ".env",
    "secrets/",
    ".remoroo_venvs/",
    "remoroo_venvs/",
]

# Default directory names to exclude from repository scanning
DEFAULT_EXCLUDED_DIRS: set = {
    # Python
    "__pycache__",
    "venv",
    ".venv",
    "env",
    "remoroo_venvs",
    ".remoroo_venvs",
    ".pytest_cache",
    ".mypy_cache",
    ".tox",
    ".hypothesis",
    # Node.js
    "node_modules",
    ".npm",
    ".yarn",
    # Build artifacts
    "build",
    "dist",
    "target",
    "out",
    "bin",
    "obj",
    # IDE/Editor
    ".idea",
    ".vscode",
    ".vs",
    ".settings",
    # Coverage/Testing
    ".coverage",
    ".nyc_output",
    "coverage",
    # OS
    ".DS_Store",
    "Thumbs.db",
    # Other
    ".cache",
    ".tmp",
    "tmp",
    "temp",
    ".temp",
    # Remoroo System
    "runs",
    "artifacts",
    "logs",
    ".remoroo"
}
