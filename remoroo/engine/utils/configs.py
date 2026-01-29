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
# ARTIFACT & PATH CONFIGURATION (v9 Universal)
# ============================================================================

ARTIFACTS_DIR_NAME: str = "artifacts"
METRICS_FILENAME: str = "metrics.json"

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
    ARTIFACTS_DIR_NAME, # Use constant
    "logs",
    ".remoroo"
}

# Default filenames to exclude from repository scanning
DEFAULT_EXCLUDED_FILES: set = {
    "remoroo_monitor.py",
    "remoroo_metrics.json",
    "baseline_metrics.json",
    "current_metrics.json",
    METRICS_FILENAME, # Use constant
}
