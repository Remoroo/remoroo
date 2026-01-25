"""
Execution layer configuration values.

Contains file system, path exclusion, venv, and instrumentation pipeline configs.
"""
from typing import Dict, Any

# ============================================================================
# VENV CONFIGURATION
# ============================================================================

# Always create venv even if no requirements files are detected
# If False: Only create venv when requirements files are found (current behavior)
# If True: Always create venv for isolation, even without requirements files
DEFAULT_ALWAYS_CREATE_VENV: bool = True

# ============================================================================
# INSTRUMENTATION / VALIDATION PIPELINE CONFIGURATION
# ============================================================================

# Enable the one-time instrumentation pipeline (post-EnvDoctor, pre-turn0).
DEFAULT_ENABLE_INSTRUMENTATION_PIPELINE: bool = True

# Enable Validator-LLM generation/execution of validation scripts.
# When durable instrumentation is enabled, this should be False (artifacts come from normal runs).
DEFAULT_ENABLE_VALIDATION_SCRIPTS: bool = True

# Optional heuristic metric-source suggestions (suggest-only). Default OFF.
DEFAULT_ENABLE_METRIC_SOURCE_HEURISTICS: bool = False

# Env var used to distinguish baseline vs current metrics emission.
DEFAULT_METRICS_PHASE_ENV_VAR: str = "REMOROO_METRICS_PHASE"

# ============================================================================
# BASELINE EXECUTION CONFIGURATION
# ============================================================================

# Enable baseline execution before turn 0
DEFAULT_ENABLE_BASELINE_EXECUTION: bool = True

# Timeout per command during baseline execution (seconds) 
DEFAULT_BASELINE_TIMEOUT_PER_COMMAND: float = 9999.0

# Maximum number of commands to run during baseline
DEFAULT_BASELINE_MAX_COMMANDS: int = 100

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
# These are directory names (not paths) that should be skipped during os.walk
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
    ".temp"
}

# Default filenames to exclude from repository scanning
DEFAULT_EXCLUDED_FILES: set = {
    "remoroo_monitor.py",
    "remoroo_metrics.json",
    "baseline_metrics.json",
    "current_metrics.json",
    "metrics.json",
}
