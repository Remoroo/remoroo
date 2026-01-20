"""
Runtime monitoring helper for Remoroo instrumentation.
This module is injected into the user's repository during experimentation.
It provides a safe, atomic way to emit metrics without race conditions.
"""
import os
import json
import uuid
import time
import sys
from typing import Any, Optional

class MetricEmitter:
    """
    Handles atomic emission of metrics to partial artifact files.
    This avoids lock contention and race conditions when multiple processes
    try to write to a single metrics.json file.
    """
    
    def __init__(self, artifact_dir: Optional[str] = None):
        """
        Initialize the emitter.
        
        Args:
            artifact_dir: Optional explicit path. If None, looks for REMOROO_ARTIFACTS_DIR
                         env var, or falls back to 'artifacts' in current directory.
        """
        self.artifact_dir = (
            artifact_dir 
            or os.environ.get("REMOROO_ARTIFACTS_DIR") 
            or os.path.join(os.getcwd(), "artifacts")
        )
        # Ensure it exists (safe mkdir)
        try:
            os.makedirs(self.artifact_dir, exist_ok=True)
        except Exception:
            pass
            
        self.pid = os.getpid()
        self.process_uuid = str(uuid.uuid4())[:8]

    def emit(self, name: str, value: Any, unit: str = "", source: str = "custom_instrumentation") -> bool:
        """
        Emit a single metric to a unique partial artifact file.
        
        Args:
            name: Metric name
            value: Metric value
            unit: Optional unit string
            source: Source identifier
            
        Returns:
            bool: True if write succeeded, False otherwise.
        """
        try:
            timestamp = time.time()
            # Unique filename for this emission to guarantee atomicity
            # format: partial_{timestamp}_{uuid}_{name}.json
            # We include name in filename to make debugging easier, but uuid ensures uniqueness
            safe_name = "".join(c for c in name if c.isalnum() or c in "._-")[:50]
            filename = f"partial_{timestamp:.6f}_{self.process_uuid}_{safe_name}.json"
            filepath = os.path.join(self.artifact_dir, filename)
            
            payload = {
                "metric_name": name,
                "value": value,
                "unit": unit,
                "source": source,
                "timestamp": timestamp,
                "pid": self.pid,
                "process_uuid": self.process_uuid,
                "version": "1.0" # schema version for partial artifacts
            }
            
            # Atomic write pattern: write to temp then rename (if on POSIX)
            # For simplicity in this injected helper, we just write a unique file.
            # Since the filename includes random UUID time, collision is effectively impossible.
            with open(filepath, "w", encoding="utf-8") as f:
                json.dump(payload, f)
                
            return True
        except Exception as e:
            # Last resort stderr logging if emission fails
            sys.stderr.write(f"[Remoroo] Failed to emit metric '{name}': {e}\n")
            return False

# Global instance for easy import usage
_global_emitter = None

def emit(name: str, value: Any, unit: str = "", source: str = "custom_instrumentation"):
    """
    Global convenience function.
    Usage:
        import monitor
        monitor.emit("accuracy", 0.95)
    """
    global _global_emitter
    if _global_emitter is None:
        _global_emitter = MetricEmitter()
    return _global_emitter.emit(name, value, unit, source)
