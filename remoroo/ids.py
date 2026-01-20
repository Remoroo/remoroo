import uuid
import time
from datetime import datetime

def new_run_id() -> str:
    """Generate a new unique run ID."""
    # Using a timestamp + short UUID suffix for readability and uniqueness
    # Format: YYYYMMDD-HHMMSS-uuid
    now = datetime.now().strftime("%Y%m%d-%H%M%S")
    suffix = str(uuid.uuid4())[:8]
    return f"{now}-{suffix}"
