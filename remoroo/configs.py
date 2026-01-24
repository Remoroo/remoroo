import os

def get_api_url() -> str:
    """
    Get the Remoroo API URL from environment variables or use default.
    """
    return os.getenv("REMOROO_API_URL", "https://brain.remoroo.com").rstrip("/")

def get_default_engine() -> str:
    """
    Get the default execution engine (docker or venv).
    """
    return os.getenv("REMOROO_DEFAULT_ENGINE", "docker").lower()
