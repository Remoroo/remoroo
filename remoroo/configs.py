import os

def get_api_url() -> str:
    """
    Get the Remoroo API URL from environment variables or use default.
    """
    return os.getenv("REMOROO_API_URL", "http://localhost:8000").rstrip("/")
