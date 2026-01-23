"""
Smoke tests for Remoroo CLI.

These tests verify basic functionality and that core modules load successfully.
Designed to be fast (<1s) with zero external dependencies.
"""
import sys
from pathlib import Path


def test_import_remoroo():
    """Test that the remoroo package can be imported."""
    import remoroo
    assert remoroo is not None


def test_import_auth():
    """Test that the auth module loads successfully."""
    from remoroo.auth import AuthClient
    assert AuthClient is not None


def test_auth_client_initialization():
    """Test that AuthClient can be instantiated with default settings."""
    from remoroo.auth import AuthClient
    
    # Should not raise any exceptions
    client = AuthClient(base_url="https://test.example.com")
    assert client is not None
    assert client.base_url == "https://test.example.com"


def test_import_configs():
    """Test that configs module loads successfully."""
    from remoroo.configs import get_api_url
    assert get_api_url is not None


def test_import_execution_modules():
    """Test that core execution modules can be imported."""
    from remoroo.execution import repo_manager
    assert repo_manager is not None
    
    from remoroo.execution.repo_manager import create_working_copy
    assert create_working_copy is not None


def test_python_version():
    """Verify Python version is compatible (3.8+)."""
    assert sys.version_info >= (3, 8), "Python 3.8+ is required"
