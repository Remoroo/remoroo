import os
import json
from pathlib import Path
from typing import Optional, Dict, Any
import typer
import webbrowser
import time

CRED_PATH = Path.home() / ".config" / "remoroo" / "credentials"

class AuthClient:
    def __init__(self, base_url: Optional[str] = None):
        self.base_url = base_url or os.environ.get("REMOROO_API_URL", "https://brain.remoroo.com")
        self.auth_url = os.environ.get("REMOROO_AUTH_URL", "https://console.brain.remoroo.com/console") # Point to console for key generation
        self._token: Optional[str] = None
        self._load_token()

    def _load_token(self):
        if CRED_PATH.exists():
            try:
                content = CRED_PATH.read_text().strip()
                # Simple format: just the token for now, or JSON later
                self._token = content
            except Exception:
                pass

    def is_authenticated(self) -> bool:
        return bool(self._token)
        
    def get_token(self) -> Optional[str]:
        return self._token

    def login(self) -> None:
        """Interactive login flow."""
        if self.is_authenticated():
            typer.echo("Already logged in.")
            return

        CRED_PATH.parent.mkdir(parents=True, exist_ok=True)
        
        typer.secho("Opening browser to sign in...", fg=typer.colors.BLUE)
        print(f"URL: {self.auth_url}")
        
        try:
            webbrowser.open(self.auth_url)
        except Exception:
            typer.echo("Could not open browser automatically.")
            
        token = typer.prompt("Paste your API key (will be saved locally)", hide_input=True)
        token = (token or "").strip()
        
        if not token:
            typer.secho("Login aborted.", fg=typer.colors.RED)
            raise typer.Exit(code=1)
            
        # Validate Token
        try:
            import requests # Make sure requests is imported or available
            typer.echo("Verifying credentials...")
            res = requests.get(f"{self.base_url}/user/me", headers={"Authorization": token})
            if res.status_code == 200:
                user = res.json()
                typer.secho(f"Successfully logged in as {user.get('email') or user.get('username')}", fg=typer.colors.GREEN)
                self._save_token(token)
            else:
                 typer.secho(f"Login failed: {res.status_code} - Invalid API Key", fg=typer.colors.RED)
                 raise typer.Exit(code=1)
        except Exception as e:
             typer.secho(f"Verification error: {e}", fg=typer.colors.YELLOW)
             # Proceed? No, fail secure.
             raise typer.Exit(code=1)

    def logout(self) -> None:
        if CRED_PATH.exists():
            CRED_PATH.unlink()
        self._token = None
        typer.echo("Logged out.")

    def _save_token(self, token: str) -> None:
        self._token = token
        tmp = CRED_PATH.with_suffix(".tmp")
        tmp.write_text(token + "\n", encoding="utf-8")
        tmp.replace(CRED_PATH)
        try:
            os.chmod(CRED_PATH, 0o600)
        except Exception:
            pass

    def create_run(self, repo_path: str, goal: str, mode: str) -> Dict[str, Any]:
        """
        Mock create run - in future this hits the Control Plane API.
        Returns a run configuration/ID.
        """
        # Mock response
        run_id = time.strftime("%Y%m%d-%H%M%S") + "-" + os.urandom(4).hex()
        return {
            "run_id": run_id,
            "status": "created",
            "config": {
                "goal": goal,
                "mode": mode
            }
        }

# Global instance for CLI convenience, but preferable to instantiate in commands
_client = AuthClient()

def ensure_logged_in() -> AuthClient:
    if not _client.is_authenticated():
        _client.login()
    return _client
