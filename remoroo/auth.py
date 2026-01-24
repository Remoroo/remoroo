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
            # Check if current token works for the target base_url
            try:
                import requests
                res = requests.get(
                    f"{self.base_url}/user/me", 
                    headers={"Authorization": f"Bearer {self._token}"},
                    timeout=3.0
                )
                if res.status_code == 200:
                    user = res.json()
                    typer.secho(f"✓ You are already logged in as {user.get('email') or user.get('username')}.", fg=typer.colors.GREEN)
                    typer.echo(f"  Target: {self.base_url}")
                    typer.echo("  To switch accounts or update your API key, run: remoroo logout && remoroo login")
                    return
                else:
                     typer.secho(f"⚠️  Stored token is invalid for {self.base_url} (HTTP {res.status_code})", fg=typer.colors.YELLOW)
                     typer.echo("  Proceeding to re-authenticate...\n")
            except Exception:
                 # If we can't reach the server, better to proceed with login flow
                 pass

        CRED_PATH.parent.mkdir(parents=True, exist_ok=True)
        
        typer.secho("\n🔐 Remoroo Authentication", fg=typer.colors.BLUE, bold=True)
        typer.echo("─" * 40)
        typer.echo("\n1. Opening the Remoroo Console in your browser...")
        typer.echo("2. Click 'Generate' to create a new API key")
        typer.secho("   ⚠️  Note: Generating a new key revokes any previous keys!", fg=typer.colors.YELLOW)
        typer.echo("3. Copy the key and paste it below\n")
        
        print(f"Console URL: {self.auth_url}")
        
        try:
            webbrowser.open(self.auth_url)
        except Exception:
            typer.echo("Could not open browser automatically. Please visit the URL above.")
            
        token = typer.prompt("Paste your API key here (input is hidden)", hide_input=True)
        token = (token or "").strip()
        
        if not token:
            typer.secho("Login aborted.", fg=typer.colors.RED)
            raise typer.Exit(code=1)
            
        # Validate Token
        try:
            import requests
            typer.echo("Verifying credentials...")
            res = requests.get(f"{self.base_url}/user/me", headers={"Authorization": f"Bearer {token}"})
            if res.status_code == 200:
                user = res.json()
                self._save_token(token)
                typer.echo()
                typer.secho("✅ Successfully authenticated!", fg=typer.colors.GREEN, bold=True)
                typer.echo(f"   Logged in as: {user.get('email') or user.get('username')}")
                typer.echo(f"   Credentials saved to: {CRED_PATH}")
                typer.echo()
            else:
                 typer.secho(f"❌ Login failed: Invalid API Key (HTTP {res.status_code})", fg=typer.colors.RED)
                 typer.echo("   Make sure you copied the complete key from the Console.")
                 raise typer.Exit(code=1)
        except requests.exceptions.ConnectionError:
             typer.secho("❌ Connection error: Could not reach the Remoroo API.", fg=typer.colors.RED)
             typer.echo(f"   Tried: {self.base_url}")
             raise typer.Exit(code=1)
        except Exception as e:
             typer.secho(f"❌ Verification error: {e}", fg=typer.colors.RED)
             raise typer.Exit(code=1)

    def logout(self) -> None:
        if CRED_PATH.exists():
            CRED_PATH.unlink()
        self._token = None
        typer.echo("Logged out.")

    def whoami(self) -> None:
        """Display current authentication status and user information."""
        if not self.is_authenticated():
            typer.secho("❌ Not logged in", fg=typer.colors.RED)
            typer.echo("   Run 'remoroo login' to authenticate")
            raise typer.Exit(code=1)
        try:
            import requests
            typer.echo(f"🔍 Checking status at {self.base_url}...")
            res = requests.get(
                f"{self.base_url}/user/me", 
                headers={"Authorization": f"Bearer {self._token}"},
                timeout=5.0
            )
            
            if res.status_code == 200:
                user = res.json()
                typer.echo()
                typer.secho("✅ Authenticated", fg=typer.colors.GREEN, bold=True)
                typer.echo(f"   Email: {user.get('email') or 'N/A'}")
                typer.echo(f"   Username: {user.get('username') or 'N/A'}")
                typer.echo(f"   API Endpoint: {self.base_url}")
                typer.echo(f"   Credentials: {CRED_PATH}")
                typer.echo()
            else:
                typer.secho(f"❌ Authentication failed (HTTP {res.status_code})", fg=typer.colors.RED)
                typer.echo(f"   Target Endpoint: {self.base_url}")
                typer.echo("   Your API key may have expired, been revoked, or belongs to a different environment.")
                typer.echo("   Run 'remoroo logout && remoroo login' to re-authenticate.")
                raise typer.Exit(code=1)
                
        except requests.exceptions.ConnectionError:
            typer.secho("❌ Connection error: Could not reach the Remoroo API", fg=typer.colors.RED)
            typer.echo(f"   Endpoint: {self.base_url}")
            raise typer.Exit(code=1)
        except Exception as e:
            typer.secho(f"❌ Error: {e}", fg=typer.colors.RED)
            raise typer.Exit(code=1)


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
