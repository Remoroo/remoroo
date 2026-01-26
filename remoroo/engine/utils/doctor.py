import json
import os
import sys
import shutil
import subprocess
import typer
from importlib import metadata
from packaging import version
import requests

PACKAGE_NAME = "remoroo"

def get_current_version():
    """Dynamically detect the installed version of the package."""
    try:
        return metadata.version(PACKAGE_NAME)
    except metadata.PackageNotFoundError:
        # Fallback for development environments where the package isn't 'installed'
        return "0.0.0-dev"

def check_cli_update():
    """Check PyPI for a newer version of the CLI."""
    current_v = get_current_version()
    if current_v == "0.0.0-dev":
        return

    try:
        # We use a short timeout to not block the CLI start too long
        response = requests.get(f"https://pypi.org/pypi/{PACKAGE_NAME}/json", timeout=1.5)
        if response.status_code == 200:
            data = response.json()
            latest_version = data["info"]["version"]
            
            if version.parse(latest_version) > version.parse(current_v):
                typer.secho(f"\n🚀 A newer version of Remoroo is available: {latest_version} (you have {current_v})", fg=typer.colors.BRIGHT_YELLOW)
                
                confirm = typer.confirm("Would you like to upgrade now?", default=True)
                if confirm:
                    typer.secho(f"📦 Upgrading {PACKAGE_NAME}...", fg=typer.colors.CYAN)
                    subprocess.run([sys.executable, "-m", "pip", "install", "--upgrade", PACKAGE_NAME], check=True)
                    typer.secho("✅ Upgrade complete! Please restart your command.", fg=typer.colors.GREEN)
                    raise typer.Exit(code=0)
    except Exception:
        # Fail silently if offline or PyPI is down to not block the user
        pass

def check_git() -> bool:
    """Check if git is installed and in PATH."""
    try:
        subprocess.run(["git", "--version"], capture_output=True, check=True)
        return True
    except (subprocess.SubprocessError, FileNotFoundError):
        return False

def install_git():
    """Attempt to install git based on the platform."""
    platform = sys.platform
    
    if platform == "win32":
        # Windows: Check for winget
        try:
            subprocess.run(["winget", "--version"], capture_output=True, check=True)
            typer.secho("📦 Found winget. Attempting to install Git...", fg=typer.colors.CYAN)
            subprocess.run([
                "winget", "install", "--id", "Git.Git", "-e", 
                "--source", "winget", 
                "--accept-package-agreements", 
                "--accept-source-agreements"
            ], check=True)
            typer.secho("✅ Git installed successfully via winget.", fg=typer.colors.GREEN)
            return True
        except (subprocess.SubprocessError, FileNotFoundError):
            typer.secho("❌ winget not found. Please download Git from https://git-scm.com/download/win", fg=typer.colors.RED)
            return False
            
    elif platform == "darwin":
        # macOS: Check for brew
        try:
            subprocess.run(["brew", "--version"], capture_output=True, check=True)
            typer.secho("📦 Found Homebrew. Attempting to install Git...", fg=typer.colors.CYAN)
            subprocess.run(["brew", "install", "git"], check=True)
            return True
        except (subprocess.SubprocessError, FileNotFoundError):
            typer.secho("❌ Homebrew not found. Please install Git via Xcode Command Line Tools or https://git-scm.com/download/mac", fg=typer.colors.RED)
            return False
            
    else:
        # Linux (simplistic)
        if shutil.which("apt-get"):
            typer.secho("📦 Attempting to install Git via apt-get...", fg=typer.colors.CYAN)
            try:
                subprocess.run(["sudo", "apt-get", "update"], check=True)
                subprocess.run(["sudo", "apt-get", "install", "-y", "git"], check=True)
                return True
            except: pass
        
        typer.secho("❌ Automatic installation failed. Please install Git manually using your package manager.", fg=typer.colors.RED)
        return False

def ensure_git():
    """Ensure git is present, prompt to install if not."""
    if check_git():
        return True
        
    typer.secho("\n⚠️  Git is missing! Remoroo needs Git to manage repository copies and generate patches.", fg=typer.colors.YELLOW)
    
    confirm = typer.confirm("🚀 Would you like me to try and install Git for you?", default=True)
    if confirm:
        success = install_git()
        if success:
            # Check again to be sure it's in PATH now
            if check_git():
                return True
            else:
                typer.secho("ℹ️  Git was installed but might not be in your current shell's PATH yet. Please restart your terminal.", fg=typer.colors.CYAN)
                return False
    
    typer.secho("🛑 Git is required to continue. Please install it and try again.", fg=typer.colors.RED)
    raise typer.Exit(code=1)

def ensure_ready():
    """Run all pre-flight checks (updates, dependencies)."""
    # 1. Check for CLI updates (non-blocking)
    check_cli_update()
    
    # 2. Ensure Git is present
    ensure_git()
