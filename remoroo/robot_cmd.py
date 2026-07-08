"""``remoroo robot`` — rig identity commands, and the activation gate the Studio rides on.

``remoroo robot activate`` is the once-per-lifetime rig activation (the pricing model's
enforcement surface). Recognition and re-binding are silent and free; only an explicit
"this is a NEW rig" confirmation creates a serial (and, from robot 2, the annual robot
license). Ambiguity is a question here, never a charge.

``ensure_rig_activated()`` is the same flow as a callable: setup and the Studio refuse to
open on an unactivated rig, but a rig the control plane recognizes (token or rolling
fingerprint — including every EXISTING rig that activated once before) passes silently.
"""
from __future__ import annotations

import sys
from pathlib import Path
from typing import Optional

import typer

robot_app = typer.Typer(no_args_is_help=True, help="Rig identity: activate, inspect.")


def _payload(repo_path: Path, name: str = "") -> dict:
    from . import rig_identity as rid

    fingerprint = rid.compute_fingerprint(repo_path)
    return {
        "rig_token": rid.rig_token(repo_path),
        "fingerprint": fingerprint,
        "host": fingerprint["components"]["host"][0],
        "name": name,
    }


def _post(base: str, token: str, payload: dict) -> dict:
    from . import rig_identity as rid

    return rid.post_activate(base, token, payload)


def ensure_rig_activated(repo_path: Path, client, base: str,
                         *, interactive: Optional[bool] = None) -> Optional[str]:
    """The Studio/setup gate: returns the rig serial, or None when the rig is not
    activated and could not be (non-interactive, declined, or unreachable-and-unknown).

    Order: saved serial (fast path) -> silent recognition (token / rolling fingerprint;
    this is how every EXISTING rig passes without any dialog) -> the human question."""
    from . import rig_identity as rid

    serial = rid.saved_serial(repo_path)
    if serial:
        return serial
    if interactive is None:
        interactive = sys.stdin.isatty()

    try:
        out = _post(base, client.get_token() or "", _payload(repo_path))
    except Exception as e:                              # noqa: BLE001 - stated
        typer.secho(f"rig check failed ({type(e).__name__}: {e}) — the control plane is "
                    "needed once to activate this rig.", fg=typer.colors.RED)
        return None

    if out.get("status") in ("recognized", "rebound"):
        rid.save_serial(repo_path, out["serial"])
        return out["serial"]

    # status == "question": a human decides; nothing auto-invoices.
    rigs = out.get("existing_rigs") or []
    if not interactive:
        typer.secho("This rig is not activated. Run `remoroo robot activate` once "
                    "(existing rigs re-bind for free).", fg=typer.colors.RED)
        return None
    typer.secho("This hardware isn't recognized yet.", fg=typer.colors.YELLOW)
    if rigs:
        typer.echo("Existing rigs on your account:")
        for i, r in enumerate(rigs, 1):
            typer.echo(f"  {i}. {r['serial']}  {r.get('name') or ''}")
        typer.echo("  n. this is a NEW rig")
        choice = typer.prompt("Which is this rig? (number, or n)", default="n").strip()
    else:
        choice = "n"
    if choice.lower() == "n":
        if not typer.confirm("Activate as a NEW rig? (first robot is free; from robot 2 "
                             "the annual license applies)", default=True):
            return None
        name = typer.prompt("Name this rig", default="cell A")
        out = _post(base, client.get_token() or "",
                    {**_payload(repo_path, name), "confirm_new": True})
    else:
        try:
            claim = rigs[int(choice) - 1]["serial"]
        except (ValueError, IndexError):
            typer.secho("No such entry.", fg=typer.colors.RED)
            return None
        out = _post(base, client.get_token() or "",
                    {**_payload(repo_path), "claim_serial": claim})
    if out.get("status") == "billing_required":
        _print_billing_required(out)
        return None
    if out.get("serial"):
        rid.save_serial(repo_path, out["serial"])
        if out.get("status") == "activated":
            annuity = out.get("annuity_usd") or 0.0
            note = ("your first robot — free" if not out.get("billable")
                    else f"annual robot license: ${annuity:,.0f}/yr")
            typer.secho(f"✓ NEW rig activated: {out['serial']} ({note}).",
                        fg=typer.colors.GREEN)
        else:
            typer.secho(f"✓ rig {out['serial']} re-bound to this hardware. Billed: $0.",
                        fg=typer.colors.GREEN)
        return out["serial"]
    typer.secho(f"activation did not complete: {out}", fg=typer.colors.RED)
    return None


def _print_billing_required(out: dict) -> None:
    typer.secho(out.get("message") or "Activation needs a payment method on file "
                "(your first robot is still free).", fg=typer.colors.YELLOW)
    url = out.get("setup_url")
    if url:
        typer.echo(f"Add one here, then rerun: {url}")


@robot_app.command("activate")
def activate(
    repo: Path = typer.Option(Path("."), "--repo"),
    name: str = typer.Option("", "--name", help="A human name for this rig (cell A…)."),
    claim: Optional[str] = typer.Option(None, "--claim",
                                        help="Claim an EXISTING serial after "
                                             "maintenance/refit (bills nothing)."),
    new: bool = typer.Option(False, "--new",
                             help="Confirm this is a NEW rig (the only billable path)."),
    brain_url: Optional[str] = typer.Option(None, "--brain-url"),
):
    """Activate (or re-recognize) THIS rig with your Remoroo account."""
    from .auth import ensure_logged_in
    from .paths import resolve_repo_path
    from . import rig_identity as rid

    repo_path = resolve_repo_path(repo)
    client = ensure_logged_in()
    base = brain_url or client.base_url

    payload = _payload(repo_path, name)
    if claim:
        payload["claim_serial"] = claim
    if new:
        payload["confirm_new"] = True

    try:
        out = _post(base, client.get_token() or "", payload)
    except Exception as e:                              # noqa: BLE001 - stated
        typer.secho(f"activation failed: {type(e).__name__}: {e}", fg=typer.colors.RED)
        raise typer.Exit(code=2)

    status = out.get("status")
    if status == "billing_required":
        _print_billing_required(out)
        raise typer.Exit(code=4)
    if status == "recognized":
        rid.save_serial(repo_path, out["serial"])
        typer.secho(f"✓ rig recognized: {out['serial']} (leg: {out.get('leg')}). "
                    "Nothing to do, nothing billed.", fg=typer.colors.GREEN)
        return
    if status in ("rebound", "activated"):
        rid.save_serial(repo_path, out["serial"])
        if status == "activated":
            annuity = out.get("annuity_usd") or 0.0
            note = ("your first robot — free" if not out.get("billable")
                    else f"annual robot license: ${annuity:,.0f}/yr")
            typer.secho(f"✓ NEW rig activated: {out['serial']} ({note}).",
                        fg=typer.colors.GREEN)
        else:
            typer.secho(f"✓ rig {out['serial']} re-bound to this hardware. Billed: $0.",
                        fg=typer.colors.GREEN)
        return

    # status == "question": fall into the same interactive flow the Studio gate uses.
    serial = ensure_rig_activated(repo_path, client, base, interactive=True)
    raise typer.Exit(code=0 if serial else 3)


@robot_app.command("status")
def status(
    repo: Path = typer.Option(Path("."), "--repo"),
    brain_url: Optional[str] = typer.Option(None, "--brain-url"),
):
    """Show this rig's serial and your account's rigs."""
    import requests

    from .auth import ensure_logged_in
    from .paths import resolve_repo_path
    from . import rig_identity as rid

    repo_path = resolve_repo_path(repo)
    client = ensure_logged_in()
    base = brain_url or client.base_url
    typer.echo(f"this rig: {rid.saved_serial(repo_path) or 'not activated'}")
    try:
        r = requests.get(f"{base.rstrip('/')}/rigs",
                         headers={"Authorization": f"Bearer {client.get_token() or ''}"},
                         timeout=15)
        r.raise_for_status()
        for rig in r.json().get("rigs", []):
            typer.echo(f"  {rig['serial']}  {rig.get('name') or ''}  {rig['status']}"
                       + ("" if rig.get("billable") else "  (first robot: free)"))
    except Exception as e:                              # noqa: BLE001 - stated
        typer.secho(f"(account rigs unavailable: {type(e).__name__}: {e})",
                    fg=typer.colors.YELLOW)
