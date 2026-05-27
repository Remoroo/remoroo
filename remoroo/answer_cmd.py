"""`remoroo answer <run_id> <text>` — submit a reply to an in-flight ask_human.

Companion to the agent-side ``ask_human`` tool. When a run is interactive
and the agent has called ``ask_human``, the brain blocks waiting on the
Redis key ``runs:{run_id}:answer``. This command:

  1. Verifies the run exists and is awaiting input (``GET /runs/{id}/awaiting``).
  2. POSTs the answer to ``/runs/{id}/answer``.
  3. Prints the question that was asked (so the operator sees what they're
     answering) and confirms the post.

By default the answer text is taken from a positional argument. Pass
``--stdin`` to read the answer from stdin (useful for multi-line replies
or scripted use). If neither is provided, the operator is prompted
interactively.

Errors are mapped to non-zero exit codes:

  * 1 : run not found / unauthorized / network error
  * 2 : run is not awaiting input (no question in flight)
  * 3 : answer was empty and no default exists
"""
from __future__ import annotations

import sys
from typing import Optional

import typer


def _resolve_brain_url(brain_url: Optional[str]) -> str:
    if brain_url:
        return brain_url
    from .configs import get_api_url

    return get_api_url()


def _auth_headers() -> dict:
    import os

    token = os.getenv("REMOROO_API_KEY")
    if not token:
        try:
            from .auth import _client

            if _client.is_authenticated():
                token = _client.get_token()
        except Exception:
            token = None
    if not token:
        raise typer.Exit(code=1)
    return {"Authorization": f"Bearer {token}"}


def answer(
    run_id: str = typer.Argument(..., metavar="RUN_ID", help="Run id to answer."),
    text: Optional[str] = typer.Argument(
        None,
        metavar="TEXT",
        help="Answer text. Omit to read from stdin or be prompted interactively.",
    ),
    use_stdin: bool = typer.Option(
        False, "--stdin", help="Read answer text from stdin (for multi-line answers)."
    ),
    brain_url: Optional[str] = typer.Option(
        None, "--brain-url", help="Override Brain API base URL."
    ),
) -> None:
    """Submit an answer to an in-flight ask_human prompt on a running session."""
    import requests

    api_url = _resolve_brain_url(brain_url).rstrip("/")
    headers = _auth_headers()

    # 1. Probe whether the run is awaiting input (also surfaces the question).
    try:
        resp = requests.get(
            f"{api_url}/runs/{run_id}/awaiting", headers=headers, timeout=10.0
        )
    except Exception as e:
        typer.secho(f"❌ Could not reach {api_url}: {e}", fg=typer.colors.RED)
        raise typer.Exit(code=1)

    if resp.status_code == 404:
        typer.secho(f"❌ Run not found: {run_id}", fg=typer.colors.RED)
        raise typer.Exit(code=1)
    if resp.status_code in (401, 403):
        typer.secho("❌ Authentication failed.", fg=typer.colors.RED)
        raise typer.Exit(code=1)
    if resp.status_code == 204:
        typer.secho(
            f"⚠  Run {run_id} is not awaiting input. No question in flight.",
            fg=typer.colors.YELLOW,
        )
        raise typer.Exit(code=2)

    try:
        info = resp.json() or {}
    except Exception:
        info = {}

    question = (info.get("question") or "").strip()
    ctx_text = (info.get("context") or "").strip()
    default = (info.get("default") or "").strip()

    typer.echo("")
    typer.secho("❓ Question from agent:", fg=typer.colors.CYAN, bold=True)
    typer.echo(f"   {question}")
    if ctx_text:
        typer.secho(f"   why: {ctx_text}", fg=typer.colors.BRIGHT_BLACK)
    if default:
        typer.secho(f"   default: {default}", fg=typer.colors.BRIGHT_BLACK)
    typer.echo("")

    # 2. Resolve the answer text.
    body_text: Optional[str] = text
    if use_stdin:
        body_text = sys.stdin.read()
    if body_text is None:
        try:
            from prompt_toolkit import prompt
            from prompt_toolkit.formatted_text import HTML

            body_text = prompt(HTML("<b>your answer> </b>"))
        except Exception:
            body_text = input("your answer> ")
    body_text = (body_text or "").strip()

    if not body_text and not default:
        typer.secho(
            "❌ Answer is empty and no default was provided. Refusing to post.",
            fg=typer.colors.RED,
        )
        raise typer.Exit(code=3)

    # 3. POST the answer.
    try:
        post = requests.post(
            f"{api_url}/runs/{run_id}/answer",
            json={"answer": body_text},
            headers=headers,
            timeout=10.0,
        )
    except Exception as e:
        typer.secho(f"❌ POST failed: {e}", fg=typer.colors.RED)
        raise typer.Exit(code=1)

    if post.status_code == 409:
        typer.secho(
            "⚠  Question already answered or timed out before this reply landed.",
            fg=typer.colors.YELLOW,
        )
        raise typer.Exit(code=2)
    if post.status_code in (401, 403):
        typer.secho("❌ Authentication failed.", fg=typer.colors.RED)
        raise typer.Exit(code=1)
    if post.status_code != 200:
        try:
            detail = post.json().get("detail", post.text)
        except Exception:
            detail = post.text
        typer.secho(f"❌ Server returned {post.status_code}: {detail}", fg=typer.colors.RED)
        raise typer.Exit(code=1)

    final_used = body_text if body_text else default
    typer.secho(f"✅ Answer posted ({len(final_used)} chars).", fg=typer.colors.GREEN)
