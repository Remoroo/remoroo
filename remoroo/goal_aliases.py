"""Thin client-side helpers for goal aliases.

The brain owns the canonical alias registry — see
``remoroo_brain/prompts/goal_aliases.py``. The CLI does *not* know prompt
text, default metrics, or seed files: it ships ``--goal @<name>`` to the
control plane verbatim and lets the server resolve everything.

This module exists for two reasons:

1. ``is_alias("@foo")`` — a one-line check used by the CLI to decide
   whether to print "this is an alias, the server will resolve it" hints
   in the launch banner. No content lookups happen here.
2. ``fetch_aliases(brain_url, token)`` — pulls the public alias summary
   from ``GET /goal_aliases`` so we can render ``remoroo aliases`` and
   tab-complete alias names.
"""
from __future__ import annotations

from typing import Any, Dict, List, Optional


def is_alias(raw_goal: Optional[str]) -> bool:
    """Return True iff ``raw_goal`` is shaped like ``@<name>``.

    Does NOT validate that the alias exists — that's the server's job.
    """
    g = (raw_goal or "").strip()
    return bool(g) and g.startswith("@") and len(g) > 1


def alias_name(raw_goal: Optional[str]) -> Optional[str]:
    """Return the bare alias name (without ``@``), or None for plain goals."""
    if not is_alias(raw_goal):
        return None
    return (raw_goal or "").strip()[1:].strip() or None


def fetch_aliases(brain_url: str, token: Optional[str] = None,
                  timeout: float = 5.0) -> List[Dict[str, Any]]:
    """Fetch the alias summary from the control plane.

    Returns an empty list if the endpoint isn't reachable or doesn't
    exist (older brains): callers should treat this as best-effort.
    """
    import requests

    url = brain_url.rstrip("/") + "/goal_aliases"
    headers = {"Authorization": f"Bearer {token}"} if token else {}
    try:
        resp = requests.get(url, headers=headers, timeout=timeout)
    except Exception:
        return []
    if resp.status_code != 200:
        return []
    body = resp.json() or {}
    aliases = body.get("aliases")
    return aliases if isinstance(aliases, list) else []
