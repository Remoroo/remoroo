"""`remoroo continue <url>` — land a Try-Now session locally.

Scope (Stage 2, §8.2 of the Try-Now plan): this command is a
**repository-preparation command**, nothing more. It fetches the
receipt for a completed Try-Now run, clones the env-specific public
template repo at the same base commit, applies the final diff, and
optionally drops the agent's final checkpoint in place. It does **not**
invoke `remoroo run`, it does not detect GPUs, it does not branch on
hardware — the user decides what to do next.

All side-effecting operations (HTTP fetch, `git clone`, `git apply`,
filesystem writes) are injected, so the pure logic is unit-tested
without network or git. The `remoroo continue` Typer command in
`cli.py` wires the defaults.

See §8.2 in `docs/try_now_implementation_plan.md` for the product spec.
"""
from __future__ import annotations

import json
import os
import re
import subprocess
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable, Dict, Mapping, Optional


# ── Configuration + results ────────────────────────────────────────-

# Used only when the receipt does not already point at an explicit
# template URL (i.e. the server hasn't upgraded to the richer schema
# yet). The `{env}` placeholder is substituted with the lowercased env
# name from the receipt. Operators can override via the
# REMOROO_TRY_NOW_TEMPLATE_URL env var or the `--template-url-template`
# CLI flag.
DEFAULT_TEMPLATE_URL_TEMPLATE = (
    "https://github.com/remoroo-inc/try_now_{env}.git"
)

DEFAULT_RECEIPT_URL_TEMPLATE = (
    "https://remoroo.com/api/try/receipts/{rid}"
)


@dataclass
class ContinueConfig:
    """User-facing inputs to the command.

    `input_url` can be any of:
      * `https://remoroo.com/r/<rid>`     (public receipt page)
      * `https://remoroo.com/api/try/receipts/<rid>` (JSON endpoint)
      * a bare receipt id like `abc123`

    The resolver (`resolve_receipt_url`) accepts all three.
    """

    input_url: str
    dest_dir: Path
    with_checkpoint: bool = False
    template_url_template: str = DEFAULT_TEMPLATE_URL_TEMPLATE
    receipt_url_template: str = DEFAULT_RECEIPT_URL_TEMPLATE


@dataclass
class ContinueResult:
    rid: str
    env: str
    receipt: Mapping[str, Any]
    clone_dir: Path
    wrote_checkpoint: bool
    warnings: list = field(default_factory=list)


class ContinueError(RuntimeError):
    """User-visible failure with a non-zero exit code.

    The Typer wrapper in `cli.py` maps this to `typer.Exit(code)` and
    prints `message` in red; the code is also surfaced for scripted
    callers that want to distinguish "bad URL" from "diff rejected".
    """

    def __init__(self, message: str, *, code: int = 1) -> None:
        super().__init__(message)
        self.message = message
        self.code = code


# ── Injectable protocols ────────────────────────────────────────────
#
# Every side-effecting operation is a `Callable`, not a `Protocol`
# subclass — tests can pass plain lambdas / recorders and the CLI
# passes real implementations. Same convention as `diff_hook.py` and
# `clip_watch.py`.

FetchBytesFn = Callable[[str], bytes]
CloneFn = Callable[[str, str, Path], None]  # (repo_url, commit_sha, dest)
ApplyFn = Callable[[Path, bytes], None]     # (repo_dir, diff_bytes)


# ── Receipt URL resolver ────────────────────────────────────────────

# A lenient receipt-id extractor. The `remoroo.com/r/<rid>` URL format
# is the canonical public link (§8.3) but copy-pasters often paste with
# trailing slashes, fragments, or query strings — so we grab the id
# anywhere we recognise the path structure. Valid rids are 6-40 chars,
# url-safe (letters, digits, `-`, `_`). We keep the character class
# wide enough to survive schema tweaks (e.g. rid switching to base32)
# but narrow enough to reject obviously malformed input.
_RID_PATTERN = re.compile(r"[A-Za-z0-9_-]{6,40}")
_URL_R_PATH = re.compile(r"/r/([A-Za-z0-9_-]{6,40})(?:[/?#].*)?$")
_URL_RECEIPTS_PATH = re.compile(
    r"/api/try/receipts/([A-Za-z0-9_-]{6,40})(?:[/?#].*)?$"
)


def resolve_receipt_url(
    input_url: str,
    *,
    receipt_url_template: str = DEFAULT_RECEIPT_URL_TEMPLATE,
) -> tuple[str, str]:
    """Return `(rid, receipt_json_url)`.

    Raises `ContinueError` if the input cannot be parsed. The receipt
    JSON URL is always produced from the rid via the template so the
    shape of the endpoint is consistent even when the user pastes the
    HTML URL.
    """
    if not isinstance(input_url, str):
        raise ContinueError("Expected a URL or receipt id string.", code=1)
    cleaned = input_url.strip()
    if not cleaned:
        raise ContinueError("Empty URL / receipt id.", code=1)

    rid: Optional[str] = None
    m = _URL_RECEIPTS_PATH.search(cleaned)
    if m:
        rid = m.group(1)
    else:
        m = _URL_R_PATH.search(cleaned)
        if m:
            rid = m.group(1)
        elif "://" not in cleaned and "/" not in cleaned:
            # Bare id — reject anything that isn't exclusively valid
            # rid characters so pasting "./" or a local path doesn't
            # accidentally get interpreted as a receipt id.
            if _RID_PATTERN.fullmatch(cleaned):
                rid = cleaned
    if rid is None:
        raise ContinueError(
            f"Could not find a receipt id in {cleaned!r}. "
            "Expected a URL like https://remoroo.com/r/<rid> or a bare <rid>.",
            code=1,
        )

    return rid, receipt_url_template.format(rid=rid)


# ── Receipt → resource URLs ─────────────────────────────────────────

def _receipt_field(receipt: Mapping[str, Any], *path: str) -> Any:
    cur: Any = receipt
    for key in path:
        if not isinstance(cur, Mapping):
            return None
        cur = cur.get(key)
    return cur


def artefact_url_from_receipt(
    receipt: Mapping[str, Any],
    *,
    kind: str,
) -> Optional[str]:
    """Pick the absolute URL for a named artefact from the receipt.

    Supported kinds (v1): `final_diff`, `checkpoint`.

    The v1 receipt schema (see `remoroo_cp/try_now/receipt.py`) stores
    `final_diff` as a relative R2 key; more recent CP deployments also
    attach a `urls` block with pre-signed absolute URLs. We prefer the
    absolute form when present.
    """
    urls = _receipt_field(receipt, "urls")
    if isinstance(urls, Mapping):
        u = urls.get(kind)
        if isinstance(u, str) and u.strip():
            return u.strip()

    if kind == "final_diff":
        rel = _receipt_field(receipt, "final_diff")
        base = _receipt_field(receipt, "artefact_base_url")
        if isinstance(rel, str) and isinstance(base, str):
            return _join_url(base, rel)
    if kind == "checkpoint":
        rel = _receipt_field(receipt, "checkpoint_key")
        base = _receipt_field(receipt, "artefact_base_url")
        if isinstance(rel, str) and isinstance(base, str):
            return _join_url(base, rel)

    return None


def _join_url(base: str, rel: str) -> str:
    """Join base + rel without dropping the base path prefix (which
    `urllib.parse.urljoin` would do for absolute-looking rels like
    `diffs/final.diff`).
    """
    if not base:
        return rel
    if not rel:
        return base
    if rel.startswith("http://") or rel.startswith("https://"):
        return rel
    if base.endswith("/") and rel.startswith("/"):
        return base[:-1] + rel
    if not base.endswith("/") and not rel.startswith("/"):
        return base + "/" + rel
    return base + rel


# ── Default fetch / clone / apply ──────────────────────────────────-

def default_fetch_bytes(url: str) -> bytes:
    """Fetch an absolute URL as bytes using `requests`.

    Never returns a partial read; raises `ContinueError(code=1)` on any
    failure. Tiny wrapper so callers get a predictable exception type.
    """
    try:
        import requests
    except ImportError as exc:  # pragma: no cover — CLI always has requests
        raise ContinueError(
            "`requests` is required for `remoroo continue`. Reinstall the CLI."
        ) from exc

    try:
        resp = requests.get(url, timeout=60.0)
    except Exception as exc:  # noqa: BLE001
        raise ContinueError(f"Could not fetch {url}: {exc}", code=1) from exc
    if resp.status_code == 404:
        raise ContinueError(f"Not found: {url}", code=1)
    if resp.status_code >= 400:
        raise ContinueError(
            f"HTTP {resp.status_code} fetching {url}",
            code=1,
        )
    return resp.content


def default_clone_fn(repo_url: str, commit_sha: str, dest: Path) -> None:
    """`git clone` then `git checkout <commit_sha>` to reach the exact
    base commit. We use a two-step so the error for "commit not in
    remote" is unambiguous; a single `git clone --branch <sha>` only
    works for branches/tags.
    """
    dest.parent.mkdir(parents=True, exist_ok=True)
    if dest.exists():
        raise ContinueError(
            f"Destination {dest} already exists; pick an empty path.", code=1
        )
    cp = subprocess.run(
        ["git", "clone", "--quiet", repo_url, str(dest)],
        capture_output=True,
    )
    if cp.returncode != 0:
        raise ContinueError(
            f"git clone {repo_url} failed: "
            + cp.stderr.decode("utf-8", errors="replace")[:500],
            code=1,
        )
    cp = subprocess.run(
        ["git", "-C", str(dest), "checkout", "--quiet", commit_sha],
        capture_output=True,
    )
    if cp.returncode != 0:
        raise ContinueError(
            f"git checkout {commit_sha} failed in {dest}: "
            + cp.stderr.decode("utf-8", errors="replace")[:500],
            code=1,
        )


def default_apply_fn(repo_dir: Path, diff_bytes: bytes) -> None:
    """`git apply --index` the unified diff inside `repo_dir`. Fails
    loud: per §8.2 "if the diff fails to apply ... the command fails
    loud and tells the user what went wrong". No `--3way`, no silent
    skips — the receipt's base commit is exactly the template's base
    commit, so applying cleanly is the contract.
    """
    cp = subprocess.run(
        ["git", "-C", str(repo_dir), "apply", "--index", "-"],
        input=diff_bytes,
        capture_output=True,
    )
    if cp.returncode != 0:
        raise ContinueError(
            "git apply failed. The remote diff did not apply to the local "
            "template. "
            + cp.stderr.decode("utf-8", errors="replace")[:800],
            code=2,
        )


# ── Program.md writer ──────────────────────────────────────────────-

def _program_md_contents(receipt: Mapping[str, Any]) -> str:
    """Reproduce the goal + metrics strings the remote run used.

    The receipt doesn't carry goal/metric text verbatim (that's brain
    input, not public output); we derive a short description from the
    public fields so the local brain has *some* context if the user
    runs `remoroo run`. Users who need the exact goal can re-enter it
    on the CLI wizard.
    """
    env = str(_receipt_field(receipt, "env") or "unknown")
    target = _receipt_field(receipt, "target")
    baseline_reward = _receipt_field(receipt, "baseline", "reward")
    metric_key = (
        _receipt_field(receipt, "metric_key")
        or "mean_reward"
    )
    best_reward = _receipt_field(receipt, "best", "reward")
    lines = [
        f"# Remoroo continue: {env}",
        "",
        "This repository was seeded by `remoroo continue` from a completed "
        "Try-Now session. The final diff has already been applied.",
        "",
        "## Goal",
        "",
        f"Train a `{env}` agent to maximise `{metric_key}` above the target.",
        "",
        "## Targets",
        "",
        f"- **Baseline** (stock agent at session start): `{baseline_reward}`",
        f"- **Session best**: `{best_reward}`",
        f"- **Bar to beat**: `{target}`",
        "",
        "## Metrics",
        "",
        f"- `{metric_key}`",
        "",
        "## Next",
        "",
        "Inspect the diff (`git diff HEAD~1`) and/or run:",
        "",
        "    remoroo run --local",
        "",
    ]
    return "\n".join(lines)


def _write_program_md(repo_dir: Path, receipt: Mapping[str, Any]) -> None:
    target = repo_dir / "program.md"
    # Not atomic-via-rename because this runs after `git apply` already
    # wrote lots of files non-atomically; the failure mode (partial
    # write on CTRL-C) is exactly what the user would see anyway, and
    # worth fewer temp files in their working copy.
    target.write_text(_program_md_contents(receipt), encoding="utf-8")


def _write_checkpoint(repo_dir: Path, content: bytes) -> Path:
    target = repo_dir / "checkpoint.pt"
    tmp = target.with_suffix(".pt.partial")
    tmp.write_bytes(content)
    os.replace(tmp, target)
    return target


# ── Top-level orchestrator ──────────────────────────────────────────

def run_continue(
    cfg: ContinueConfig,
    *,
    fetch_bytes_fn: FetchBytesFn = default_fetch_bytes,
    clone_fn: CloneFn = default_clone_fn,
    apply_fn: ApplyFn = default_apply_fn,
) -> ContinueResult:
    """Full flow: receipt → clone → apply → program.md → (optional) checkpoint.

    Every step raises `ContinueError` on failure so the CLI wrapper
    can map the message+code cleanly. Success returns a `ContinueResult`
    so callers (and tests) can assert on the warnings list.
    """
    rid, receipt_url = resolve_receipt_url(
        cfg.input_url, receipt_url_template=cfg.receipt_url_template
    )

    raw = fetch_bytes_fn(receipt_url)
    try:
        receipt = json.loads(raw.decode("utf-8"))
    except Exception as exc:  # noqa: BLE001
        raise ContinueError(
            f"Receipt at {receipt_url} is not valid JSON: {exc}", code=1
        ) from exc
    if not isinstance(receipt, Mapping):
        raise ContinueError("Receipt JSON must be an object.", code=1)

    env = _receipt_field(receipt, "env")
    base_commit = _receipt_field(receipt, "repo_base_commit")
    if not isinstance(env, str) or not env:
        raise ContinueError("Receipt is missing `env`.", code=1)
    if not isinstance(base_commit, str) or not base_commit:
        raise ContinueError(
            "Receipt is missing `repo_base_commit`.", code=1
        )

    final_diff_url = artefact_url_from_receipt(receipt, kind="final_diff")
    if not final_diff_url:
        raise ContinueError(
            "Receipt does not expose a URL for `final_diff`. "
            "The server may not have uploaded the diff yet.",
            code=1,
        )

    template_url = cfg.template_url_template.format(env=env.lower())

    # Resolve destination before clone so we fail fast on collisions.
    dest = cfg.dest_dir
    if dest.exists():
        if not dest.is_dir():
            raise ContinueError(
                f"Destination {dest} already exists and is not a directory.",
                code=1,
            )
        if any(dest.iterdir()):
            raise ContinueError(
                f"Destination {dest} already exists and is not empty.",
                code=1,
            )

    clone_fn(template_url, base_commit, dest)

    diff_bytes = fetch_bytes_fn(final_diff_url)
    if not diff_bytes:
        raise ContinueError(
            f"Final diff at {final_diff_url} is empty — aborting.", code=1
        )
    apply_fn(dest, diff_bytes)

    _write_program_md(dest, receipt)

    wrote_checkpoint = False
    warnings: list = []
    if cfg.with_checkpoint:
        ck_url = artefact_url_from_receipt(receipt, kind="checkpoint")
        if not ck_url:
            warnings.append(
                "No checkpoint URL in receipt; --with-checkpoint ignored."
            )
        else:
            ck_bytes = fetch_bytes_fn(ck_url)
            if not ck_bytes:
                warnings.append(
                    f"Checkpoint at {ck_url} is empty; skipping."
                )
            else:
                _write_checkpoint(dest, ck_bytes)
                wrote_checkpoint = True

    return ContinueResult(
        rid=rid,
        env=env,
        receipt=receipt,
        clone_dir=dest,
        wrote_checkpoint=wrote_checkpoint,
        warnings=warnings,
    )


__all__ = [
    "ApplyFn",
    "CloneFn",
    "ContinueConfig",
    "ContinueError",
    "ContinueResult",
    "DEFAULT_RECEIPT_URL_TEMPLATE",
    "DEFAULT_TEMPLATE_URL_TEMPLATE",
    "FetchBytesFn",
    "artefact_url_from_receipt",
    "default_apply_fn",
    "default_clone_fn",
    "default_fetch_bytes",
    "resolve_receipt_url",
    "run_continue",
]
