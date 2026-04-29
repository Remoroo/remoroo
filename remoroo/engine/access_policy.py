"""Access policy: enforce a customer-declared blocklist for the agent.

One source of truth: ``.remorooignore`` at the repo root, gitignore-style.
Loaded once per ``LocalWorker`` lifetime. Two enforcement surfaces:

1. **Structured-tool gate** — ``is_blocked(path)`` is consulted at the top of
   each file-touching branch in ``LocalWorker.handle_request`` so blocked
   paths look like "doesn't exist" / "not writable" to the brain.
2. **Bash kernel-level sandbox** — ``host_bash_wrapper(repo_root)`` returns
   a function that wraps a shell command with an OS-native permission
   sandbox (``sandbox-exec`` on macOS, ``bwrap`` or ``firejail`` on Linux).
   Files stay on disk where the customer's IDE can see them; only the
   agent's bash subprocess gets EPERM on blocked paths.

No copies, no remounts. The host filesystem is unchanged.

See ``docs/access_policy_plan.md`` for the full design.
"""
from __future__ import annotations

import fnmatch
import os
import re
import shlex
import shutil
import sys
from dataclasses import dataclass
from typing import Callable, Iterable, List, Optional, Sequence, Set, Tuple


IGNORE_FILENAME = ".remorooignore"

# Patterns we always block on top of whatever the customer declares. These
# are strict secrets that no legitimate agent task should need. Customers can
# opt out by creating an empty ``.remorooignore`` (the file existing silences
# the auto-defaults — they're only added when ``remoroo block`` first writes
# the file).
HARD_DEFAULT_PATTERNS: Tuple[str, ...] = ()

# Recommended starting set written by ``remoroo block`` on first invocation.
RECOMMENDED_DEFAULT_PATTERNS: Tuple[str, ...] = (
    ".env",
    ".env.*",
    "*.pem",
    "*.key",
    "id_rsa*",
    "*.crt",
    "secrets/",
    ".aws/",
    ".ssh/",
    "credentials*",
    ".netrc",
    ".npmrc",
    "*.kdbx",
)


@dataclass(frozen=True)
class AccessPolicy:
    """Immutable policy snapshot for one worker run.

    Construct via ``AccessPolicy.load(repo_root)``; never mutate fields after
    construction. All matching is best-effort case-sensitive on POSIX, mirroring
    gitignore semantics.

    Attributes:
        repo_root: Absolute path to the repo root the policy applies to.
        patterns: Normalised gitignore-style patterns from ``.remorooignore``.
            Comments and blanks stripped, trailing slashes preserved (they
            mark directory-only patterns).
    """

    repo_root: str
    patterns: Tuple[str, ...]

    # ── Loading ─────────────────────────────────────────────────────────────

    @classmethod
    def load(cls, repo_root: str) -> "AccessPolicy":
        """Read ``.remorooignore`` from ``repo_root`` and return a policy.

        Returns an empty policy if the file is missing or unreadable. Never
        raises — a broken blocklist must not break the worker.
        """
        repo_abs = os.path.abspath(repo_root)
        path = os.path.join(repo_abs, IGNORE_FILENAME)
        patterns: List[str] = []
        if os.path.isfile(path):
            try:
                with open(path, "r", encoding="utf-8", errors="replace") as fh:
                    for raw in fh:
                        line = raw.strip()
                        if not line or line.startswith("#"):
                            continue
                        patterns.append(line)
            except OSError:
                pass
        # Hard defaults always apply, even with no .remorooignore present.
        for hd in HARD_DEFAULT_PATTERNS:
            if hd not in patterns:
                patterns.append(hd)
        return cls(repo_root=repo_abs, patterns=tuple(patterns))

    def has_patterns(self) -> bool:
        return bool(self.patterns)

    # ── Path-level checks ───────────────────────────────────────────────────

    def is_blocked(self, path: str) -> bool:
        """True if ``path`` matches any blocklist pattern.

        ``path`` may be absolute, relative to the repo root, or relative to the
        process cwd. Symlinks are resolved before matching, so a symlink in a
        non-blocked location pointing into a blocked dir is also blocked.
        """
        if not self.patterns:
            return False
        rel = self._to_repo_relative(path)
        if rel is None:
            # Path is outside the repo entirely. Don't block — the worker
            # already restricts most operations to repo_root.
            return False
        return _match_any(rel, self.patterns)

    def filter_paths(self, paths: Iterable[str]) -> List[str]:
        """Return only the entries that are not blocked, preserving order."""
        return [p for p in paths if not self.is_blocked(p)]

    def _to_repo_relative(self, path: str) -> Optional[str]:
        """Resolve ``path`` (incl. symlinks) and return it as a repo-relative
        POSIX path, or ``None`` if it lies outside the repo.
        """
        try:
            if os.path.isabs(path):
                abs_path = os.path.realpath(path)
            else:
                abs_path = os.path.realpath(os.path.join(self.repo_root, path))
        except OSError:
            return path.replace(os.sep, "/").lstrip("./")
        try:
            rel = os.path.relpath(abs_path, self.repo_root)
        except ValueError:
            return None
        rel = rel.replace(os.sep, "/")
        if rel.startswith("../") or rel == "..":
            return None
        return rel

    # ── Sandbox profile inputs ──────────────────────────────────────────────

    def absolute_subpaths(self) -> List[str]:
        """Expand patterns into concrete absolute paths under repo_root.

        Used to build sandbox-exec / bwrap / firejail rules. Returns only paths
        that currently exist on disk; pattern entries that match nothing are
        silently dropped (they'd be no-ops in the sandbox anyway).

        Note: gitignore patterns can match files that don't yet exist (e.g.
        a future ``.env``). We intentionally skip those for the kernel
        sandbox because the kernel needs literal paths, but ``is_blocked``
        still catches them at the structured-tool layer.
        """
        if not self.patterns:
            return []
        results: Set[str] = set()
        for pat in self.patterns:
            for hit in _expand_pattern(self.repo_root, pat):
                results.add(hit)
        return sorted(results)

    # ── Bash command literal pre-flight ─────────────────────────────────────

    def literal_match_in_command(self, cmd: str) -> Optional[str]:
        """Best-effort substring scan of a shell command for blocked paths.

        Returns the first matched pattern fragment if found, else ``None``.
        Used as a cheap, OS-independent guard that gives the brain a clean
        error message before the kernel sandbox even spawns the process.
        """
        if not cmd or not self.patterns:
            return None
        # Tokenise loosely so we don't trip on substrings like "data" inside
        # "datadog". We accept boundary chars common in shell syntax.
        for pat in self.patterns:
            frag = pat.rstrip("/").lstrip("/")
            if not frag:
                continue
            if "*" in frag or "?" in frag:
                # Glob — convert to a regex that requires a path-ish boundary.
                regex = _glob_to_boundary_regex(frag)
                if regex.search(cmd):
                    return pat
            else:
                # Literal — require boundary before and after to cut down on
                # false positives from substring collisions.
                regex = re.compile(rf"(?<![A-Za-z0-9_./-]){re.escape(frag)}(?![A-Za-z0-9_])")
                if regex.search(cmd):
                    return pat
        return None

    # ── Bash sandbox wrapper ────────────────────────────────────────────────

    def host_bash_wrapper(self) -> Optional[Callable[[str], str]]:
        """Return a function that wraps a shell command with an OS sandbox.

        Returns ``None`` when no kernel-level sandbox is available on this
        platform. Callers should fall back to literal pre-flight only.

        Selection order:
          - macOS  → ``sandbox-exec``
          - Linux  → ``bwrap`` (preferred) or ``firejail``
          - other  → ``None``

        The returned function accepts a shell command string and returns a
        new shell command string that, when executed via ``bash -c`` / shell,
        runs the original under the sandbox. The wrapped command preserves
        the host repo_root as the cwd, so realtime FS visibility is kept.
        """
        if not self.patterns:
            return None
        blocked = self.absolute_subpaths()
        if not blocked:
            # No concrete paths exist yet on disk — nothing for the kernel to
            # block. Pre-flight + structured-tool gate cover this case.
            return None
        if sys.platform == "darwin":
            profile = build_sandbox_exec_profile(self.repo_root, blocked)
            return _build_macos_wrapper(profile)
        if sys.platform.startswith("linux"):
            if shutil.which("bwrap"):
                return _build_bwrap_wrapper(self.repo_root, blocked)
            if shutil.which("firejail"):
                return _build_firejail_wrapper(blocked)
            return None
        return None

    # ── Convenience for callers ─────────────────────────────────────────────

    def describe_engine(self) -> str:
        """Human-readable summary of which sandbox flavour is active."""
        if not self.patterns:
            return "off (no .remorooignore patterns)"
        if sys.platform == "darwin":
            return "macOS sandbox-exec"
        if sys.platform.startswith("linux"):
            if shutil.which("bwrap"):
                return "Linux bwrap"
            if shutil.which("firejail"):
                return "Linux firejail"
            return "literal pre-flight only (install bwrap or firejail for kernel-level enforcement)"
        return "literal pre-flight only (no kernel sandbox on this OS)"


# ─── Module-level helpers ───────────────────────────────────────────────────


def _match_any(rel_posix: str, patterns: Sequence[str]) -> bool:
    """Match a repo-relative POSIX path against gitignore-ish patterns."""
    name = rel_posix.rsplit("/", 1)[-1]
    for raw in patterns:
        pat = raw.strip()
        if not pat:
            continue
        dir_only = pat.endswith("/")
        core = pat.rstrip("/")
        # Anchor at repo root if pattern starts with "/"
        anchored = core.startswith("/")
        if anchored:
            core = core.lstrip("/")
        # Directory-only patterns: match if path is the dir or under it.
        if dir_only:
            if anchored:
                if rel_posix == core or rel_posix.startswith(core + "/"):
                    return True
            else:
                # Match anywhere in the tree.
                if rel_posix == core or rel_posix.startswith(core + "/"):
                    return True
                # Pattern like "secrets/" should also match "deeply/nested/secrets/foo".
                if ("/" + core + "/") in ("/" + rel_posix + "/"):
                    return True
            continue
        # Glob/literal matching — try basename first, then full path.
        if "/" in core:
            target = rel_posix
            if anchored:
                if fnmatch.fnmatchcase(target, core):
                    return True
            else:
                # Match against each suffix of the path.
                if fnmatch.fnmatchcase(target, core):
                    return True
                # Also try nested form: "data/x" should match "sub/data/x".
                if fnmatch.fnmatchcase(target, "*/" + core):
                    return True
        else:
            # Patterns without slashes match by basename anywhere.
            if fnmatch.fnmatchcase(name, core):
                return True
    return False


def _expand_pattern(repo_root: str, pattern: str) -> List[str]:
    """Expand a single gitignore-ish pattern into concrete absolute paths."""
    pat = pattern.strip()
    if not pat or pat.startswith("#"):
        return []
    dir_only = pat.endswith("/")
    core = pat.rstrip("/").lstrip("/")
    if not core:
        return []
    hits: List[str] = []
    has_glob = any(ch in core for ch in "*?[")
    if has_glob:
        # Walk the repo and let _match_any decide. Cheaper than glob for
        # patterns like "**/*.pem" that the customer may write.
        for dirpath, dirnames, filenames in os.walk(repo_root):
            # Don't descend into .git or its peers — the customer almost never
            # means to mask ".git/secrets" via the policy, and walking it is
            # wasteful.
            dirnames[:] = [d for d in dirnames if d not in {".git", ".remoroo_venvs"}]
            rel_dir = os.path.relpath(dirpath, repo_root).replace(os.sep, "/")
            if rel_dir == ".":
                rel_dir = ""
            for d in list(dirnames):
                rel = (rel_dir + "/" + d) if rel_dir else d
                if _match_any(rel, [pattern]):
                    hits.append(os.path.join(repo_root, rel))
            if dir_only:
                continue
            for f in filenames:
                rel = (rel_dir + "/" + f) if rel_dir else f
                if _match_any(rel, [pattern]):
                    hits.append(os.path.join(repo_root, rel))
    else:
        target = os.path.join(repo_root, core)
        if os.path.exists(target):
            if dir_only and not os.path.isdir(target):
                return []
            hits.append(target)
        else:
            # Maybe the pattern targets a basename anywhere — walk and find it.
            if "/" not in core:
                for dirpath, dirnames, filenames in os.walk(repo_root):
                    dirnames[:] = [d for d in dirnames if d not in {".git", ".remoroo_venvs"}]
                    candidates = list(dirnames) if dir_only else (list(dirnames) + list(filenames))
                    for c in candidates:
                        if c == core:
                            hits.append(os.path.join(dirpath, c))
    return hits


def _glob_to_boundary_regex(glob: str) -> "re.Pattern[str]":
    """Convert a fnmatch-style glob to a regex that requires path boundaries
    on either side, to make the literal pre-flight stricter on substrings.
    """
    parts: List[str] = []
    i = 0
    while i < len(glob):
        ch = glob[i]
        if ch == "*":
            parts.append(r"[A-Za-z0-9_./*?-]*")
        elif ch == "?":
            parts.append(r"[A-Za-z0-9_.-]")
        elif ch in r".+()|^$\{}[]":
            parts.append(re.escape(ch))
        else:
            parts.append(ch)
        i += 1
    body = "".join(parts)
    return re.compile(rf"(?<![A-Za-z0-9_./-]){body}(?![A-Za-z0-9_])")


# ─── Sandbox profile builders ────────────────────────────────────────────────


def build_sandbox_exec_profile(repo_root: str, blocked_abs: Sequence[str]) -> str:
    """Build an SBPL profile for ``sandbox-exec`` (macOS) that denies all
    file-read and file-write operations on the given absolute paths.

    The base profile is permissive (``allow default``) so the agent's bash
    behaves identically except for the denied paths.
    """
    lines = [
        "(version 1)",
        "(allow default)",
    ]
    # Fold all subpath denies into one rule for compactness.
    if blocked_abs:
        ops = ("file-read*", "file-read-data", "file-read-metadata",
               "file-write*", "file-write-data")
        denies: List[str] = []
        for path in blocked_abs:
            # SBPL string literals are wrapped in double quotes; embed-escape
            # any quotes (rare in real paths but cheap to be safe).
            esc = path.replace("\\", "\\\\").replace('"', '\\"')
            if os.path.isdir(path):
                denies.append(f'(subpath "{esc}")')
            else:
                denies.append(f'(literal "{esc}")')
        lines.append("(deny " + " ".join(ops) + " " + " ".join(denies) + ")")
    return "\n".join(lines) + "\n"


def _build_macos_wrapper(profile: str) -> Callable[[str], str]:
    """Return a wrapper that prepends ``sandbox-exec -p <profile>`` to a cmd.

    The profile is passed as a single argv element so paths with spaces or
    quotes embedded in patterns can't break out.
    """

    def _wrap(cmd: str) -> str:
        # We cannot stream the profile via stdin while preserving shell=True,
        # so embed it directly as the -p argument. shlex.quote handles the
        # newlines/quotes inside the profile.
        return f"sandbox-exec -p {shlex.quote(profile)} bash -c {shlex.quote(cmd)}"

    return _wrap


def _build_bwrap_wrapper(repo_root: str, blocked_abs: Sequence[str]) -> Callable[[str], str]:
    """Linux bwrap wrapper. Mounts a tmpfs (for dirs) or /dev/null (for files)
    on top of each blocked path inside the child's mount namespace. The host
    filesystem is unchanged.
    """
    args: List[str] = ["bwrap", "--die-with-parent", "--bind", "/", "/"]
    # Make /proc and /dev present inside the mount namespace.
    args += ["--proc", "/proc", "--dev", "/dev"]
    for path in blocked_abs:
        if os.path.isdir(path):
            args += ["--tmpfs", path]
        else:
            args += ["--ro-bind", "/dev/null", path]
    args += ["--chdir", repo_root, "bash", "-c"]
    prefix = " ".join(shlex.quote(a) for a in args)

    def _wrap(cmd: str) -> str:
        return f"{prefix} {shlex.quote(cmd)}"

    return _wrap


def _build_firejail_wrapper(blocked_abs: Sequence[str]) -> Callable[[str], str]:
    """Linux firejail fallback. Uses ``--blacklist`` to deny each blocked path."""
    args: List[str] = ["firejail", "--quiet", "--noprofile"]
    for path in blocked_abs:
        args.append(f"--blacklist={path}")
    args += ["bash", "-c"]
    prefix = " ".join(shlex.quote(a) for a in args)

    def _wrap(cmd: str) -> str:
        return f"{prefix} {shlex.quote(cmd)}"

    return _wrap


# ─── .remorooignore mutation helpers (used by ``remoroo block`` CLI) ─────────


def ignore_file_path(repo_root: str) -> str:
    return os.path.join(os.path.abspath(repo_root), IGNORE_FILENAME)


def seed_default_ignore_if_missing(repo_root: str) -> bool:
    """Create ``.remorooignore`` with the recommended defaults if absent.

    Returns ``True`` if the file was just created, ``False`` if it already
    existed (even empty — an empty file is the customer's explicit opt-out).
    Called by ``LocalWorker.__init__`` so customers get sensible secret-hiding
    on day one without having to read docs first.

    Idempotent on subsequent runs. Never raises — failure to seed must not
    block the worker.
    """
    path = ignore_file_path(repo_root)
    if os.path.exists(path):
        return False
    try:
        with open(path, "w", encoding="utf-8") as fh:
            fh.write("# Remoroo blocklist — paths here are hidden from the agent.\n")
            fh.write("# Same syntax as .gitignore. Edit by hand or via `remoroo block <pattern>`.\n")
            fh.write("# To disable Remoroo's privacy guard entirely, leave this file empty.\n")
            fh.write("\n# Recommended defaults (auto-seeded on first run):\n")
            for default_pat in RECOMMENDED_DEFAULT_PATTERNS:
                fh.write(default_pat + "\n")
        return True
    except OSError:
        return False


def read_ignore_file(repo_root: str) -> List[str]:
    """Return the patterns listed in ``.remorooignore`` (excluding comments)."""
    path = ignore_file_path(repo_root)
    if not os.path.isfile(path):
        return []
    out: List[str] = []
    try:
        with open(path, "r", encoding="utf-8", errors="replace") as fh:
            for raw in fh:
                line = raw.strip()
                if not line or line.startswith("#"):
                    continue
                out.append(line)
    except OSError:
        return []
    return out


def append_to_ignore_file(repo_root: str, patterns: Sequence[str]) -> Tuple[List[str], List[str]]:
    """Append patterns to ``.remorooignore``. Idempotent.

    Returns ``(added, already_present)``. Creates the file with a header
    comment + recommended defaults on first write.
    """
    path = ignore_file_path(repo_root)
    existing = read_ignore_file(repo_root)
    existing_set = set(existing)
    added: List[str] = []
    already: List[str] = []
    for pat in patterns:
        pat = pat.strip()
        if not pat:
            continue
        if pat in existing_set:
            already.append(pat)
            continue
        added.append(pat)
        existing_set.add(pat)

    if not added and os.path.isfile(path):
        return added, already

    is_new_file = not os.path.isfile(path)
    with open(path, "a", encoding="utf-8") as fh:
        if is_new_file:
            fh.write("# Remoroo blocklist — paths here are hidden from the agent.\n")
            fh.write("# Same syntax as .gitignore. Edit by hand or via `remoroo block <pattern>`.\n")
            fh.write("\n# Recommended defaults:\n")
            for default_pat in RECOMMENDED_DEFAULT_PATTERNS:
                fh.write(default_pat + "\n")
                if default_pat not in existing_set:
                    existing_set.add(default_pat)
                    if default_pat not in added:
                        added.append(default_pat)
            fh.write("\n# Custom entries:\n")
        for pat in added:
            if is_new_file and pat in RECOMMENDED_DEFAULT_PATTERNS:
                # Already written above as a default.
                continue
            fh.write(pat + "\n")
    return added, already


def remove_from_ignore_file(repo_root: str, pattern: str) -> bool:
    """Remove the literal line matching ``pattern``. Returns True if removed."""
    path = ignore_file_path(repo_root)
    if not os.path.isfile(path):
        return False
    target = pattern.strip()
    removed = False
    try:
        with open(path, "r", encoding="utf-8", errors="replace") as fh:
            lines = fh.readlines()
    except OSError:
        return False
    new_lines: List[str] = []
    for line in lines:
        if line.strip() == target and not removed:
            removed = True
            continue
        new_lines.append(line)
    if not removed:
        return False
    try:
        with open(path, "w", encoding="utf-8") as fh:
            fh.writelines(new_lines)
    except OSError:
        return False
    return True
