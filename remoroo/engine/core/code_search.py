"""BM25-based code search over repository files and symbols.

Builds a lightweight index on first call per repo root, caches it for the
process lifetime.  No external dependencies — pure-Python BM25 over
file paths, symbol names, docstrings, and leading comments.
"""
from __future__ import annotations

import math
import os
import re
import time
from typing import Any, Dict, List, Optional, Set, Tuple

_SKIP_DIRS: Set[str] = {
    ".git", "__pycache__", "node_modules", ".venv", "venv", ".remoroo",
    ".tox", ".mypy_cache", ".pytest_cache", "dist", "build", ".eggs",
    "env", ".env", ".idea", ".vscode", "vendor", "third_party",
    ".remoroo_venvs", "egg-info",
}

_CODE_EXTS: Set[str] = {
    ".py", ".js", ".jsx", ".ts", ".tsx", ".go", ".rs",
    ".java", ".c", ".cc", ".cpp", ".h", ".hpp", ".rb", ".php",
    ".cs", ".swift", ".kt", ".scala", ".sh", ".bash",
}

_SYMBOL_RE: Dict[str, List[re.Pattern]] = {
    ".py": [
        re.compile(r"^\s*class\s+(\w+)"),
        re.compile(r"^\s*(?:async\s+)?def\s+(\w+)"),
    ],
    ".js": [
        re.compile(r"^\s*(?:export\s+)?(?:default\s+)?class\s+(\w+)"),
        re.compile(r"^\s*(?:export\s+)?(?:default\s+)?(?:async\s+)?function\s+(\w+)"),
        re.compile(r"^\s*(?:export\s+)?(?:const|let|var)\s+(\w+)\s*=\s*(?:async\s+)?\("),
    ],
    ".ts": None,  # same as .js — filled below
    ".tsx": None,
    ".jsx": None,
    ".go": [
        re.compile(r"^\s*type\s+(\w+)\s+struct"),
        re.compile(r"^\s*func\s+(?:\([^)]+\)\s*)?(\w+)"),
    ],
    ".rs": [
        re.compile(r"^\s*(?:pub\s+)?struct\s+(\w+)"),
        re.compile(r"^\s*(?:pub\s+)?enum\s+(\w+)"),
        re.compile(r"^\s*(?:pub\s+)?(?:async\s+)?fn\s+(\w+)"),
    ],
    ".java": [
        re.compile(r"^\s*(?:public|private|protected)?\s*(?:static\s+)?class\s+(\w+)"),
        re.compile(r"^\s*(?:public|private|protected)?\s*(?:static\s+)?(?:\w+\s+)+(\w+)\s*\("),
    ],
    ".c": [
        re.compile(r"^\s*(?:static\s+)?(?:inline\s+)?(?:\w+[\s*]+)+(\w+)\s*\("),
    ],
    ".cpp": None,
    ".cc": None,
    ".h": None,
    ".hpp": None,
    ".rb": [
        re.compile(r"^\s*class\s+(\w+)"),
        re.compile(r"^\s*def\s+(\w+)"),
    ],
}
_SYMBOL_RE[".ts"] = _SYMBOL_RE[".js"]
_SYMBOL_RE[".tsx"] = _SYMBOL_RE[".js"]
_SYMBOL_RE[".jsx"] = _SYMBOL_RE[".js"]
_SYMBOL_RE[".cpp"] = _SYMBOL_RE[".c"]
_SYMBOL_RE[".cc"] = _SYMBOL_RE[".c"]
_SYMBOL_RE[".h"] = _SYMBOL_RE[".c"]
_SYMBOL_RE[".hpp"] = _SYMBOL_RE[".c"]

_TOKENIZE_RE = re.compile(r"[A-Za-z][a-z]+|[A-Z]+(?=[A-Z][a-z]|\b)|[a-z]+|[A-Z][a-z]*|\d+")


def _tokenize(text: str) -> List[str]:
    """Split text into lowercase tokens, handling camelCase/snake_case/paths."""
    parts = text.replace("/", " ").replace("\\", " ").replace("_", " ").replace("-", " ").replace(".", " ")
    return [t.lower() for t in _TOKENIZE_RE.findall(parts) if len(t) > 1]


class _Document:
    __slots__ = ("path", "tokens", "symbols", "signature_lines")

    def __init__(self, path: str, tokens: List[str], symbols: List[str],
                 signature_lines: List[str]):
        self.path = path
        self.tokens = tokens
        self.symbols = symbols
        self.signature_lines = signature_lines


class CodeSearchIndex:
    """BM25 index over a repository's files, symbol names, and signatures."""

    def __init__(self, repo_root: str, *, max_file_bytes: int = 500_000):
        self.repo_root = repo_root
        self._max_file_bytes = max_file_bytes
        self._docs: List[_Document] = []
        self._df: Dict[str, int] = {}  # document frequency
        self._avgdl: float = 0.0
        self._built = False
        self._build_time_ms: float = 0.0

    def _build(self) -> None:
        if self._built:
            return
        t0 = time.monotonic()
        docs: List[_Document] = []
        df: Dict[str, int] = {}
        total_tokens = 0

        for root, dirs, files in os.walk(self.repo_root):
            dirs[:] = [d for d in dirs if d not in _SKIP_DIRS and not d.startswith(".")]
            for fname in files:
                _, ext = os.path.splitext(fname)
                if ext.lower() not in _CODE_EXTS:
                    continue
                abs_path = os.path.join(root, fname)
                rel = os.path.relpath(abs_path, self.repo_root)
                try:
                    size = os.path.getsize(abs_path)
                except OSError:
                    continue
                if size > self._max_file_bytes or size == 0:
                    continue

                try:
                    with open(abs_path, "r", encoding="utf-8", errors="ignore") as f:
                        content = f.read()
                except OSError:
                    continue

                path_tokens = _tokenize(rel)
                symbols, sig_lines = self._extract_symbols(content, ext.lower())
                symbol_tokens = []
                for sym in symbols:
                    symbol_tokens.extend(_tokenize(sym))

                comment_tokens = self._extract_leading_comments(content, ext.lower())

                all_tokens = path_tokens + symbol_tokens + symbol_tokens + comment_tokens
                if not all_tokens:
                    continue

                doc = _Document(
                    path=rel,
                    tokens=all_tokens,
                    symbols=symbols,
                    signature_lines=sig_lines[:15],
                )
                docs.append(doc)
                total_tokens += len(all_tokens)

                seen = set()
                for t in all_tokens:
                    if t not in seen:
                        df[t] = df.get(t, 0) + 1
                        seen.add(t)

        self._docs = docs
        self._df = df
        self._avgdl = total_tokens / max(len(docs), 1)
        self._built = True
        self._build_time_ms = (time.monotonic() - t0) * 1000

    @staticmethod
    def _extract_symbols(content: str, ext: str) -> Tuple[List[str], List[str]]:
        """Extract symbol names and signature lines from file content."""
        patterns = _SYMBOL_RE.get(ext)
        if not patterns:
            return [], []
        symbols: List[str] = []
        sig_lines: List[str] = []
        for line in content.split("\n"):
            for pat in patterns:
                m = pat.match(line)
                if m:
                    symbols.append(m.group(1))
                    sig_lines.append(line.rstrip()[:120])
                    break
        return symbols, sig_lines

    @staticmethod
    def _extract_leading_comments(content: str, ext: str) -> List[str]:
        """Extract tokens from file-level docstrings/comments (first ~30 lines)."""
        tokens: List[str] = []
        lines = content.split("\n")[:30]
        in_docstring = False
        for line in lines:
            stripped = line.strip()
            if ext == ".py":
                if stripped.startswith('"""') or stripped.startswith("'''"):
                    in_docstring = not in_docstring
                    tokens.extend(_tokenize(stripped))
                    continue
                if in_docstring:
                    tokens.extend(_tokenize(stripped))
                    continue
                if stripped.startswith("#"):
                    tokens.extend(_tokenize(stripped))
            elif ext in (".js", ".ts", ".tsx", ".jsx", ".java", ".go", ".rs", ".c", ".cpp"):
                if stripped.startswith("//") or stripped.startswith("/*") or stripped.startswith("*"):
                    tokens.extend(_tokenize(stripped))
        return tokens

    def search(self, query: str, max_results: int = 20) -> List[Dict[str, Any]]:
        """BM25 search. Returns ranked list of {path, score, symbols, skeleton}."""
        self._build()

        query_tokens = _tokenize(query)
        if not query_tokens:
            return []

        k1 = 1.5
        b = 0.75
        n = len(self._docs)
        if n == 0:
            return []

        results: List[Tuple[float, _Document]] = []
        for doc in self._docs:
            score = 0.0
            dl = len(doc.tokens)
            tf_map: Dict[str, int] = {}
            for t in doc.tokens:
                tf_map[t] = tf_map.get(t, 0) + 1

            for qt in query_tokens:
                tf = tf_map.get(qt, 0)
                if tf == 0:
                    continue
                df_val = self._df.get(qt, 0)
                idf = math.log((n - df_val + 0.5) / (df_val + 0.5) + 1.0)
                tf_norm = (tf * (k1 + 1)) / (tf + k1 * (1 - b + b * dl / self._avgdl))
                score += idf * tf_norm

            if score > 0:
                results.append((score, doc))

        results.sort(key=lambda x: -x[0])
        top = results[:max_results]

        out: List[Dict[str, Any]] = []
        for score, doc in top:
            skeleton = "\n".join(doc.signature_lines) if doc.signature_lines else ""
            out.append({
                "path": doc.path,
                "score": round(score, 3),
                "symbols": doc.symbols[:20],
                "skeleton": skeleton,
            })
        return out

    @property
    def stats(self) -> Dict[str, Any]:
        return {
            "files_indexed": len(self._docs),
            "build_time_ms": round(self._build_time_ms, 1),
        }


# Per-process cache: repo_root -> CodeSearchIndex
_index_cache: Dict[str, CodeSearchIndex] = {}


def get_or_build_index(repo_root: str) -> CodeSearchIndex:
    """Get cached index or build one. Cached per repo_root for process lifetime."""
    abs_root = os.path.abspath(repo_root)
    if abs_root not in _index_cache:
        _index_cache[abs_root] = CodeSearchIndex(abs_root)
    return _index_cache[abs_root]
