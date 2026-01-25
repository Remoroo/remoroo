"""
Repository indexer for large-repo migration (v2).

Extracts structural information from repositories:
- Symbols (classes, functions, methods)
- Module dependencies
- Entrypoints
- File metadata

Designed for incremental updates and symbol-level hydration.
"""
from __future__ import annotations
import os
import ast
import json
import hashlib
import time
from typing import Dict, Any, List, Optional, Set, Tuple
from pathlib import Path
from datetime import datetime

from ..utils.configs import DEFAULT_DENY_PATHS, DEFAULT_EXCLUDED_DIRS, DEFAULT_EXCLUDED_FILES


def _compute_sha256_bytes(data: bytes) -> str:
    """Compute SHA256 hash of bytes."""
    sha256_hash = hashlib.sha256()
    sha256_hash.update(data)
    return sha256_hash.hexdigest()


def _compute_sha256(file_path: str) -> str:
    """Backward-compatible file hash helper (used by other modules).

    NOTE: Prefer using mtime/size for change detection. This function exists for
    compatibility and for cases where a content hash is required.
    """
    try:
        with open(file_path, "rb") as f:
            return _compute_sha256_bytes(f.read())
    except Exception:
        return ""


def _safe_stat(path: str) -> Tuple[int, float]:
    """Return (size_bytes, mtime) safely."""
    try:
        st = os.stat(path)
        return int(st.st_size), float(st.st_mtime)
    except Exception:
        return 0, 0.0


class PythonIndexer:
    """Python AST-based indexer for extracting symbols from Python files."""
    
    def __init__(self, repo_root: str):
        self.repo_root = repo_root
    
    class _Visitor(ast.NodeVisitor):
        """Single-pass visitor to collect imports, symbols, exports, and (some) entrypoints."""
        def __init__(self, module_name: str, file_path: str, lines: List[str]):
            self.module_name = module_name
            self.file_path = file_path
            self.lines = lines
            self.imports: Set[str] = set()
            self.exports: Set[str] = set()
            self.entrypoints: List[Dict[str, Any]] = []
            self.top_level_symbols: List[str] = []
            self.symbols: List[Dict[str, Any]] = []
            self._class_stack: List[str] = []

        def visit_Import(self, node: ast.Import) -> Any:
            for alias in node.names:
                if alias.name:
                    self.imports.add(alias.name)
            self.generic_visit(node)

        def visit_ImportFrom(self, node: ast.ImportFrom) -> Any:
            # Normalize relative imports to fully-qualified module names so downstream
            # reachability (instrumentation target selection) can follow them.
            #
            # Examples (module_name="jobber.__main__"):
            # - from .core.runner import run  -> imports "jobber.core.runner"
            # - from ..io import output       -> imports "jobber.io"
            #
            # For absolute imports (level==0), keep node.module as-is.
            try:
                level = int(getattr(node, "level", 0) or 0)
            except Exception:
                level = 0

            mod = node.module  # may be None for `from . import x`
            if mod:
                if level <= 0:
                    self.imports.add(mod)
                else:
                    # Compute base package for relative import.
                    # Start from the current module's package (drop the last segment).
                    pkg_parts = self.module_name.split(".")[:-1] if self.module_name else []
                    # level==1 means "current package"; level==2 means parent of current package, etc.
                    up = max(0, level - 1)
                    if up and len(pkg_parts) >= up:
                        pkg_parts = pkg_parts[: -up]
                    # If we underflow, fall back to best-effort prefixing.
                    if not pkg_parts:
                        self.imports.add(mod)
                    else:
                        self.imports.add(".".join(pkg_parts + mod.split(".")))
            self.generic_visit(node)

        def visit_ClassDef(self, node: ast.ClassDef) -> Any:
            line_start = getattr(node, "lineno", 1)
            line_end = getattr(node, "end_lineno", None) or line_start
            class_name = node.name
            qualified_name = f"{self.module_name}.{class_name}" if self.module_name else class_name
            symbol_id = f"sym:{self.module_name}:{class_name}#L{line_start}"
            self.top_level_symbols.append(symbol_id)
            if not class_name.startswith("_"):
                self.exports.add(class_name)

            # Signature (cheap)
            signature = f"class {class_name}"
            if getattr(node, "bases", None):
                try:
                    base_names = []
                    for base in node.bases:
                        if isinstance(base, ast.Name):
                            base_names.append(base.id)
                        elif isinstance(base, ast.Attribute):
                            # best-effort
                            base_names.append(getattr(base, "attr", "Unknown"))
                    if base_names:
                        signature += f"({', '.join(base_names)})"
                except Exception:
                    pass
            signature += ": ..."

            doc = ast.get_docstring(node) or ""
            doc = doc[:200] if doc else ""

            self.symbols.append({
                "id": symbol_id,
                "kind": "class",
                "name": class_name,
                "qualified_name": qualified_name,
                "file_path": self.file_path,
                "span": {"line_start": line_start, "line_end": line_end},
                "signature": signature,
                "doc": doc,
                "exports": [],
                "references": {
                    "calls": [],
                    "called_by": [],
                    # Best-effort: attach file-level imports as "imports_used"
                    "imports_used": []
                },
                "visibility": "public" if not class_name.startswith("_") else "private"
            })

            self._class_stack.append(class_name)
            self.generic_visit(node)
            self._class_stack.pop()

        def visit_FunctionDef(self, node: ast.FunctionDef) -> Any:
            line_start = getattr(node, "lineno", 1)
            line_end = getattr(node, "end_lineno", None) or line_start
            func_name = node.name

            if self._class_stack:
                cls = self._class_stack[-1]
                qualified_name = f"{self.module_name}.{cls}.{func_name}" if self.module_name else f"{cls}.{func_name}"
                symbol_id = f"sym:{self.module_name}:{cls}.{func_name}#L{line_start}"
                kind = "method"
            else:
                qualified_name = f"{self.module_name}.{func_name}" if self.module_name else func_name
                symbol_id = f"sym:{self.module_name}:{func_name}#L{line_start}"
                kind = "function"
                self.top_level_symbols.append(symbol_id)
                if not func_name.startswith("_"):
                    self.exports.add(func_name)

            # Signature: cheap parse from source line
            signature = ""
            try:
                if 1 <= line_start <= len(self.lines):
                    def_line = self.lines[line_start - 1]
                    signature = def_line.split(":")[0].strip() if ":" in def_line else f"def {func_name}(...)"
                else:
                    signature = f"def {func_name}(...)"
            except Exception:
                signature = f"def {func_name}(...)"

            doc = ast.get_docstring(node) or ""
            doc = doc[:200] if doc else ""

            self.symbols.append({
                "id": symbol_id,
                "kind": kind,
                "name": func_name,
                "qualified_name": qualified_name,
                "file_path": self.file_path,
                "span": {"line_start": line_start, "line_end": line_end},
                "signature": signature,
                "doc": doc,
                "exports": [],
                "references": {
                    "calls": [],
                    "called_by": [],
                    "imports_used": []
                },
                "visibility": "public" if not func_name.startswith("_") else "private"
            })

            self.generic_visit(node)

        def finalize(self) -> None:
            # Fill imports_used with file-level imports (best-effort, avoids quadratic scans)
            imports_sorted = sorted(self.imports)
            for sym in self.symbols:
                sym["references"]["imports_used"] = imports_sorted
    
    def index_file(self, file_path: str) -> Dict[str, Any]:
        """
        Index a single Python file.
        
        Returns:
            {
                "path": relative path,
                "sha256": file hash,
                "size_bytes": file size,
                "language": "python",
                "module": module name,
                "imports": list of imports,
                "exports": list of exported names,
                "top_level_symbols": list of symbol IDs,
                "entrypoints": list of entrypoint info
            }
        """
        abs_path = os.path.join(self.repo_root, file_path)
        if not os.path.exists(abs_path):
            raise FileNotFoundError(f"File not found: {abs_path}")
        
        # Read once
        with open(abs_path, "rb") as f:
            raw = f.read()
        try:
            content = raw.decode("utf-8", errors="ignore")
        except Exception:
            content = ""

        # Stat once
        size_bytes, mtime = _safe_stat(abs_path)
        # Compute hash from already-read bytes (no second read)
        file_hash = _compute_sha256_bytes(raw) if raw else ""
        
        # Parse module name from path
        module_name = self._path_to_module(file_path)
        
        lines = content.split("\n") if content else []
        try:
            tree = ast.parse(content, filename=file_path)
            visitor = self._Visitor(module_name, file_path, lines)
            visitor.visit(tree)
            visitor.finalize()
            imports = sorted(visitor.imports)
            exports = sorted(visitor.exports)
            top_level_symbols = visitor.top_level_symbols
            entrypoints = []

            # Entrypoint: __main__ block (cheap string scan)
            if 'if __name__ == "__main__"' in content:
                main_start, main_end = self._find_main_block_lines(content)
                if main_start:
                    entrypoints.append({"kind": "__main__", "line_start": main_start, "line_end": main_end})

            # Entrypoint: top-level main/cli/etc (name-based + cheap body scan)
            for sym in visitor.symbols:
                if sym.get("kind") == "function":
                    name = sym.get("name", "")
                    if name and name.lower() in ["main", "cli", "entrypoint"]:
                        entrypoints.append({
                            "kind": "function",
                            "name": name,
                            "line_start": sym["span"]["line_start"],
                            "line_end": sym["span"]["line_end"]
                        })
        except SyntaxError:
            imports, exports, top_level_symbols, entrypoints = [], [], [], []
        
        return {
            "path": file_path,
            "sha256": file_hash,
            "size_bytes": size_bytes,
            "mtime": mtime,
            "language": "python",
            "module": module_name,
            "imports": imports,
            "exports": exports,
            "top_level_symbols": top_level_symbols,
            "entrypoints": entrypoints
        }

    def index_file_with_symbols(self, file_path: str) -> Tuple[Dict[str, Any], List[Dict[str, Any]]]:
        """Index file and extract symbols in a single parse pass."""
        abs_path = os.path.join(self.repo_root, file_path)
        if not os.path.exists(abs_path):
            raise FileNotFoundError(f"File not found: {abs_path}")

        with open(abs_path, "rb") as f:
            raw = f.read()
        try:
            content = raw.decode("utf-8", errors="ignore")
        except Exception:
            content = ""
        size_bytes, mtime = _safe_stat(abs_path)
        file_hash = _compute_sha256_bytes(raw) if raw else ""
        module_name = self._path_to_module(file_path)
        lines = content.split("\n") if content else []

        file_info = {
            "path": file_path,
            "sha256": file_hash,
            "size_bytes": size_bytes,
            "mtime": mtime,
            "language": "python",
            "module": module_name,
            "imports": [],
            "exports": [],
            "top_level_symbols": [],
            "entrypoints": []
        }

        try:
            tree = ast.parse(content, filename=file_path)
            visitor = self._Visitor(module_name, file_path, lines)
            visitor.visit(tree)
            visitor.finalize()
            file_info["imports"] = sorted(visitor.imports)
            file_info["exports"] = sorted(visitor.exports)
            file_info["top_level_symbols"] = visitor.top_level_symbols

            eps: List[Dict[str, Any]] = []
            if 'if __name__ == "__main__"' in content:
                main_start, main_end = self._find_main_block_lines(content)
                if main_start:
                    eps.append({"kind": "__main__", "line_start": main_start, "line_end": main_end})
            for sym in visitor.symbols:
                if sym.get("kind") == "function":
                    name = sym.get("name", "")
                    if name and name.lower() in ["main", "cli", "entrypoint"]:
                        eps.append({"kind": "function", "name": name, "line_start": sym["span"]["line_start"], "line_end": sym["span"]["line_end"]})
            file_info["entrypoints"] = eps
            return file_info, visitor.symbols
        except SyntaxError:
            return file_info, []
    
    def extract_symbols(self, file_path: str) -> List[Dict[str, Any]]:
        """Backward-compatible API: now uses single-pass implementation."""
        _file_info, symbols = self.index_file_with_symbols(file_path)
        return symbols
    
    def _path_to_module(self, file_path: str) -> str:
        """Convert file path to module name."""
        # Remove .py extension
        if file_path.endswith(".py"):
            file_path = file_path[:-3]
        
        # Normalize path separators
        parts = file_path.replace(os.sep, ".").split(".")
        
        # Remove __init__ from module name
        if parts and parts[-1] == "__init__":
            parts = parts[:-1]
        
        return ".".join(parts) if parts else ""
    
    def _extract_imports(self, tree: ast.AST) -> List[str]:
        """Extract import statements from AST."""
        imports = []
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                for alias in node.names:
                    imports.append(alias.name)
            elif isinstance(node, ast.ImportFrom):
                if node.module:
                    imports.append(node.module)
        return sorted(set(imports))
    
    def _is_entrypoint_function(self, node: ast.FunctionDef, content: str, line_num: int) -> bool:
        """Check if function is an entrypoint (CLI, main, etc.)."""
        # Check for common patterns
        func_name = node.name.lower()
        if func_name in ["main", "cli", "entrypoint"]:
            return True
        
        # Check for argparse/click/typer patterns in function body
        func_lines = content.split("\n")[line_num-1:line_num+20]
        func_text = "\n".join(func_lines)
        
        if any(keyword in func_text for keyword in ["argparse", "click", "typer", "ArgumentParser", "Command"]):
            return True
        
        return False
    
    def _has_main_block(self, tree: ast.AST, content: str) -> bool:
        """Check if file has __main__ block."""
        return 'if __name__ == "__main__"' in content
    
    def _find_main_block_lines(self, content: str) -> Tuple[Optional[int], Optional[int]]:
        """Find line numbers for __main__ block."""
        lines = content.split("\n")
        for i, line in enumerate(lines, 1):
            if 'if __name__ == "__main__"' in line:
                # Find the end of this block (next dedent or end of file)
                indent_level = len(line) - len(line.lstrip())
                start_line = i
                end_line = start_line
                for j in range(i, len(lines)):
                    if lines[j].strip() and len(lines[j]) - len(lines[j].lstrip()) <= indent_level:
                        if j > i:
                            end_line = j - 1
                            break
                else:
                    end_line = len(lines)
                return start_line, end_line
        return None, None
    
    # NOTE: We intentionally removed per-symbol import resolution helpers that caused
    # quadratic behavior (AST-walking the entire module for each symbol). We now attach
    # file-level imports as a best-effort "imports_used" list for each symbol.


class RepoIndexer:
    """Repository indexer with plugin interface for multiple languages."""
    
    def __init__(self, repo_root: str, config: Optional[Dict[str, Any]] = None):
        self.repo_root = os.path.abspath(repo_root)
        self.config = config or {}
        
        # Plugin registry (Python-only for now, extensible)
        self.indexers = {
            "python": PythonIndexer(self.repo_root)
        }
        
        # Default config
        self.include_globs = self.config.get("include_globs", ["**/*.py"])
        self.exclude_globs = self.config.get("exclude_globs", [
            "**/venv/**",
            "**/__pycache__/**",
            "**/.git/**",
            "**/.remoroo/**",
            "**/.remoroo_venvs/**"
        ])
        self.max_file_bytes = self.config.get("max_file_bytes", 2000000)
        # Centralized deny/exclude policy (configs.py) for consistent repo hygiene.
        self.deny_paths = list(self.config.get("deny_paths", DEFAULT_DENY_PATHS))
        self.excluded_dirs = set(self.config.get("excluded_dirs", DEFAULT_EXCLUDED_DIRS))
        self.excluded_files = set(self.config.get("excluded_files", DEFAULT_EXCLUDED_FILES))
    
    def index(self, force: bool = False) -> Dict[str, Any]:
        """
        Build or update repo_index.json.
        
        Args:
            force: If True, force full re-index. If False, load existing and update incrementally.
        
        Returns:
            Complete repo_index dictionary
        """
        index_path = self._get_index_path()
        index_meta_path = self._get_index_meta_path()
        
        # Load existing index if available and not forcing
        existing_index = None
        if not force and os.path.exists(index_path):
            try:
                with open(index_path, "r") as f:
                    existing_index = json.load(f)
                print(f"  📇 Loading existing index from {index_path}")
            except (json.JSONDecodeError, IOError):
                existing_index = None
        
        # Find all Python files
        python_files = self._find_python_files()
        
        if existing_index and not force:
            # Incremental update: only index changed files
            # Prefer mtime+size for fast change detection; fall back to sha256 if missing.
            existing_files = {}
            for f in existing_index.get("files", []) or []:
                if not isinstance(f, dict) or "path" not in f:
                    continue
                existing_files[f["path"]] = {
                    "sha256": f.get("sha256", ""),
                    "mtime": f.get("mtime", None),
                    "size_bytes": f.get("size_bytes", None)
                }
            changed_files = []
            removed_files: List[str] = []

            # Drop any files that are no longer present in the repo file set.
            # This is critical when exclusion rules change (e.g., excluding .remoroo_venvs/)
            # so the index doesn't keep stale vendor/venv files forever.
            current_set = set(python_files)
            for old_path in list(existing_files.keys()):
                if old_path not in current_set:
                    removed_files.append(old_path)
            
            for file_path in python_files:
                abs_path = os.path.join(self.repo_root, file_path)
                size_bytes, mtime = _safe_stat(abs_path)
                prev = existing_files.get(file_path)
                if not prev:
                    changed_files.append(file_path)
                    continue
                prev_mtime = prev.get("mtime")
                prev_size = prev.get("size_bytes")
                if prev_mtime is not None and prev_size is not None:
                    if float(prev_mtime) != float(mtime) or int(prev_size) != int(size_bytes):
                        changed_files.append(file_path)
                    continue
                # Fallback to sha256 compare if older index doesn't have mtime/size
                with open(abs_path, "rb") as f:
                    raw = f.read()
                current_hash = _compute_sha256_bytes(raw) if raw else ""
                if prev.get("sha256") != current_hash:
                    changed_files.append(file_path)
            
            if not changed_files and not removed_files:
                print(f"  ✅ Index up to date (no changes)")
                return existing_index
            
            if removed_files:
                print(f"  🧹 Removing {len(removed_files)} stale file(s) from index")
            if changed_files:
                print(f"  📇 Updating index for {len(changed_files)} changed file(s)")
            return self._update_index(existing_index, changed_files, removed_files)
        else:
            # Full index
            print(f"  📇 Building full index for {len(python_files)} file(s)")
            return self._build_full_index(python_files)
    
    def update_index(self, changed_files: List[str]) -> Dict[str, Any]:
        """
        Incremental update: only re-index changed files.
        
        Args:
            changed_files: List of file paths (relative to repo_root) that changed
        
        Returns:
            Updated repo_index
        """
        index_path = self._get_index_path()
        
        if not os.path.exists(index_path):
            # No existing index, do full index
            return self.index(force=True)
        
        with open(index_path, "r") as f:
            existing_index = json.load(f)
        
        return self._update_index(existing_index, changed_files, removed_files=[])
    
    def _build_full_index(self, python_files: List[str]) -> Dict[str, Any]:
        """Build complete index from scratch."""
        files = []
        all_symbols = []
        
        for file_path in python_files:
            try:
                file_info, symbols = self.indexers["python"].index_file_with_symbols(file_path)
                files.append(file_info)
                all_symbols.extend(symbols)
            except Exception as e:
                print(f"  ⚠️  Error indexing {file_path}: {e}")
                continue
        
        # Build dependency graph
        graph = self._build_dependency_graph(files, all_symbols)
        
        # Extract entrypoints
        entrypoints = self._extract_entrypoints(files)
        
        # Build index
        repo_index = {
            "schema_version": "1.0",
            "repo": {
                "root": self.repo_root,
                "name": os.path.basename(self.repo_root),
                "default_branch": self._get_default_branch(),
                "vcs": self._get_vcs_info()
            },
            "build": {
                "created_at": datetime.utcnow().isoformat() + "Z",
                "indexer": {"name": "remoroo-indexer", "version": "0.1.0"},
                "languages": ["python"],
                "config": {
                    "include_globs": self.include_globs,
                    "exclude_globs": self.exclude_globs,
                    "max_file_bytes": self.max_file_bytes
                }
            },
            "files": files,
            "symbols": all_symbols,
            "graph": graph,
            "entrypoints": entrypoints
        }
        
        # Save index
        self._save_index(repo_index)
        
        return repo_index
    
    def _update_index(
        self,
        existing_index: Dict[str, Any],
        changed_files: List[str],
        removed_files: List[str],
    ) -> Dict[str, Any]:
        """Update existing index with changed files and drop removed files."""
        # Remove old entries for changed files
        existing_files = {f["path"]: i for i, f in enumerate(existing_index.get("files", []))}
        existing_symbols_by_file = {}
        
        for i, symbol in enumerate(existing_index.get("symbols", [])):
            file_path = symbol.get("file_path")
            if file_path not in existing_symbols_by_file:
                existing_symbols_by_file[file_path] = []
            existing_symbols_by_file[file_path].append(i)
        
        # Remove old file entries
        files_to_remove = []
        symbols_to_remove = []
        for file_path in list(changed_files or []) + list(removed_files or []):
            if file_path in existing_files:
                files_to_remove.append(existing_files[file_path])
            if file_path in existing_symbols_by_file:
                symbols_to_remove.extend(existing_symbols_by_file[file_path])
        
        # Remove in reverse order to maintain indices
        for idx in sorted(files_to_remove, reverse=True):
            existing_index["files"].pop(idx)
        for idx in sorted(symbols_to_remove, reverse=True):
            existing_index["symbols"].pop(idx)
        
        # Index changed files
        for file_path in changed_files:
            try:
                file_info, symbols = self.indexers["python"].index_file_with_symbols(file_path)
                existing_index["files"].append(file_info)
                existing_index["symbols"].extend(symbols)
            except Exception as e:
                print(f"  ⚠️  Error indexing {file_path}: {e}")
                continue
        
        # Rebuild graph (only for changed files' dependencies)
        graph = self._build_dependency_graph(existing_index["files"], existing_index["symbols"])
        existing_index["graph"] = graph
        
        # Update entrypoints
        entrypoints = self._extract_entrypoints(existing_index["files"])
        existing_index["entrypoints"] = entrypoints
        
        # Update build metadata
        existing_index["build"]["created_at"] = datetime.utcnow().isoformat() + "Z"
        
        # Save updated index
        self._save_index(existing_index)
        
        return existing_index
    
    def _build_dependency_graph(self, files: List[Dict[str, Any]], symbols: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Build module dependency graph and symbol call graph."""
        # Module dependencies (fast path: avoid O(n^2) scanning)
        module_deps: List[Dict[str, Any]] = []
        module_imports: Dict[str, List[str]] = {}

        # Build module name -> file mapping once
        known_modules: Set[str] = set()
        for file_info in files:
            mod = file_info.get("module", "") or ""
            if mod:
                known_modules.add(mod)
            module_imports[mod] = file_info.get("imports", []) or []

        # Helper: resolve an import string to the closest known module by prefix
        def resolve_import(imp: str) -> Optional[str]:
            if not imp:
                return None
            # Try direct match first
            if imp in known_modules:
                return imp
            # Try progressively shorter prefixes: a.b.c -> a.b -> a
            parts = imp.split(".")
            while len(parts) > 1:
                parts.pop()
                cand = ".".join(parts)
                if cand in known_modules:
                    return cand
            return None

        dep_counts: Dict[Tuple[str, str], int] = {}
        for module, imports in module_imports.items():
            if not module:
                continue
            for imp in imports:
                resolved = resolve_import(imp)
                if not resolved:
                    continue
                if resolved == module:
                    continue
                key = (module, resolved)
                dep_counts[key] = dep_counts.get(key, 0) + 1

        module_deps = [{"from": f, "to": t, "count": c} for (f, t), c in dep_counts.items()]
        
        # Symbol calls (simplified - would need more sophisticated analysis)
        symbol_calls = []
        # TODO: Implement call graph analysis (requires more complex AST traversal)
        
        return {
            "module_deps": module_deps,
            "symbol_calls": symbol_calls
        }
    
    def _extract_entrypoints(self, files: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Extract entrypoints from files."""
        entrypoints = []
        
        # If a file has a __main__ block, prefer that as the canonical entrypoint.
        # Many single-file scripts define a `main()` function AND call it under
        # `if __name__ == "__main__": main()`. In those cases, generating a
        # "python_function" entrypoint like `python -m <module>.main` is misleading:
        # `python -m` expects an importable module/package layout, not a plain file.
        files_with_dunder_main = set()
        for file_info in files:
            file_path = file_info.get("path", "")
            for ep in file_info.get("entrypoints", []) or []:
                if ep.get("kind") == "__main__":
                    files_with_dunder_main.add(file_path)
        
        for file_info in files:
            file_path = file_info.get("path", "")
            module = file_info.get("module", "")
            
            for ep in file_info.get("entrypoints", []):
                if ep["kind"] == "__main__":
                    # If this is a package __main__.py, prefer `python -m <package>`
                    how = f"python {file_path}"
                    if isinstance(module, str) and module.endswith(".__main__"):
                        how = f"python -m {module[: -len('.__main__')]}"
                    elif isinstance(file_path, str) and file_path.endswith("__main__.py"):
                        pkg = file_path.replace(os.sep, "/")
                        pkg = pkg[: -len("/__main__.py")] if pkg.endswith("/__main__.py") else pkg
                        pkg_mod = pkg.replace("/", ".").strip(".")
                        if pkg_mod:
                            how = f"python -m {pkg_mod}"
                    entrypoints.append({
                        "kind": "python_main",
                        "file_path": file_path,
                        "symbol_id": None,  # __main__ doesn't have a symbol
                        "how_to_run": how
                    })
                elif ep["kind"] == "function":
                    # If the file has a __main__ entrypoint, don't emit a separate
                    # function entrypoint. This avoids suggesting invalid commands
                    # like `python -m file.main` for single-file scripts.
                    if file_path in files_with_dunder_main:
                        continue
                    func_name = ep.get("name", "")
                    symbol_id = f"sym:{module}:{func_name}#L{ep['line_start']}"
                    if module:
                        # Prefer a python -c wrapper; this is robust even when `python -m` is ambiguous.
                        how = f"python -c \"from {module} import {func_name}; {func_name}()\""
                    else:
                        how = f"python {file_path}"
                    entrypoints.append({
                        "kind": "python_function",
                        "file_path": file_path,
                        "symbol_id": symbol_id,
                        "how_to_run": how
                    })
        
        return entrypoints
    
    def _find_python_files(self) -> List[str]:
        """Find all Python files in repository."""
        python_files = []
        
        for root, dirs, files in os.walk(self.repo_root):
            # Skip excluded directories early (fast path).
            # - `excluded_dirs`: directory basenames to prune during os.walk
            # - `deny_paths`: prefix paths to prune (e.g., ".remoroo_venvs/")
            pruned: List[str] = []
            for d in list(dirs):
                if d in self.excluded_dirs:
                    dirs.remove(d)
                    continue
                abs_d = os.path.join(root, d)
                rel_d = os.path.relpath(abs_d, self.repo_root).replace(os.sep, "/")
                # Ensure dirs are checked as prefixes with trailing "/"
                if self._is_denied_path(rel_d + "/"):
                    dirs.remove(d)
                    continue
                # Back-compat: also honor exclude_globs
                if any(self._matches_glob(rel_d + "/", pattern) for pattern in self.exclude_globs):
                    dirs.remove(d)
                    continue
            
            for filename in files:
                if filename.startswith('.') or filename.startswith('__') or filename in self.excluded_files:
                    continue
                if filename.endswith(".py"):
                    file_path = os.path.join(root, filename)
                    rel_path = os.path.relpath(file_path, self.repo_root)
                    rel_posix = rel_path.replace(os.sep, "/")
                    
                    # Check exclude patterns
                    if self._is_denied_path(rel_posix):
                        continue
                    if any(self._matches_glob(rel_posix, pattern) for pattern in self.exclude_globs):
                        continue
                    
                    # Check file size
                    if os.path.getsize(file_path) > self.max_file_bytes:
                        continue
                    
                    python_files.append(rel_posix)
        
        return sorted(python_files)
    
    def _matches_glob(self, path: str, pattern: str) -> bool:
        """
        Glob matching for repo-relative POSIX paths.

        IMPORTANT: Do not "simplify" patterns by stripping '**' — that breaks
        patterns like '**/.remoroo_venvs/**' and causes vendor/venv code to leak
        into the index, which can explode LLM payloads.
        """
        try:
            from pathlib import PurePosixPath

            p = PurePosixPath((path or "").replace("\\", "/"))
            pat = (pattern or "").replace("\\", "/").lstrip("/")
            return p.match(pat)
        except Exception:
            # Conservative fallback
            return False

    def _is_denied_path(self, rel_posix_path: str) -> bool:
        """Return True if path is denied by prefix policy."""
        p = (rel_posix_path or "").replace("\\", "/")
        for pref in self.deny_paths or []:
            if not isinstance(pref, str) or not pref:
                continue
            pref_norm = pref.replace("\\", "/")
            if p.startswith(pref_norm):
                return True
        return False
    
    def _get_index_path(self) -> str:
        """Get path to repo_index.json in .remoroo/."""
        remoroo_dir = os.path.join(self.repo_root, ".remoroo")
        os.makedirs(remoroo_dir, exist_ok=True)
        return os.path.join(remoroo_dir, "repo_index.json")
    
    def _get_index_meta_path(self) -> str:
        """Get path to repo_index.meta.json in .remoroo/."""
        remoroo_dir = os.path.join(self.repo_root, ".remoroo")
        os.makedirs(remoroo_dir, exist_ok=True)
        return os.path.join(remoroo_dir, "repo_index.meta.json")
    
    def _get_default_branch(self) -> str:
        """Get default git branch if available."""
        try:
            import subprocess
            result = subprocess.run(
                ["git", "symbolic-ref", "refs/remotes/origin/HEAD"],
                cwd=self.repo_root,
                capture_output=True,
                text=True,
                timeout=5
            )
            if result.returncode == 0:
                return result.stdout.strip().split("/")[-1]
        except:
            pass
        return "main"
    
    def _get_vcs_info(self) -> Dict[str, Any]:
        """Get VCS information if available."""
        try:
            import subprocess
            result = subprocess.run(
                ["git", "rev-parse", "HEAD"],
                cwd=self.repo_root,
                capture_output=True,
                text=True,
                timeout=5
            )
            if result.returncode == 0:
                return {
                    "type": "git",
                    "head_commit": result.stdout.strip()
                }
        except:
            pass
        return {"type": "none"}
    
    def _save_index(self, repo_index: Dict[str, Any]) -> None:
        """Save index to .remoroo/repo_index.json and metadata."""
        index_path = self._get_index_path()
        meta_path = self._get_index_meta_path()
        
        # Save main index
        with open(index_path, "w") as f:
            json.dump(repo_index, f, indent=2)
        
        # Save metadata
        meta = {
            "index_path": index_path,
            "indexed_at": datetime.utcnow().isoformat() + "Z",
            "file_count": len(repo_index.get("files", [])),
            "symbol_count": len(repo_index.get("symbols", [])),
            "entrypoint_count": len(repo_index.get("entrypoints", []))
        }
        
        with open(meta_path, "w") as f:
            json.dump(meta, f, indent=2)
        
        print(f"  ✅ Index saved to {index_path}")

