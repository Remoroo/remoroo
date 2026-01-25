"""
Environment Doctor LLM - Ensures repository environment is healthy before experiment.

This module provides a dedicated LLM stage that runs BEFORE the main experiment loop.
Its sole job is to:
1. Detect if the environment is broken
2. Diagnose the issue
3. Fix it with targeted commands
4. Verify the fix worked

Only after the environment is healthy do we proceed to Planner → Patcher → Validator → Judge.
"""

from __future__ import annotations
import os
import re
import json
import time
from dataclasses import dataclass, field
from typing import List, Dict, Any, Optional, Tuple
from pathlib import Path

import sys
import shutil
import requests
from ..engine.core.executor import run_command_with_timeout
from ..engine.core.env_setup import (
    EnvSetupResult,
    execute_env_setup,
    detect_requirements_files,
    detect_needs_editable_install
)


@dataclass
class EnvDoctorResult:
    """Result of Environment Doctor healing process."""
    healthy: bool
    diagnosis: str
    commands_run: List[Dict[str, Any]] = field(default_factory=list)
    iterations: int = 0
    final_error: Optional[str] = None
    smoke_test_output: Optional[str] = None
    total_duration_s: float = 0.0


@dataclass 
class EnvDiagnosis:
    """Diagnosis from Env Doctor LLM."""
    diagnosis: str
    fix_commands: List[str]
    confidence: str  # high, medium, low
    is_unfixable: bool = False
    unfixable_reason: Optional[str] = None


class EnvDoctor:
    """
    Environment Doctor - heals broken Python environments.
    
    Flow:
    1. Run smoke test (import or simple command)
    2. If fails, call Env Doctor LLM with minimal context
    3. Execute suggested fix commands
    4. Retry smoke test
    5. Repeat until healthy or max iterations
    """
    
    MAX_ITERATIONS = 3
    SMOKE_TEST_TIMEOUT = 30.0
    FIX_COMMAND_TIMEOUT = 120.0
    PYPI_TIMEOUT = 5.0
    
    # Static list of stdlib modules for Python < 3.10
    _STDLIB_FALLBACK = {
        "abc", "argparse", "ast", "asynchat", "asyncio", "asyncore", "base64", "bdb", "binascii", "binhex",
        "bisect", "builtins", "bz2", "calendar", "cgi", "cgitb", "chunk", "cmath", "cmd", "code", "codecs",
        "codeop", "collections", "colorsys", "compileall", "concurrent", "configparser", "contextlib",
        "contextvars", "copy", "copyreg", "crypt", "csv", "ctypes", "curses", "dataclasses", "datetime",
        "dbm", "decimal", "difflib", "dis", "distutils", "doctest", "dummy_threading", "email", "encodings",
        "ensurepip", "enum", "errno", "faulthandler", "filecmp", "fileinput", "fnmatch", "formatter",
        "fpectl", "fractions", "ftplib", "functools", "gc", "getopt", "getpass", "gettext", "glob", "grp",
        "gzip", "hashlib", "heapq", "hmac", "html", "http", "imaplib", "imghdr", "imp", "importlib", "inspect",
        "io", "ipaddress", "itertools", "json", "keyword", "lib2to3", "linecache", "locale", "logging",
        "lzma", "mailbox", "mailcap", "marshal", "math", "mimetypes", "mmap", "modulefinder", "msilib",
        "msvcrt", "multiprocessing", "netrc", "nis", "nntplib", "ntpath", "numbers", "operator", "optparse",
        "os", "ossaudiodev", "parser", "pathlib", "pdb", "pickle", "pickletools", "pipes", "pkgutil",
        "platform", "plistlib", "poplib", "posix", "posixpath", "pprint", "profile", "pstats", "pty", "pwd",
        "py_compile", "pyclbr", "pydoc", "queue", "quopri", "random", "re", "readline", "reprlib", "resource",
        "rlcompleter", "runpy", "sched", "secrets", "select", "selectors", "shelve", "shlex", "shutil",
        "signal", "site", "smtpd", "smtplib", "sndhdr", "socket", "socketserver", "spwd", "sqlite3", "ssl",
        "stat", "statistics", "string", "stringprep", "struct", "subprocess", "sunau", "symbol", "symtable",
        "sys", "sysconfig", "syslog", "tabnanny", "tarfile", "telnetlib", "tempfile", "termios", "test",
        "textwrap", "threading", "time", "timeit", "tkinter", "token", "tokenize", "trace", "traceback",
        "tracemalloc", "tty", "types", "typing", "unicodedata", "unittest", "urllib", "uu", "uuid", "venv",
        "warnings", "wave", "weakref", "webbrowser", "winreg", "winsound", "wsgiref", "xdrlib", "xml",
        "xmlrpc", "zipapp", "zipfile", "zipimport", "zlib"
    }
    
    
    def __init__(
        self,
        repo_root: str,
        artifact_dir: str,
        venv_python: Optional[str] = None,
        packages_to_install: Optional[List[str]] = None,  # Explicit packages passed by caller
        # Callbacks to intelligence layer (optional - if not provided, LLM features disabled)
        analyze_imports_callback: Optional[Any] = None,  # Callable[[Dict], ImportAnalysisResult]
        diagnose_error_callback: Optional[Any] = None,   # Callable[[Dict], DiagnosisResult]
    ):
        self.repo_root = repo_root
        self.artifact_dir = artifact_dir
        self.venv_python = venv_python or "python"
        self.packages_to_install = packages_to_install or []
        self.analyze_imports_callback = analyze_imports_callback
        self.diagnose_error_callback = diagnose_error_callback
        self.commands_tried: List[str] = []

    def _read_repo_file(self, rel_path: str) -> Optional[str]:
        """Read file from locally mounted repo (content or None)."""
        path = os.path.join(self.repo_root, rel_path)
        if os.path.exists(path):
            try:
                with open(path, "r", encoding="utf-8", errors="replace") as f:
                    return f.read()
            except Exception:
                pass
        return None

    @staticmethod
    def _normalize_pkg_name(name: str) -> str:
        """Normalize package names for loose matching (PEP 503-ish)."""
        if not name:
            return ""
        return re.sub(r"[-_.]+", "-", name.strip().lower())

    @classmethod
    def is_stdlib(cls, module_name: str) -> bool:
        """Check if a module is part of the Python Standard Library."""
        if not module_name:
            return False
        # Use sys.stdlib_module_names if available (Python 3.10+)
        if hasattr(sys, "stdlib_module_names"):
            if module_name in sys.stdlib_module_names:
                return True
        # Fallback to static list
        return module_name in cls._STDLIB_FALLBACK

    def _detect_local_modules(self) -> List[str]:
        """Detect local files and directories that could be imported."""
        local_modules = []
        try:
            for item in os.listdir(self.repo_root):
                if item.endswith('.py'):
                    local_modules.append(item[:-3])  # Remove .py extension
                elif os.path.isdir(os.path.join(self.repo_root, item)) and not item.startswith('.'):
                    local_modules.append(item)
        except Exception:
            pass
        return local_modules

    def verify_pypi_package(self, pkg_name: str) -> bool:
        """Verify if a package exists on PyPI."""
        if not pkg_name:
            return False
        # Normalize to avoid common mishaps
        norm_name = self._normalize_pkg_name(pkg_name)
        try:
            url = f"https://pypi.org/pypi/{norm_name}/json"
            response = requests.get(url, timeout=self.PYPI_TIMEOUT)
            return response.status_code == 200
        except Exception as e:
            print(f"  ⚠️  PyPI verification failed for {pkg_name}: {e}")
            # Err on the side of caution - assume it exists if PyPI is unreachable
            return True

    @classmethod
    def _extract_requirement_names(cls, requirements_txt: str) -> List[str]:
        """Extract base package names from requirements.txt content (best-effort).

        Handles lines like:
        - pytest>=7.0
        - pytest-cov==4.1
        - requests[socks]>=2.0
        - -r other.txt / --find-links ... (ignored)
        - git+https://...#egg=package (captures egg)
        """
        names: List[str] = []
        if not requirements_txt:
            return names
        for raw in requirements_txt.splitlines():
            line = raw.strip()
            if not line or line.startswith("#"):
                continue
            # Skip directives
            if line.startswith(("-", "--")):
                # Try to capture egg name from VCS lines
                m_egg = re.search(r"[#&]egg=([A-Za-z0-9_.-]+)", line)
                if m_egg:
                    names.append(cls._normalize_pkg_name(m_egg.group(1)))
                continue
            # Remove inline comments
            if " #" in line:
                line = line.split(" #", 1)[0].strip()
            # Split off environment markers
            if ";" in line:
                line = line.split(";", 1)[0].strip()
            # Strip extras: package[extra]
            base = line.split("[", 1)[0].strip()
            # Split at version/operator (==,>=,<=,~=,!=,>,<)
            base = re.split(r"(==|>=|<=|~=|!=|>|<)", base, maxsplit=1)[0].strip()
            if base:
                names.append(cls._normalize_pkg_name(base))
        return names

    @classmethod
    def _filter_packages_already_required(cls, packages: List[str], requirements_txt: str) -> List[str]:
        """Remove packages that appear to already be present in requirements.txt."""
        if not packages:
            return []
        req_names = set(cls._extract_requirement_names(requirements_txt))
        filtered: List[str] = []
        for pkg in packages:
            if not pkg or not isinstance(pkg, str):
                continue
            norm = cls._normalize_pkg_name(pkg)
            if norm in req_names:
                continue
            filtered.append(pkg)
        return filtered
    
    def heal(self, smoke_test_cmd: Optional[str] = None) -> EnvDoctorResult:
        """
        Iteratively fix environment until healthy or max iterations.
        
        Args:
            smoke_test_cmd: Command to test if env is healthy.
                           If None, will be inferred from repo structure.
        
        Returns:
            EnvDoctorResult with healing outcome
        """
        start_time = time.time()
        
        # Infer smoke test if not provided
        if smoke_test_cmd is None:
            smoke_test_cmd = self._infer_smoke_test()
        
        print(f"\n{'='*70}")
        print("🏥 ENVIRONMENT DOCTOR")
        print(f"{'='*70}")
        print(f"  Smoke test: {smoke_test_cmd}")
        
        all_commands_run = []
        
        # Step 0: Run initial installation (auto-detected requirements)
        # This is the ONLY place where initial installs happen
        initial_install_commands = self._get_initial_install_commands()
        if initial_install_commands:
            print(f"\n  📦 Running initial installation ({len(initial_install_commands)} command(s))...")
            for cmd in initial_install_commands:
                print(f"    ▶️  {cmd}")
                self.commands_tried.append(cmd)
                outcome = self._run_fix_command(cmd)
                all_commands_run.append(outcome)
                if outcome.get("exit_code", 0) == 0:
                    print(f"    ✅ Succeeded")
                else:
                    print(f"    ⚠️  Failed (exit {outcome.get('exit_code')}) - will retry in healing loop")
        
        for iteration in range(self.MAX_ITERATIONS):
            print(f"\n  📋 Iteration {iteration + 1}/{self.MAX_ITERATIONS}")
            
            # Step 1: Run smoke test
            success, error_output = self._run_smoke_test(smoke_test_cmd)
            
            if success:
                print(f"  ✅ Environment is healthy!")
                return EnvDoctorResult(
                    healthy=True,
                    diagnosis="Environment passed smoke test",
                    commands_run=all_commands_run,
                    iterations=iteration + 1,
                    smoke_test_output=error_output,
                    total_duration_s=time.time() - start_time
                )
            
            print(f"  ❌ Smoke test failed")
            
            # Step 2: Call Env Doctor LLM
            diagnosis = self._call_env_doctor_llm(error_output)
            
            if diagnosis.is_unfixable:
                print(f"  🚫 Environment is unfixable: {diagnosis.unfixable_reason}")
                return EnvDoctorResult(
                    healthy=False,
                    diagnosis=diagnosis.diagnosis,
                    commands_run=all_commands_run,
                    iterations=iteration + 1,
                    final_error=diagnosis.unfixable_reason,
                    smoke_test_output=error_output,
                    total_duration_s=time.time() - start_time
                )
            
            print(f"  🔍 Diagnosis: {diagnosis.diagnosis}")
            print(f"  💊 Fix commands: {diagnosis.fix_commands}")
            
            # Step 3: Execute fix commands
            fix_commands = self._filter_pip_install_commands(diagnosis.fix_commands)
            
            for cmd in fix_commands:
                if cmd in self.commands_tried:
                    print(f"  ⏭️  Skipping (already tried): {cmd}")
                    continue
                
                self.commands_tried.append(cmd)
                print(f"  ▶️  Running: {cmd}")
                
                outcome = self._run_fix_command(cmd)
                all_commands_run.append(outcome)
                
                if outcome.get("exit_code", 0) != 0:
                    print(f"  ⚠️  Command failed (exit {outcome.get('exit_code')})")
                else:
                    print(f"  ✅ Command succeeded")
        
        # Max iterations reached
        success, final_error = self._run_smoke_test(smoke_test_cmd)
        
        return EnvDoctorResult(
            healthy=success,
            diagnosis="Max iterations reached" if not success else "Environment healed",
            commands_run=all_commands_run,
            iterations=self.MAX_ITERATIONS,
            final_error=final_error if not success else None,
            smoke_test_output=final_error,
            total_duration_s=time.time() - start_time
        )
    
    def _infer_smoke_test(self) -> str:
        """Infer a simple smoke test command based on repo structure."""
        
        # Try to find the main package name
        pkg_name = self._find_package_name()
        
        if pkg_name:
            # Rewrite python to use venv_python
            return f"{self.venv_python} -c \"import {pkg_name}\""
        
        # Check for common entry points
        for entry in ["main.py", "app.py", "run.py", "__main__.py"]:
            if os.path.exists(os.path.join(self.repo_root, entry)):
                return f"{self.venv_python} -c \"import importlib.util; spec = importlib.util.spec_from_file_location('m', '{entry}'); print('ok')\""
        
        # Fallback: just check python works with basic imports
        return f"{self.venv_python} -c \"import sys; print('Python', sys.version)\""
    
    def _find_package_name(self) -> Optional[str]:
        """Find the main package name from repo structure.
        
        PRIORITY ORDER:
        1. Package directories with __init__.py (the ACTUAL importable name)
        2. setup.py name (fallback - pip package name may differ from import name)
        3. pyproject.toml name (fallback)
        
        This order is important because setup.py might have name="foo-bar" 
        but the importable package is actually "foo" (the directory name).
        """
        
        # PRIORITY 1: Look for directories that ARE packages (have __init__.py)
        # This is the ACTUAL importable name, which may differ from setup.py name
        for item in os.listdir(self.repo_root):
            item_path = os.path.join(self.repo_root, item)
            if os.path.isdir(item_path) and not item.startswith(".") and not item.startswith("_"):
                init_path = os.path.join(item_path, "__init__.py")
                if os.path.exists(init_path):
                    # Skip common non-package directories
                    if item not in ["tests", "test", "docs", "examples", "scripts", "venv", "env", "build", "dist", ".remoroo_venvs"]:
                        return item
        
        # PRIORITY 2: Check setup.py
        content = self._read_repo_file("setup.py")
        if content:
            match = re.search(r'name\s*=\s*["\']([^"\']+)["\']', content)
            if match:
                return match.group(1).replace("-", "_")
        
        # PRIORITY 3: Check pyproject.toml
        content = self._read_repo_file("pyproject.toml")
        if content:
            # Try PEP 621 format
            match = re.search(r'\[project\].*?name\s*=\s*["\']([^"\']+)["\']', content, re.DOTALL)
            if match:
                return match.group(1).replace("-", "_")
            # Try Poetry format
            match = re.search(r'\[tool\.poetry\].*?name\s*=\s*["\']([^"\']+)["\']', content, re.DOTALL)
            if match:
                return match.group(1).replace("-", "_")
        
        return None
    
    def _get_initial_install_commands(self) -> List[str]:
        """Get initial installation commands based on repo structure + code analysis.
        
        Auto-detects (in order):
        1. requirements.txt -> pip install -r requirements.txt
        2. setup.py / pyproject.toml -> pip install -e .
        3. Code imports (via LLM) -> pip install detected packages
        4. Goal-inferred packages (via LLM) -> pip install goal-specific packages
        """
        commands = []
        
        # Check for requirements.txt
        req_path = os.path.join(self.repo_root, "requirements.txt")
        if os.path.exists(req_path):
            commands.append("pip install -r requirements.txt")
        
        # Check for requirements-dev.txt
        req_dev_path = os.path.join(self.repo_root, "requirements-dev.txt")
        if os.path.exists(req_dev_path):
            commands.append("pip install -r requirements-dev.txt")
        
        # Check for setup.py or pyproject.toml (needs editable install)
        setup_path = os.path.join(self.repo_root, "setup.py")
        pyproject_path = os.path.join(self.repo_root, "pyproject.toml")
        if os.path.exists(setup_path) or os.path.exists(pyproject_path):
            commands.append("pip install -e .")
        
        # Detect packages from code imports (LLM-based)
        import_packages = self._detect_imports_from_code()
        commands.extend(import_packages)
        
        # Install explicit packages passed by caller (inferred by Brain)
        explicit_commands = []
        for pkg_name in self.packages_to_install:
            explicit_commands.append(f"pip install {pkg_name}")
            
        # Filter all current commands for stdlib/local
        commands = self._filter_pip_install_commands(commands)
        commands.extend(self._filter_pip_install_commands(explicit_commands))
        
        # Deduplicate while preserving order
        seen = set()
        final_commands = []
        for cmd in commands:
            if cmd not in seen:
                final_commands.append(cmd)
                seen.add(cmd)
        
        commands = final_commands

        # Safety net: ensure numpy is present for most repos unless explicitly pinned already.
        # Unit tests rely on this default behavior.
        req_text = ""
        try:
             content = self._read_repo_file("requirements.txt")
             if content:
                 req_text = content.lower()
        except Exception:
            req_text = ""
        has_numpy_in_requirements = "numpy" in req_text
        has_numpy_install_cmd = any(isinstance(c, str) and "pip install" in c and "numpy" in c.lower() for c in commands)
        if not has_numpy_in_requirements and not has_numpy_install_cmd:
            commands.append("pip install numpy")
        
        return commands
    

    
    def scan_import_context(self) -> Dict[str, Any]:
        """
        Scan repository for import statements and gather context for LLM analysis.
        
        Returns:
            Dict containing detected imports, code samples, local modules, etc.
        """
        # Get local modules (files/dirs in repo that are NOT packages to install)
        local_modules = self._detect_local_modules()
        local_modules_set = set(local_modules)

        def _top_module_from_import_line(s: str) -> Optional[str]:
            s = s.strip()
            if s.startswith("import "):
                rest = s[len("import "):].strip()
                if not rest:
                    return None
                name = rest.split()[0].split(",")[0].split(".")[0]
                return name if re.match(r"^[A-Za-z_][A-Za-z0-9_]*$", name) else None
            if s.startswith("from "):
                # Only treat as a real import if it has " import " (avoid doc text like "from the solution")
                if " import " not in s:
                    return None
                rest = s[len("from "):].strip()
                if rest.startswith("."):
                    return None  # relative import => local
                mod = rest.split()[0].split(".")[0]
                return mod if re.match(r"^[A-Za-z_][A-Za-z0-9_]*$", mod) else None
            return None

        # IMPORTANT: For large repos, scan the whole repo for import *lines* (cheap, line-based),
        # but filter out local-module subimports to avoid huge noisy payloads (e.g. roboticstoolbox.*).
        detected_imports_set: set = set()
        module_to_examples: Dict[str, List[str]] = {}
        module_to_count: Dict[str, int] = {}
        MAX_UNIQUE_IMPORTS = 400
        MAX_IMPORT_EXAMPLES_PER_MODULE = 3

        # Optional bounded code samples for disambiguation (keep tiny)
        code_samples: List[str] = []
        MAX_CODE_SAMPLES = 4
        MAX_CHARS_PER_SAMPLE = 1500

        for root, dirs, files in os.walk(self.repo_root):
            # Skip hidden and venv directories
            dirs[:] = [d for d in dirs if not d.startswith('.') and d not in
                      ('venv', 'env', '.venv', '__pycache__', 'node_modules', '.remoroo_venvs', 'build', 'dist')]

            for file in files:
                if not file.endswith('.py'):
                    continue

                filepath = os.path.join(root, file)
                rel_path = os.path.relpath(filepath, self.repo_root)

                # Capture a few small code samples for context
                if len(code_samples) < MAX_CODE_SAMPLES:
                    try:
                        content = self._read_repo_file(rel_path)
                        if content:
                            content = content[:MAX_CHARS_PER_SAMPLE]
                            code_samples.append(f"### {rel_path}\n{content}")
                    except Exception:
                        pass

                try:
                    content = self._read_repo_file(rel_path)
                    if content:
                        for line in content.splitlines():
                            s = line.strip()
                            if not (s.startswith("import ") or s.startswith("from ")):
                                continue
                            top = _top_module_from_import_line(s)
                            if not top:
                                continue
                            # Drop local module imports (including all submodules), e.g. roboticstoolbox.*
                            if top in local_modules_set:
                                continue

                            detected_imports_set.add(top)
                            module_to_count[top] = module_to_count.get(top, 0) + 1
                            ex = module_to_examples.get(top)
                            if ex is None:
                                ex = []
                                module_to_examples[top] = ex
                            if len(ex) < MAX_IMPORT_EXAMPLES_PER_MODULE and s not in ex:
                                ex.append(s)

                            if len(detected_imports_set) >= MAX_UNIQUE_IMPORTS:
                                break
                except Exception:
                    pass

                if len(detected_imports_set) >= MAX_UNIQUE_IMPORTS:
                    break

            if len(detected_imports_set) >= MAX_UNIQUE_IMPORTS:
                break

        detected_imports: List[str] = sorted(detected_imports_set)

        # Build compact, high-signal import evidence grouped by top-level module
        import_lines_out: List[str] = []
        for mod in detected_imports:
            count = module_to_count.get(mod, 0)
            import_lines_out.append(f"# {mod} (occurrences: {count})")
            for ex in module_to_examples.get(mod, [])[:MAX_IMPORT_EXAMPLES_PER_MODULE]:
                import_lines_out.append(f"- {ex}")
        import_lines_str = "\n".join(import_lines_out)
        
        # Get existing requirements
        req_contents = ""
        req_path = os.path.join(self.repo_root, "requirements.txt")
        if os.path.exists(req_path):
            try:
                with open(req_path, "r") as f:
                    req_contents = f.read()[:1000]
            except Exception:
                pass
        
        return {
            "detected_imports": detected_imports,
            "import_lines": import_lines_str,
            "code_samples": "\n\n".join(code_samples),
            "requirements_txt": req_contents,
            "goal": "",
            "local_modules": local_modules
        }

    def _detect_imports_from_code(self) -> List[str]:
        """Use logic or LLM (via callback if available) to detect packages."""
        payload = self.scan_import_context()
        req_contents = payload.get("requirements_txt", "")
        code_samples = payload.get("code_samples", "")
        local_modules = payload.get("local_modules", [])
        
        try:
            # Use callback to analyze imports if available
            if self.analyze_imports_callback:
                print(f"  🔍 Analyzing code for required packages...")
                result = self.analyze_imports_callback(payload)
                packages = result.packages if hasattr(result, 'packages') else []
                reasoning = result.reasoning if hasattr(result, 'reasoning') else ""
            else:
                # No LLM callback available - skip LLM analysis
                packages = []
                reasoning = "LLM analysis skipped (no callback)"
            
            packages = self._filter_packages_already_required(packages, req_contents)
            
            if packages:
                print(f"  📦 Detected from code (LLM): {packages}")
                # Filter out stdlib and local modules
                filtered_packages = []
                for pkg in packages:
                    if not pkg or not isinstance(pkg, str):
                        continue
                    
                    # 1. Stdlib check
                    if self.is_stdlib(pkg):
                        print(f"      ⏭️  Skipping {pkg} (Standard Library)")
                        continue
                        
                    # 2. Local module check
                    if pkg in local_modules:
                        print(f"      ⏭️  Skipping {pkg} (Local Module)")
                        continue
                    
                    # 3. Resolve package name if it's a known import-to-package discrepancy
                    resolved_pkg = self.KNOWN_THIRD_PARTY.get(pkg, pkg)
                    if resolved_pkg != pkg:
                        print(f"      🔄 Resolved {pkg} -> {resolved_pkg}")
                    
                    # 4. PyPI verification (only for "suspicious" or unknown names)
                    # We skip verification for very common packages to save time
                    if resolved_pkg.lower() not in self.KNOWN_THIRD_PARTY.values():
                        print(f"      🔍 Verifying {resolved_pkg} on PyPI...")
                        if not self.verify_pypi_package(resolved_pkg):
                            print(f"      ❌ Skipping {resolved_pkg} (Not found on PyPI)")
                            continue
                            
                    filtered_packages.append(resolved_pkg)
                
                llm_packages = [f"pip install {pkg}" for pkg in filtered_packages]
            else:
                llm_packages = []
            
            # Fallback: regex-based detection for common packages the LLM might miss
            fallback_packages = self._detect_imports_regex(code_samples, req_contents, local_modules)
            
            # Merge results (avoid duplicates)
            all_packages = set(llm_packages)
            for pkg_cmd in fallback_packages:
                if pkg_cmd not in all_packages:
                    all_packages.add(pkg_cmd)
            
            if all_packages:
                return list(all_packages)
            else:
                print(f"  ✅ No additional packages detected from code")
                return []
                
        except Exception as e:
            print(f"  ⚠️  Could not analyze code for packages: {e}")
            # Try regex fallback even if LLM failed
            return self._detect_imports_regex(code_samples, req_contents, local_modules)
    
    # Known third-party packages with their pip install names
    KNOWN_THIRD_PARTY = {
        # Scientific computing
        "numpy": "numpy",
        "np": "numpy",
        "scipy": "scipy",
        "pandas": "pandas",
        "pd": "pandas",
        "matplotlib": "matplotlib",
        "plt": "matplotlib",
        "seaborn": "seaborn",
        "sns": "seaborn",
        "sklearn": "scikit-learn",
        "tensorflow": "tensorflow",
        "tf": "tensorflow",
        "torch": "torch",
        "pytorch": "torch",
        "keras": "keras",
        "cv2": "opencv-python",
        "PIL": "Pillow",
        "Image": "Pillow",
        
        # Web/API
        "requests": "requests",
        "flask": "flask",
        "django": "django",
        "fastapi": "fastapi",
        "aiohttp": "aiohttp",
        "httpx": "httpx",
        "bs4": "beautifulsoup4",
        "BeautifulSoup": "beautifulsoup4",
        
        # Data/Config
        "yaml": "pyyaml",
        "toml": "toml",
        "dotenv": "python-dotenv",
        
        # Testing
        "pytest": "pytest",
        
        # Utils
        "tqdm": "tqdm",
        "rich": "rich",
        "click": "click",
        "pydantic": "pydantic",
        "attrs": "attrs",
        
        # Async
        "asyncio": None,  # stdlib, but listed to avoid false positives
        "aiofiles": "aiofiles",
    }
    
    def _detect_imports_regex(
        self, 
        code_samples: List[str], 
        requirements_txt: str, 
        local_modules: List[str]
    ) -> List[str]:
        """Regex-based fallback detection for common third-party packages.
        
        Acts as a safety net when the LLM misses obvious packages like numpy.
        """
        found_packages = set()
        
        # Parse imports from code samples
        import_pattern = re.compile(r'^(?:import|from)\s+([a-zA-Z_][a-zA-Z0-9_]*)', re.MULTILINE)
        
        full_code = "\n".join(code_samples)
        matches = import_pattern.findall(full_code)
        
        # Check each import against known third-party packages
        for module in matches:
            # 1. Stdlib check
            if self.is_stdlib(module):
                continue
                
            # 2. Local module check
            if module in local_modules:
                continue
            
            pip_name = self.KNOWN_THIRD_PARTY.get(module)
            if pip_name:  # None means stdlib (though we checked above) or skip
                # Check if already in requirements (robust parse)
                req_names = set(self._extract_requirement_names(requirements_txt))
                if self._normalize_pkg_name(pip_name) in req_names:
                    continue
                found_packages.add(pip_name)
        
        if found_packages:
            print(f"  📦 Detected from code (regex fallback): {list(found_packages)}")
            return [f"pip install {pkg}" for pkg in found_packages]
        
        return []

    def _filter_pip_install_commands(self, commands: List[str]) -> List[str]:
        """Filter out pip install commands for standard library or local modules."""
        local_modules = self._detect_local_modules()
        filtered = []
        
        for cmd in commands:
            if not isinstance(cmd, str):
                filtered.append(cmd)
                continue
                
            # Only filter pip install commands
            if not (cmd.startswith("pip install ") or cmd.startswith("pip3 install ")):
                filtered.append(cmd)
                continue
                
            # Extract package name (simple space split, handles pip install pkg==1.0)
            parts = cmd.split()
            if len(parts) < 3:
                filtered.append(cmd)
                continue
                
            # The package name is usually the last part, or the one after 'install'
            # e.g. pip install -r req.txt (skip), pip install pkg (pkg)
            if "-r" in parts or "-e" in parts:
                filtered.append(cmd)
                continue
                
            pkg = parts[-1]
            # Clean version specifiers
            pkg_name = re.split(r'[=<>~!]', pkg)[0]
            
            # 1. Stdlib check
            if self.is_stdlib(pkg_name):
                print(f"      ⏭️  Skipping {cmd} (Standard Library)")
                continue
                
            # 2. Local module check
            if pkg_name in local_modules:
                print(f"      ⏭️  Skipping {cmd} (Local Module)")
                continue
                
            filtered.append(cmd)
            
        return filtered

    
    def _run_smoke_test(self, cmd: str) -> Tuple[bool, str]:
        """Run smoke test command, return (success, output)."""
        outcome = run_command_with_timeout(
            cmd,
            self.repo_root,
            timeout_s=self.SMOKE_TEST_TIMEOUT
        )
        
        success = outcome.get("exit_code", 1) == 0
        output = outcome.get("stderr", "") or outcome.get("stdout", "")
        
        return success, output
    
    def _run_fix_command(self, cmd: str) -> Dict[str, Any]:
        """Run a fix command and return outcome."""
        # Final safety check: if someone tries to install a stdlib, abort here.
        if "pip install " in cmd or "pip3 install " in cmd:
            filtered = self._filter_pip_install_commands([cmd])
            if not filtered:
                return {
                    "command": cmd,
                    "rewritten_command": cmd,
                    "exit_code": 0,
                    "stdout": "Skipped (Standard Library or Local Module)",
                    "stderr": "",
                    "duration_s": 0.0
                }

        # Rewrite python/pip commands to use venv
        rewritten_cmd = self._rewrite_command(cmd)
        
        outcome = run_command_with_timeout(
            rewritten_cmd,
            self.repo_root,
            timeout_s=self.FIX_COMMAND_TIMEOUT
        )
        
        return {
            "command": cmd,
            "rewritten_command": rewritten_cmd,
            "exit_code": outcome.get("exit_code", 1),
            "stdout": outcome.get("stdout", "")[:2000],
            "stderr": outcome.get("stderr", "")[:2000],
            "duration_s": outcome.get("duration_s", 0.0)
        }
    
    def _rewrite_command(self, cmd: str) -> str:
        """Rewrite command to use venv python/pip and properly quote version specifiers."""
        cmd = cmd.strip()
        
        # Quote version specifiers that contain shell metacharacters
        # e.g., "numpy<2" -> '"numpy<2"', "numpy>=1.0,<2" -> '"numpy>=1.0,<2"'
        cmd = self._quote_version_specifiers(cmd)
        
        if cmd.startswith("pip ") or cmd.startswith("pip3 "):
            parts = cmd.split(None, 1)
            if len(parts) > 1:
                return f"{self.venv_python} -m pip {parts[1]}"
            return f"{self.venv_python} -m pip"
        
        if cmd.startswith("python ") or cmd.startswith("python3 "):
            parts = cmd.split(None, 1)
            if len(parts) > 1:
                return f"{self.venv_python} {parts[1]}"
            return self.venv_python
        
        return cmd
    
    def _quote_version_specifiers(self, cmd: str) -> str:
        """Quote package version specifiers to prevent shell interpretation.
        
        Converts: pip install numpy<2 -> pip install 'numpy<2'
        Converts: pip install numpy>=1.0,<2 -> pip install 'numpy>=1.0,<2'
        """
        if not any(c in cmd for c in ['<', '>', '|', '&', ';']):
            return cmd  # No special chars, return as-is
        
        # Split command into parts
        parts = cmd.split()
        quoted_parts = []
        
        for part in parts:
            # If part contains version specifiers (< > etc.) and isn't already quoted
            if any(c in part for c in ['<', '>', '|', '&', ';']) and not (part.startswith('"') or part.startswith("'")):
                # Quote the entire part
                quoted_parts.append(f"'{part}'")
            else:
                quoted_parts.append(part)
        
        return ' '.join(quoted_parts)
    
    def _call_env_doctor_llm(self, error_output: str) -> EnvDiagnosis:
        """Call Env Doctor LLM with minimal context via callback."""
        
        # Build minimal context
        repo_structure = self._get_minimal_repo_structure()
        
        payload = {
            "error_output": error_output[-3000:],  # Last 3000 chars of error
            "repo_structure": repo_structure,
            "commands_already_tried": self.commands_tried[-10:],  # Last 10 commands
            "python_version": self._get_python_version()
        }
        
        # Save payload for debugging
        payload_path = os.path.join(
            self.artifact_dir,
            f"env_doctor_input_{len(self.commands_tried)}.json"
        )
        with open(payload_path, "w") as f:
            json.dump(payload, f, indent=2)
        
        # Use callback if available
        if self.diagnose_error_callback:
            try:
                result = self.diagnose_error_callback(payload)
                
                # Save output for debugging
                output_path = os.path.join(
                    self.artifact_dir,
                    f"env_doctor_output_{len(self.commands_tried)}.json"
                )
                with open(output_path, "w") as f:
                    json.dump({
                        "diagnosis": result.diagnosis if hasattr(result, 'diagnosis') else "",
                        "fix_commands": result.fix_commands if hasattr(result, 'fix_commands') else [],
                        "confidence": result.confidence if hasattr(result, 'confidence') else "low"
                    }, f, indent=2)
                
                return EnvDiagnosis(
                    diagnosis=result.diagnosis if hasattr(result, 'diagnosis') else "Unknown issue",
                    fix_commands=result.fix_commands if hasattr(result, 'fix_commands') else [],
                    confidence=result.confidence if hasattr(result, 'confidence') else "low",
                    is_unfixable=result.is_unfixable if hasattr(result, 'is_unfixable') else False,
                    unfixable_reason=result.unfixable_reason if hasattr(result, 'unfixable_reason') else None
                )
            
            except Exception as e:
                print(f"  ⚠️  Env Doctor callback failed: {e}")
                return EnvDiagnosis(
                    diagnosis=f"Callback failed: {e}",
                    fix_commands=[],
                    confidence="low"
                )
        else:
            # No callback available - return empty diagnosis
            return EnvDiagnosis(
                diagnosis="No LLM callback configured",
                fix_commands=[],
                confidence="low"
            )
    
    def _get_minimal_repo_structure(self) -> Dict[str, Any]:
        """Get minimal repo structure for context."""
        detected_files = detect_requirements_files(self.repo_root)
        needs_editable, editable_reason = detect_needs_editable_install(self.repo_root)
        
        # Find key files
        key_files = []
        for f in ["setup.py", "pyproject.toml", "requirements.txt", "setup.cfg", "Makefile"]:
            if os.path.exists(os.path.join(self.repo_root, f)):
                key_files.append(f)
        
        # Find package directories
        packages = []
        for item in os.listdir(self.repo_root):
            item_path = os.path.join(self.repo_root, item)
            if os.path.isdir(item_path) and not item.startswith("."):
                init_path = os.path.join(item_path, "__init__.py")
                if os.path.exists(init_path):
                    packages.append(item)
        
        return {
            "detected_files": detected_files,
            "key_files": key_files,
            "packages": packages,
            "needs_editable_install": needs_editable,
            "editable_reason": editable_reason if needs_editable else None,
            "has_setup_py": os.path.exists(os.path.join(self.repo_root, "setup.py")),
            "has_pyproject": os.path.exists(os.path.join(self.repo_root, "pyproject.toml")),
            "has_requirements": os.path.exists(os.path.join(self.repo_root, "requirements.txt"))
        }
    
    def _get_python_version(self) -> str:
        """Get Python version string."""
        try:
            outcome = run_command_with_timeout(
                f"{self.venv_python} --version",
                self.repo_root,
                timeout_s=5.0
            )
            return outcome.get("stdout", "").strip() or outcome.get("stderr", "").strip()
        except Exception:
            return "unknown"


def quick_env_check(
    repo_root: str,
    venv_python: Optional[str] = None,
    smoke_test_cmd: Optional[str] = None
) -> Tuple[bool, str]:
    """
    Quick check if environment is healthy (no LLM, just smoke test).
    
    Returns:
        (is_healthy, error_output)
    """
    python = venv_python or "python"
    
    if smoke_test_cmd is None:
        # Simple check: can we import the main package?
        doctor = EnvDoctor(repo_root, llm=None, artifact_dir="", venv_python=python)
        smoke_test_cmd = doctor._infer_smoke_test()
    
    outcome = run_command_with_timeout(smoke_test_cmd, repo_root, timeout_s=30.0)
    success = outcome.get("exit_code", 1) == 0
    error = outcome.get("stderr", "") or outcome.get("stdout", "")
    
    return success, error

