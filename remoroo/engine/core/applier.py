from __future__ import annotations
import json as _json
import os
import codecs
from pathlib import Path
from typing import Dict, Any, List, Tuple, Optional, TYPE_CHECKING
from ..utils.syntax_validator import validate_python_syntax, preserve_indentation, get_indentation_level, auto_fix_indentation

if TYPE_CHECKING:
    from ..utils.file_access_tracker import FileAccessTracker

from .patch_errors import PatchError

from .patch_errors import PatchError
from ..utils.syntax_validator import validate_python_syntax, validate_python_code_string, preserve_indentation, get_indentation_level, auto_fix_indentation

class ApplyError(RuntimeError):
    def __init__(self, message: str, patch_error: Optional[PatchError] = None):
        super().__init__(message)
        self.patch_error = patch_error

def validate_patch_before_apply(
    repo_root: str,
    patch: Dict[str, Any]
) -> List[PatchError]:
    """
    Simulate patch application in memory to validate syntax and matching.
    Returns a list of PatchError objects.
    """
    errors: List[PatchError] = []
    # In-memory virtual filesystem for simulation
    vfs: Dict[str, str] = {}
    
    def _get_content(path: str) -> str:
        if path in vfs:
            return vfs[path]
        abs_p = os.path.join(repo_root, path)
        if os.path.exists(abs_p):
            try:
                with open(abs_p, "r", encoding="utf-8", errors="replace") as f:
                    return f.read()
            except Exception:
                return ""
        return ""

    for i, e in enumerate(patch.get("edits", []), 1):
        path = e.get("path", "unknown")
        
        # HOTFIX: Rebase /app paths back to workspace explicitly
        if path.startswith("/app/"):
            path = path[5:]
            e["path"] = path

        kind = e.get("kind", "unknown")
        
        # Simulation
        current_content = _get_content(path)
        new_content = None
        
        if kind == "create_file":
            new_content = e.get("replacement", "")
        elif kind == "replace_file":
            new_content = e.get("replacement", "")
        elif kind == "search_replace":
            search = e.get("search", "")
            replace = e.get("replacement", "")
            if not search:
                errors.append(PatchError("validation_failed", path, "search_replace requires 'search'", edit_kind=kind))
                continue
                
            search_lines = len(search.splitlines())
            if search_lines > 500:
                # Provide strict feedback to the LLM to constrain context window usage
                errors.append(PatchError(
                    "validation_failed", 
                    path, 
                    f"Search string is too long ({search_lines} lines). Keep search blocks between 3-8 lines to save tokens and avoid ambiguity.", 
                    edit_kind=kind
                ))
                continue
            elif search_lines < 2:
                # Provide strict feedback to the LLM to prevent overly loose matches
                errors.append(PatchError(
                    "validation_failed", 
                    path, 
                    f"Search string is too short ({search_lines} lines). Include at least 2-3 lines of context to ensure a unique match.", 
                    edit_kind=kind
                ))
                continue
            
            match_count = current_content.count(search)
            if match_count == 0:
                # Check normalized
                def _norm(t: str) -> str: return "\n".join(l.rstrip() for l in t.replace("\r\n", "\n").split("\n"))
                if _norm(current_content).count(_norm(search)) == 0:
                    errors.append(PatchError("search_not_found", path, "Search text not found", edit_kind=kind, search_preview=search[:80]))
                    continue
                new_content = _norm(current_content).replace(_norm(search), _norm(replace), 1)
            elif match_count > 1:
                errors.append(PatchError("ambiguous_match", path, f"Ambiguous match ({match_count} times)", edit_kind=kind, search_preview=search[:80]))
                continue
            else:
                new_content = current_content.replace(search, replace, 1)
        elif kind == "insert":
            lines = current_content.splitlines(keepends=True)
            try:
                idx = min(int(e.get("after_line", 0)), len(lines))
                repl = _normalize_repl(e.get("replacement", ""))
                lines[idx:idx] = [repl]
                new_content = "".join(lines)
            except (ValueError, TypeError):
                errors.append(PatchError("validation_failed", path, "Invalid after_line for insert", edit_kind=kind))
                continue
        elif kind == "delete":
            lines = current_content.splitlines(keepends=True)
            try:
                s = int(e.get("start_line", 0)) - 1
                t = int(e.get("end_line", 0))
                if s < 0 or t < s or t > len(lines):
                    errors.append(PatchError("validation_failed", path, f"Delete out of bounds ({s+1}-{t} for {len(lines)} lines)", edit_kind=kind))
                    continue
                del lines[s:t]
                new_content = "".join(lines)
            except (ValueError, TypeError):
                errors.append(PatchError("validation_failed", path, "Invalid range for delete", edit_kind=kind))
                continue
        
        if kind == "json_set":
            key_path = e.get("key_path", "")
            value = e.get("value")
            if not key_path:
                errors.append(PatchError("validation_failed", path, "json_set requires 'key_path'", edit_kind=kind))
                continue
            if not path.endswith(".json"):
                errors.append(PatchError("validation_failed", path, "json_set only works on .json files", edit_kind=kind))
                continue
            try:
                data = _json.loads(current_content) if current_content.strip() else {}
                _json_set_nested(data, key_path, value)
                new_content = _json.dumps(data, indent=2) + "\n"
            except (KeyError, IndexError, TypeError, _json.JSONDecodeError) as exc:
                errors.append(PatchError("validation_failed", path, f"json_set failed: {exc}", edit_kind=kind))
                continue

        if new_content is not None:
            vfs[path] = new_content
            if path.endswith(".py") and new_content.strip():
                is_valid, error_msg = validate_python_code_string(new_content, filename=path)
                if not is_valid:
                    errors.append(PatchError("syntax_error", path, error_msg, edit_kind=kind))
            elif path.endswith(".json") and new_content.strip():
                try:
                    _json.loads(new_content)
                except _json.JSONDecodeError as exc:
                    errors.append(PatchError("syntax_error", path, f"Invalid JSON: {exc}", edit_kind=kind))

    return errors

def _read_text(path: str) -> str:
    with open(path, "r", encoding="utf-8", errors="replace") as f:
        return f.read()

def _read_lines(path: str) -> List[str]:
    with open(path, "r", encoding="utf-8", errors="replace") as f:
        return f.readlines()

def _write_text(path: str, text: str) -> None:
    try:
        dir_path = os.path.dirname(path)
        if dir_path:
            os.makedirs(dir_path, exist_ok=True)
        with open(path, "w", encoding="utf-8", newline="\n") as f:
            f.write(text)
    except (OSError, PermissionError) as e:
        raise ApplyError(f"Failed to create directory or write file '{path}': {str(e)}") from e
    except Exception as e:
        raise ApplyError(f"Failed to write file '{path}': {str(e)}") from e

def _write_lines(path: str, lines: List[str]) -> None:
    try:
        dir_path = os.path.dirname(path)
        if dir_path:
            os.makedirs(dir_path, exist_ok=True)
        with open(path, "w", encoding="utf-8", newline="\n") as f:
            f.writelines(lines)
    except (OSError, PermissionError) as e:
        raise ApplyError(f"Failed to create directory or write file '{path}': {str(e)}") from e
    except Exception as e:
        raise ApplyError(f"Failed to write file '{path}': {str(e)}") from e

def _normalize_repl(repl: str) -> str:
    if repl == "":
        return ""
    return repl if repl.endswith("\n") else (repl + "\n")


def _json_set_nested(data: Any, key_path: str, value: Any) -> None:
    """Set a value in a nested dict/list using a dotted key path.

    Supports dict keys and integer list indices, e.g. ``"model.layers.0.dim"``.
    Intermediate containers must already exist.
    """
    keys = key_path.split(".")
    target = data
    for k in keys[:-1]:
        if isinstance(target, list):
            target = target[int(k)]
        else:
            target = target[k]
    final = keys[-1]
    if isinstance(target, list):
        target[int(final)] = value
    else:
        target[final] = value

def edit_already_satisfied(repo_root: str, e: Dict[str, Any]) -> bool:
    abs_path = os.path.join(repo_root, e["path"])
    kind = e["kind"]

    if kind == "create_file":
        if not os.path.exists(abs_path):
            return False
        return _read_text(abs_path) == e.get("replacement", "")

    if not os.path.exists(abs_path):
        return False

    if kind == "insert":
        rep = e.get("replacement", "")
        if rep.strip() == "":
            return True

        try:
            current_text = _read_text(abs_path)
            lines = _read_lines(abs_path)
            idx = min(int(e.get("after_line", 0)), len(lines))
            repl = _normalize_repl(rep)

            if repl and idx > 0 and e.get("preserve_indentation", True):
                if idx < len(lines):
                    base_indent = get_indentation_level(lines[idx])
                else:
                    base_indent = get_indentation_level(lines[idx - 1]) if idx > 0 else 0

                repl_lines = repl.splitlines(keepends=False)
                if repl_lines:
                    first_line_indent = get_indentation_level(repl_lines[0])
                    if first_line_indent == 0 and base_indent > 0:
                        adjusted_lines = []
                        for line in repl_lines:
                            if line.strip():
                                adjusted_lines.append(" " * base_indent + line)
                            else:
                                adjusted_lines.append(line)
                        repl = "\n".join(adjusted_lines) + ("\n" if repl.endswith("\n") else "")

            return repl in current_text
        except Exception:
            return rep in _read_text(abs_path)

    if kind == "delete":
        lines = _read_lines(abs_path)
        s = int(e["start_line"]) - 1
        t = int(e["end_line"])
        if s < 0 or t < s or t > len(lines):
            return False
        if s >= len(lines):
            return True
        if s == t:
            return True
        return False

    if kind == "replace_file":
        if not os.path.exists(abs_path):
            return False
        current_content = _read_text(abs_path)
        desired_content = e.get("replacement", "")
        return current_content == desired_content

    if kind == "search_replace":
        if not os.path.exists(abs_path):
            return False
        current_content = _read_text(abs_path)
        search_text = e.get("search", "")
        replace_text = e.get("replacement", "")
        # Already satisfied if search text is gone and replacement is present
        if search_text and search_text not in current_content and replace_text in current_content:
            return True
        return False

    if kind == "json_set":
        if not os.path.exists(abs_path):
            return False
        try:
            data = _json.loads(_read_text(abs_path))
            keys = e.get("key_path", "").split(".")
            target = data
            for k in keys[:-1]:
                target = target[int(k)] if isinstance(target, list) else target[k]
            final = keys[-1]
            current = target[int(final)] if isinstance(target, list) else target[final]
            return current == e.get("value")
        except Exception:
            return False

    return False

def apply_patchproposal(
    repo_root: str, 
    patch: Dict[str, Any],
    file_access_tracker: Optional['FileAccessTracker'] = None,
    skip_validation: bool = False
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    applied: List[Dict[str, Any]] = []
    skipped: List[Dict[str, Any]] = []

    print(f"\n📝 Applying patch '{patch.get('patch_id', 'unknown')}' with {len(patch.get('edits', []))} edits")
    print(f"   Repo root: {repo_root}")

    # 1. Pre-application validation
    if not skip_validation:
        print(f"   🔍 Validating patch before application...")
        errors = validate_patch_before_apply(repo_root, patch)
        if errors:
            first_err = errors[0]
            print(f"   ❌ Pre-application validation failed: {first_err.message} in {first_err.file_path}")
            raise ApplyError(f"Pre-validation failed: {first_err.message}", patch_error=first_err)
        print(f"   ✅ Pre-validation passed")

    for i, e in enumerate(patch.get("edits", []), 1):
        # HOTFIX: Rebase /app paths back to workspace explicitly
        if e.get("path", "").startswith("/app/"):
            e["path"] = e["path"][5:]

        abs_path = os.path.join(repo_root, e["path"])
        file_path = e["path"]
        kind = e["kind"]
        
        # ... (validation checks) ...
        if kind == "search_replace":
            if not e.get("search"):
                raise ApplyError(f"search_replace requires 'search'", patch_error=PatchError("validation_failed", file_path, "Missing search field", edit_kind=kind))
            if e.get("replacement") is None:
                raise ApplyError(f"search_replace requires 'replacement'", patch_error=PatchError("validation_failed", file_path, "Missing replacement field", edit_kind=kind))
        elif kind == "replace_file":
            if e.get("replacement") is None:
                raise ApplyError(f"replace_file requires 'replacement'", patch_error=PatchError("validation_failed", file_path, "Missing replacement field", edit_kind=kind))
        # ... and so on ...

        print(f"\n  Edit {i}/{len(patch.get('edits', []))}: {kind} on {e['path']}")
        
        if file_access_tracker:
            if kind in ["search_replace", "replace_file", "insert", "delete"]:
                if os.path.exists(abs_path):
                    if not file_access_tracker.can_edit(file_path):
                        status = file_access_tracker.get_status(file_path)
                        raise ApplyError(
                            f"Cannot edit {file_path}: file not read in full.",
                            patch_error=PatchError("validation_failed", file_path, f"File not read in full (status: {status})", edit_kind=kind)
                        )
        
        if kind not in ["delete", "replace_file"] and edit_already_satisfied(repo_root, e):
            skipped.append(e)
            print(f"  ⏭️  Skipping {kind} edit for {e['path']} (already satisfied)")
            continue

        if kind == "create_file":
            overwrite = bool(e.get("overwrite", False))
            replacement = e.get("replacement", "")
            _write_text(abs_path, replacement)
            
            if abs_path.endswith('.py') and replacement.strip():
                is_valid, error_msg = validate_python_syntax(abs_path)
                if not is_valid:
                    if os.path.exists(abs_path):
                        os.remove(abs_path)
                    raise ApplyError(f"Syntax error in {e['path']} after create_file: {error_msg}", 
                                   patch_error=PatchError("syntax_error", file_path, error_msg, edit_kind=kind))
            elif abs_path.endswith('.json') and replacement.strip():
                try:
                    _json.loads(replacement)
                except _json.JSONDecodeError as exc:
                    if os.path.exists(abs_path):
                        os.remove(abs_path)
                    raise ApplyError(f"Invalid JSON in {e['path']} after create_file: {exc}",
                                   patch_error=PatchError("syntax_error", file_path, f"Invalid JSON: {exc}", edit_kind=kind))
            
            applied.append(e)
            continue

        if kind == "search_replace":
            if not os.path.exists(abs_path):
                raise ApplyError(f"File '{e['path']}' does not exist.", 
                               patch_error=PatchError("file_not_found", file_path, "File not found", edit_kind=kind))
            
            search_text = e.get("search", "")
            replace_text = e.get("replacement", "")
            original_content = _read_text(abs_path)
            
            search_lines = len(search_text.splitlines())
            if search_lines > 100:
                raise ApplyError(f"Search string is too long ({search_lines} lines). Keep search blocks between 3-8 lines to save tokens and avoid ambiguity.",
                               patch_error=PatchError("validation_failed", file_path, f"Search string is too long ({search_lines} lines). Keep search blocks between 3-8 lines to save tokens and avoid ambiguity.", edit_kind=kind))
            #elif search_lines < 2:
            #    raise ApplyError(f"Search string is too short ({search_lines} lines). Include at least 2-3 lines of context to ensure a unique match.",
            #                   patch_error=PatchError("validation_failed", file_path, f"Search string is too short ({search_lines} lines). Include at least 2-3 lines of context to ensure a unique match.", edit_kind=kind))
            
            # 1. Exact match
            match_count = original_content.count(search_text)
            new_content = None
            
            if match_count == 1:
                new_content = original_content.replace(search_text, replace_text, 1)
            elif match_count > 1:
                raise ApplyError(f"Ambiguous match ({match_count} locations)", 
                               patch_error=PatchError("ambiguous_match", file_path, f"Search text matches {match_count} locations", edit_kind=kind, search_preview=search_text[:80]))
            else:
                # 2. Whitespace-normalized match
                def _normalize_ws(text: str) -> str:
                    return "\n".join(line.rstrip() for line in text.replace("\r\n", "\n").split("\n"))
                
                norm_content = _normalize_ws(original_content)
                norm_search = _normalize_ws(search_text)
                norm_match_count = norm_content.count(norm_search)
                
                if norm_match_count == 1:
                    new_content = norm_content.replace(norm_search, _normalize_ws(replace_text), 1)
                    if original_content.endswith("\n") and not new_content.endswith("\n"):
                        new_content += "\n"
                    print(f"    (whitespace-normalized match)")
                elif norm_match_count > 1:
                    raise ApplyError(f"Ambiguous normalized match ({norm_match_count} locations)", 
                                   patch_error=PatchError("ambiguous_match", file_path, f"Normalized search text matches {norm_match_count} locations", edit_kind=kind, search_preview=search_text[:80]))
                else:
                    # 3. Aggressive normalization
                    def _normalize_ws_aggressive(text: str) -> str:
                        import re
                        normalized = "\n".join(line.rstrip() for line in text.replace("\r\n", "\n").split("\n"))
                        normalized = re.sub(r'\n\n+', '\n', normalized)
                        return normalized
                    
                    aggr_content = _normalize_ws_aggressive(original_content)
                    aggr_search = _normalize_ws_aggressive(search_text)
                    aggr_match_count = aggr_content.count(aggr_search)
                    
                    if aggr_match_count == 1:
                        new_content = aggr_content.replace(aggr_search, _normalize_ws_aggressive(replace_text), 1)
                        if original_content.endswith("\n") and not new_content.endswith("\n"):
                            new_content += "\n"
                        print(f"    (aggressive whitespace normalization match)")
                    elif aggr_match_count > 1:
                        raise ApplyError(f"Ambiguous aggressive match ({aggr_match_count} locations)", 
                                       patch_error=PatchError("ambiguous_match", file_path, f"Aggressively normalized search text matches {aggr_match_count} locations", edit_kind=kind, search_preview=search_text[:80]))
                    else:
                        raise ApplyError(f"Search text not found", 
                                       patch_error=PatchError("search_not_found", file_path, "Search text not found", edit_kind=kind, search_preview=search_text[:80]))
            
            _write_text(abs_path, new_content)
            
            if abs_path.endswith('.py'):
                is_valid, error_msg = validate_python_syntax(abs_path)
                if not is_valid:
                    _write_text(abs_path, original_content)
                    raise ApplyError(f"Syntax error after search_replace: {error_msg}", 
                                   patch_error=PatchError("syntax_error", file_path, error_msg, edit_kind=kind))
            elif abs_path.endswith('.json') and new_content.strip():
                try:
                    _json.loads(new_content)
                except _json.JSONDecodeError as exc:
                    _write_text(abs_path, original_content)
                    raise ApplyError(f"Invalid JSON after search_replace: {exc}",
                                   patch_error=PatchError("syntax_error", file_path, f"Invalid JSON: {exc}", edit_kind=kind))
            
            applied.append(e)
            continue

        if kind == "insert":
            if not os.path.exists(abs_path):
                raise ApplyError(f"File '{e['path']}' does not exist.", 
                               patch_error=PatchError("file_not_found", file_path, "File not found", edit_kind=kind))
            
            lines = _read_lines(abs_path)
            try:
                idx = min(int(e.get("after_line", 0)), len(lines))
            except (ValueError, TypeError):
                raise ApplyError(f"Invalid after_line: {e.get('after_line')}", 
                               patch_error=PatchError("validation_failed", file_path, "Invalid after_line", edit_kind=kind))
                
            repl = _normalize_repl(e.get("replacement", ""))
            
            if repl and idx > 0 and e.get("preserve_indentation", True):
                if idx < len(lines):
                    base_indent = get_indentation_level(lines[idx])
                else:
                    base_indent = get_indentation_level(lines[idx - 1]) if idx > 0 else 0
                
                repl_lines = repl.splitlines(keepends=False)
                if repl_lines:
                    first_line_indent = get_indentation_level(repl_lines[0])
                    if first_line_indent == 0:
                        adjusted_lines = []
                        for line in repl_lines:
                            if line.strip():
                                adjusted_lines.append(" " * base_indent + line)
                            else:
                                adjusted_lines.append(line)
                        repl = "\n".join(adjusted_lines) + ("\n" if repl.endswith("\n") else "")
            
            ins = repl.splitlines(keepends=True) if repl else []
            original_lines = lines.copy()
            lines[idx:idx] = ins
            _write_lines(abs_path, lines)
            
            if abs_path.endswith('.py'):
                is_valid, error_msg = validate_python_syntax(abs_path)
                if not is_valid:
                    _write_lines(abs_path, original_lines)
                    raise ApplyError(f"Syntax error after insert: {error_msg}", 
                                   patch_error=PatchError("syntax_error", file_path, error_msg, edit_kind=kind))
            elif abs_path.endswith('.json'):
                try:
                    _json.loads("".join(lines))
                except _json.JSONDecodeError as exc:
                    _write_lines(abs_path, original_lines)
                    raise ApplyError(f"Invalid JSON after insert: {exc}",
                                   patch_error=PatchError("syntax_error", file_path, f"Invalid JSON: {exc}", edit_kind=kind))
            
            applied.append(e)
            continue

        if kind == "delete":
            if not os.path.exists(abs_path):
                raise ApplyError(f"File '{e['path']}' does not exist.", 
                               patch_error=PatchError("file_not_found", file_path, "File not found", edit_kind=kind))
            
            lines = _read_lines(abs_path)
            try:
                s = int(e["start_line"]) - 1
                t = int(e["end_line"])
            except (ValueError, TypeError):
                raise ApplyError(f"Invalid lines: {e.get('start_line')}-{e.get('end_line')}", 
                               patch_error=PatchError("validation_failed", file_path, "Invalid start_line or end_line", edit_kind=kind))
                
            if s < 0 or t < s or t > len(lines):
                raise ApplyError(f"Delete out of bounds ({s+1}-{t} for {len(lines)} lines)", 
                               patch_error=PatchError("validation_failed", file_path, "Delete out of bounds", edit_kind=kind))
            
            if s >= len(lines) or s == t:
                skipped.append(e)
                continue
            
            original_lines = lines.copy()
            del lines[s:t]
            _write_lines(abs_path, lines)
            
            if abs_path.endswith('.py'):
                is_valid, error_msg = validate_python_syntax(abs_path)
                if not is_valid:
                    _write_lines(abs_path, original_lines)
                    raise ApplyError(f"Syntax error after delete: {error_msg}", 
                                   patch_error=PatchError("syntax_error", file_path, error_msg, edit_kind=kind))
            elif abs_path.endswith('.json'):
                try:
                    _json.loads("".join(lines))
                except _json.JSONDecodeError as exc:
                    _write_lines(abs_path, original_lines)
                    raise ApplyError(f"Invalid JSON after delete: {exc}",
                                   patch_error=PatchError("syntax_error", file_path, f"Invalid JSON: {exc}", edit_kind=kind))
            
            applied.append(e)
            continue

        if kind == "replace_file":
            if not os.path.exists(abs_path):
                raise ApplyError(f"File '{e['path']}' does not exist.", 
                               patch_error=PatchError("file_not_found", file_path, "File not found", edit_kind=kind))
            
            replacement = e.get("replacement", "")
            original_content = _read_text(abs_path)
            
            _write_text(abs_path, replacement)
            
            if abs_path.endswith('.py'):
                is_valid, error_msg = validate_python_syntax(abs_path)
                if not is_valid:
                    _write_text(abs_path, original_content)
                    raise ApplyError(f"Syntax error after replace_file: {error_msg}", 
                                   patch_error=PatchError("syntax_error", file_path, error_msg, edit_kind=kind))
            elif abs_path.endswith('.json') and replacement.strip():
                try:
                    _json.loads(replacement)
                except _json.JSONDecodeError as exc:
                    _write_text(abs_path, original_content)
                    raise ApplyError(f"Invalid JSON after replace_file: {exc}",
                                   patch_error=PatchError("syntax_error", file_path, f"Invalid JSON: {exc}", edit_kind=kind))
            
            applied.append(e)
            continue

        if kind == "json_set":
            key_path = e.get("key_path", "")
            value = e.get("value")

            if not abs_path.endswith('.json'):
                raise ApplyError(f"json_set only works on .json files, got '{e['path']}'",
                               patch_error=PatchError("validation_failed", file_path, "json_set requires a .json file", edit_kind=kind))
            if not os.path.exists(abs_path):
                raise ApplyError(f"File '{e['path']}' does not exist.",
                               patch_error=PatchError("file_not_found", file_path, "File not found", edit_kind=kind))

            original_content = _read_text(abs_path)
            try:
                data = _json.loads(original_content)
                _json_set_nested(data, key_path, value)
                new_content = _json.dumps(data, indent=2) + "\n"
            except (_json.JSONDecodeError, KeyError, IndexError, TypeError) as exc:
                raise ApplyError(f"json_set failed on '{e['path']}': {exc}",
                               patch_error=PatchError("validation_failed", file_path, f"json_set failed: {exc}", edit_kind=kind))

            _write_text(abs_path, new_content)
            print(f"    json_set {key_path} = {_json.dumps(value)}")
            applied.append(e)
            continue
        
    return applied, skipped
