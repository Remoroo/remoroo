from __future__ import annotations
import os
import codecs
from pathlib import Path
from typing import Dict, Any, List, Tuple, Optional, TYPE_CHECKING
from ..utils.syntax_validator import validate_python_syntax, preserve_indentation, get_indentation_level, auto_fix_indentation

if TYPE_CHECKING:
    from ..utils.file_access_tracker import FileAccessTracker

class ApplyError(RuntimeError):
    pass

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

    return False

def apply_patchproposal(
    repo_root: str, 
    patch: Dict[str, Any],
    file_access_tracker: Optional['FileAccessTracker'] = None
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    applied: List[Dict[str, Any]] = []
    skipped: List[Dict[str, Any]] = []

    print(f"\n📝 Applying patch '{patch.get('patch_id', 'unknown')}' with {len(patch.get('edits', []))} edits")
    print(f"   Repo root: {repo_root}")

    for i, e in enumerate(patch.get("edits", []), 1):
        abs_path = os.path.join(repo_root, e["path"])
        file_path = e["path"]
        kind = e["kind"]
        
        if kind == "replace_file":
            if not e.get("replacement"):
                raise ApplyError(f"replace_file operation requires non-empty 'replacement' field for {e['path']}")
        elif kind == "create_file":
            if not e.get("replacement"):
                raise ApplyError(f"create_file operation requires non-empty 'replacement' field for {e['path']}")
        elif kind == "insert":
            if e.get("after_line") is None:
                raise ApplyError(f"insert operation requires non-None 'after_line' field for {e['path']}")
            if not e.get("replacement"):
                raise ApplyError(f"insert operation requires non-empty 'replacement' field for {e['path']}")
        elif kind == "delete":
            if e.get("start_line") is None:
                raise ApplyError(f"delete operation requires non-None 'start_line' field for {e['path']}")
            if e.get("end_line") is None:
                raise ApplyError(f"delete operation requires non-None 'end_line' field for {e['path']}")
        
        print(f"\n  Edit {i}/{len(patch.get('edits', []))}: {kind} on {e['path']}")
        
        if file_access_tracker:
            if kind in ["replace_file", "insert", "delete"]:
                if os.path.exists(abs_path):
                    if not file_access_tracker.can_edit(file_path):
                        status = file_access_tracker.get_status(file_path)
                        raise ApplyError(
                            f"Cannot edit {file_path}: file not read in full. "
                            f"Current status: {status}."
                        )
        
        if kind not in ["delete", "replace_file"] and edit_already_satisfied(repo_root, e):
            skipped.append(e)
            print(f"  ⏭️  Skipping {kind} edit for {e['path']} (already satisfied)")
            continue

        if kind == "create_file":
            overwrite = bool(e.get("overwrite", False))
            if os.path.exists(abs_path) and not overwrite:
                print(f"  ⚠️  File '{e['path']}' already exists but overwrite=False. Overwriting anyway.")
            
            replacement = e.get("replacement", "")
            _write_text(abs_path, replacement)
            
            if abs_path.endswith('.py') and replacement.strip():
                is_valid, error_msg = validate_python_syntax(abs_path)
                if not is_valid:
                    if os.path.exists(abs_path):
                        os.remove(abs_path)
                    raise ApplyError(f"Syntax error in {e['path']} after create_file: {error_msg}")
            
            applied.append(e)
            continue

        if not os.path.exists(abs_path):
            error_msg = f"{kind} operation failed: File '{e['path']}' does not exist."
            print(f"  ❌ {error_msg}")
            raise ApplyError(error_msg)

        if kind == "insert":
            lines = _read_lines(abs_path)
            idx = min(int(e.get("after_line", 0)), len(lines))
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
                    raise ApplyError(f"Syntax error in {e['path']} after insert: {error_msg}")
            
            applied.append(e)
            continue

        if kind == "delete":
            lines = _read_lines(abs_path)
            s = int(e["start_line"]) - 1
            t = int(e["end_line"])
            if s < 0 or t < s or t > len(lines):
                raise ApplyError(f"delete out of bounds for {e['path']}: {s+1}-{t} (file has {len(lines)} lines)")
            
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
                    raise ApplyError(f"Syntax error in {e['path']} after delete: {error_msg}")
            
            applied.append(e)
            continue

        if kind == "replace_file":
            if not os.path.exists(abs_path):
                raise ApplyError(f"replace_file operation failed: File '{e['path']}' does not exist.")
            
            replacement = e.get("replacement", "")
            original_content = _read_text(abs_path)
            
            abs_path_obj = Path(abs_path)
            abs_path_obj.parent.mkdir(parents=True, exist_ok=True)
            with abs_path_obj.open("w", encoding="utf-8", newline="\n") as f:
                f.write(replacement)
            
            if abs_path.endswith('.py'):
                is_valid, error_msg = validate_python_syntax(abs_path)
                if not is_valid:
                    with abs_path_obj.open("w", encoding="utf-8", newline="\n") as f:
                        f.write(original_content)
                    raise ApplyError(f"Syntax error in {e['path']} after replace_file: {error_msg}")
            
            applied.append(e)
            continue
        
        raise ApplyError(f"Unsupported kind: {kind}")

    print(f"\n✅ Patch application complete: {len(applied)} applied, {len(skipped)} skipped")
    return applied, skipped
