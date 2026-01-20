"""Syntax validation utilities for Python code."""
from __future__ import annotations
import ast
import py_compile
import tempfile
import os
import re
from typing import Tuple, Optional, List

def validate_python_syntax(file_path: str) -> Tuple[bool, Optional[str]]:
    """
    Validate Python syntax of a file.
    
    Returns:
        (is_valid, error_message)
    """
    try:
        # Try parsing with ast
        with open(file_path, "r", encoding="utf-8") as f:
            source = f.read()
        
        try:
            ast.parse(source, filename=file_path)
        except SyntaxError as e:
            return False, f"SyntaxError at line {e.lineno}: {e.msg}\n{e.text}"
        
        # Also try compiling (catches some additional issues)
        try:
            py_compile.compile(file_path, doraise=True)
        except py_compile.PyCompileError as e:
            return False, str(e)
        
        return True, None
    except Exception as e:
        return False, f"Error validating syntax: {str(e)}"

def validate_python_code_string(code: str, filename: str = "<string>") -> Tuple[bool, Optional[str]]:
    """
    Validate Python syntax of a code string.
    
    Returns:
        (is_valid, error_message)
    """
    try:
        ast.parse(code, filename=filename)
        return True, None
    except SyntaxError as e:
        error_msg = f"SyntaxError at line {e.lineno}: {e.msg}"
        if e.text:
            error_msg += f"\n{e.text}"
            if e.offset:
                error_msg += " " * (e.offset - 1) + "^"
        return False, error_msg
    except Exception as e:
        return False, f"Error validating syntax: {str(e)}"

def get_indentation_level(line: str) -> int:
    """Get the indentation level (number of leading spaces) of a line."""
    return len(line) - len(line.lstrip(' '))

def normalize_indentation(code: str) -> str:
    """
    Normalize indentation by removing common leading whitespace.
    Similar to textwrap.dedent but handles mixed indentation better.
    
    Args:
        code: Code string with potentially inconsistent indentation
    
    Returns:
        Code with normalized indentation (minimum indent removed)
    """
    lines = code.splitlines(keepends=False)
    if not lines:
        return code
    
    # Find minimum indentation (ignoring empty lines)
    min_indent = float('inf')
    for line in lines:
        if line.strip():  # Non-empty line
            indent = get_indentation_level(line)
            min_indent = min(min_indent, indent)
    
    if min_indent == float('inf') or min_indent == 0:
        return code  # No indentation to normalize
    
    # Remove common leading whitespace
    normalized_lines = []
    for line in lines:
        if line.strip():
            # Remove min_indent spaces
            if len(line) >= min_indent and line[:min_indent].isspace():
                normalized_lines.append(line[min_indent:])
            else:
                normalized_lines.append(line)
        else:
            normalized_lines.append("")
    
    return "\n".join(normalized_lines) + ("\n" if code.endswith("\n") else "")

def preserve_indentation(original_lines: List[str], replacement: str, start_line: int) -> str:
    """
    Preserve indentation when replacing code using AST-aware approach.
    
    Args:
        original_lines: Original file lines (with newlines)
        replacement: Replacement code (may not have correct indentation)
        start_line: Line number where replacement starts (1-indexed)
    
    Returns:
        Replacement code with preserved indentation
    """
    if not original_lines or start_line < 1 or start_line > len(original_lines):
        return replacement
    
    # Get indentation of the line being replaced
    base_line = original_lines[start_line - 1].rstrip('\n\r')
    base_indent = get_indentation_level(base_line)
    
    # Normalize the replacement (remove common leading whitespace)
    normalized_repl = normalize_indentation(replacement)
    
    # Split into lines
    repl_lines = normalized_repl.splitlines(keepends=False)
    if not repl_lines:
        return replacement
    
    # Apply base indentation to all lines
    adjusted_lines = []
    for line in repl_lines:
        if line.strip():  # Non-empty line
            adjusted_lines.append(" " * base_indent + line)
        else:
            adjusted_lines.append("")
    
    result = "\n".join(adjusted_lines)
    return result + ("\n" if replacement.endswith("\n") else "")

def fix_relative_indentation(code: str) -> str:
    """
    Fix relative indentation within code block by detecting Python block structure.
    Lines after ':' should be indented more.
    """
    lines = code.splitlines(keepends=False)
    if not lines:
        return code
    
    fixed_lines = []
    indent_stack = [0]  # Track indentation levels
    
    for i, line in enumerate(lines):
        if not line.strip():
            fixed_lines.append("")
            continue
        
        # Check if previous line ended with ':'
        if i > 0 and lines[i-1].strip().endswith(':'):
            # This line should be indented more
            prev_indent = get_indentation_level(lines[i-1])
            current_indent = get_indentation_level(line)
            if current_indent <= prev_indent:
                # Needs more indentation
                new_indent = prev_indent + 4
                fixed_lines.append(" " * new_indent + line.lstrip())
                indent_stack.append(new_indent)
            else:
                fixed_lines.append(line)
                indent_stack.append(current_indent)
        else:
            # Check if we should dedent (line starts at same or less indent than previous block)
            current_indent = get_indentation_level(line)
            # Pop indent_stack until we find matching level
            while len(indent_stack) > 1 and current_indent < indent_stack[-1]:
                indent_stack.pop()
            
            # If line is at same indent as current block, keep it
            if current_indent == indent_stack[-1]:
                fixed_lines.append(line)
            elif current_indent < indent_stack[-1]:
                # Should be at current block level
                fixed_lines.append(" " * indent_stack[-1] + line.lstrip())
            else:
                # More indented, keep it
                fixed_lines.append(line)
                indent_stack.append(current_indent)
    
    return "\n".join(fixed_lines) + ("\n" if code.endswith("\n") else "")

def find_correct_indentation(replacement: str, context_lines: List[str], start_line: int, end_line: int) -> Optional[str]:
    """
    Find the correct indentation for replacement code by testing multiple strategies.
    Preserves relative indentation structure within the replacement.
    
    Args:
        replacement: Replacement code
        context_lines: Original file lines
        start_line: Start line of replacement (1-indexed)
        end_line: End line of replacement (1-indexed)
    
    Returns:
        Replacement with correct indentation, or None if couldn't determine
    """
    # Strategy 1: Use base indentation from start_line
    base_line = context_lines[start_line - 1].rstrip('\n\r') if start_line <= len(context_lines) else ""
    base_indent = get_indentation_level(base_line) if base_line else 0
    
    # Normalize replacement first (removes common leading whitespace, preserves relative)
    normalized = normalize_indentation(replacement)
    
    # Fix relative indentation structure (lines after ':' should be indented)
    normalized = fix_relative_indentation(normalized)
    
    repl_lines = normalized.splitlines(keepends=False)
    
    if not repl_lines:
        return replacement
    
    # Find minimum indentation in normalized code (should be 0 after normalization)
    min_repl_indent = min(
        (get_indentation_level(line) for line in repl_lines if line.strip()),
        default=0
    )
    
    # Try different base indentation levels and see which one parses
    for indent_offset in [0, 4, -4, 8, -8, 12, -12]:
        test_base_indent = max(0, base_indent + indent_offset)
        test_lines = []
        
        # Apply indentation preserving relative structure
        for line in repl_lines:
            if line.strip():
                # Get relative indent from normalized line
                line_indent = get_indentation_level(line)
                relative_indent = line_indent - min_repl_indent
                # Apply base indent + relative indent
                total_indent = test_base_indent + relative_indent
                test_lines.append(" " * max(0, total_indent) + line.lstrip())
            else:
                test_lines.append("")
        
        test_code = "\n".join(test_lines) + ("\n" if replacement.endswith("\n") else "")
        
        # Try to parse in context
        try:
            # Create a test file with context
            test_content = "".join(context_lines[:start_line-1]) + test_code + "".join(context_lines[end_line:])
            ast.parse(test_content)
            # This indentation works!
            return test_code
        except SyntaxError:
            continue
    
    # Fallback: use base_indent with preserved relative structure
    adjusted_lines = []
    for line in repl_lines:
        if line.strip():
            line_indent = get_indentation_level(line)
            relative_indent = line_indent - min_repl_indent
            total_indent = base_indent + relative_indent
            adjusted_lines.append(" " * max(0, total_indent) + line.lstrip())
        else:
            adjusted_lines.append("")
    return "\n".join(adjusted_lines) + ("\n" if replacement.endswith("\n") else "")

def auto_fix_indentation(code: str, base_indent: int = 0) -> str:
    """
    Auto-fix indentation using AST parsing and re-indentation.
    Falls back to simple normalization if AST parsing fails.
    
    Args:
        code: Code string to fix
        base_indent: Base indentation level (spaces)
    
    Returns:
        Code with fixed indentation
    """
    # First, try to normalize (remove common leading whitespace)
    normalized = normalize_indentation(code)
    
    # Try to parse as AST to validate structure
    try:
        # Validate the normalized code can be parsed
        ast.parse(normalized)
        
        # Apply base indentation
        lines = normalized.splitlines(keepends=False)
        adjusted_lines = []
        for line in lines:
            if line.strip():
                adjusted_lines.append(" " * base_indent + line)
            else:
                adjusted_lines.append("")
        
        result = "\n".join(adjusted_lines)
        return result + ("\n" if code.endswith("\n") else "")
    except SyntaxError:
        # If AST parsing fails, use simple normalization + base indent
        lines = normalized.splitlines(keepends=False)
        adjusted_lines = []
        for line in lines:
            if line.strip():
                adjusted_lines.append(" " * base_indent + line)
            else:
                adjusted_lines.append("")
        result = "\n".join(adjusted_lines)
        return result + ("\n" if code.endswith("\n") else "")

def detect_duplicate_lines(lines: List[str], error_line: int) -> List[int]:
    """
    Detect duplicate or erroneous lines around the error line.
    Returns list of line indices (0-indexed) that should be removed.
    """
    duplicates = []
    if error_line >= len(lines):
        return duplicates
    
    error_line_content = lines[error_line].strip()
    if not error_line_content:
        return duplicates
    
    # Check if this line appears elsewhere in the file
    for i, line in enumerate(lines):
        if i != error_line and line.strip() == error_line_content:
            # Found duplicate - check context
            # If error line is after a return/break/continue/raise, it's likely erroneous
            if error_line > 0:
                prev_line = lines[error_line - 1].strip()
                if any(keyword in prev_line for keyword in ['return', 'break', 'continue', 'raise', 'pass']):
                    duplicates.append(error_line)
                    break
    
    return duplicates

def fix_indentation_errors(file_path: str) -> Tuple[bool, Optional[str], Optional[str]]:
    """
    Attempt to auto-fix indentation errors in a Python file.
    Uses multiple strategies to fix common indentation issues.
    
    Returns:
        (success, fixed_content, error_message)
    """
    try:
        with open(file_path, "r", encoding="utf-8") as f:
            content = f.read()
        
        lines = content.splitlines(keepends=True)
        if not lines:
            return True, content, None
        
        # Try to parse and identify indentation issues
        try:
            ast.parse(content, filename=file_path)
            # No syntax errors
            return True, content, None
        except SyntaxError as e:
            error_msg = str(e)
            error_line = e.lineno - 1 if e.lineno else 0
            
            # Check if it's an indentation error
            is_indent_error = (
                "indentation" in error_msg.lower() or 
                "IndentationError" in str(type(e).__name__) or
                "expected an indented block" in error_msg.lower() or
                "unindent does not match" in error_msg.lower() or
                "unexpected indent" in error_msg.lower()
            )
            
            if is_indent_error and error_line < len(lines):
                # Strategy 0: Check for duplicate/erroneous lines
                duplicates = detect_duplicate_lines(lines, error_line)
                if duplicates:
                    # Remove duplicate lines
                    fixed_lines = lines.copy()
                    for dup_idx in sorted(duplicates, reverse=True):
                        if dup_idx < len(fixed_lines):
                            del fixed_lines[dup_idx]
                    fixed_content = "".join(fixed_lines)
                    try:
                        ast.parse(fixed_content, filename=file_path)
                        return True, fixed_content, None
                    except SyntaxError:
                        pass  # Continue to other strategies
                
                # Strategy 1: Fix based on previous line context
                fixed_content = _fix_indent_strategy1(lines, error_line)
                if fixed_content:
                    try:
                        ast.parse(fixed_content, filename=file_path)
                        return True, fixed_content, None
                    except SyntaxError:
                        pass
                
                # Strategy 2: Remove line if it's after return/break/continue/raise
                if error_line > 0:
                    prev_line = lines[error_line - 1].strip()
                    if any(keyword in prev_line for keyword in ['return', 'break', 'continue', 'raise']):
                        fixed_lines = lines.copy()
                        if error_line < len(fixed_lines):
                            del fixed_lines[error_line]
                        fixed_content = "".join(fixed_lines)
                        try:
                            ast.parse(fixed_content, filename=file_path)
                            return True, fixed_content, None
                        except SyntaxError:
                            pass
                
                # Strategy 3: Normalize all indentation and re-apply
                fixed_content = _fix_indent_strategy2(content, lines, error_line)
                if fixed_content:
                    try:
                        ast.parse(fixed_content, filename=file_path)
                        return True, fixed_content, None
                    except SyntaxError:
                        pass
                
                return False, None, f"Could not auto-fix indentation error at line {error_line + 1}: {error_msg}"
            else:
                return False, None, f"Syntax error (not indentation): {error_msg}"
    except Exception as e:
        return False, None, f"Error fixing indentation: {str(e)}"

def _fix_indent_strategy1(lines: List[str], error_line: int) -> Optional[str]:
    """Fix indentation by analyzing context around error line."""
    if error_line >= len(lines):
        return None
    
    fixed_lines = lines.copy()
    problem_line = lines[error_line].rstrip('\n\r')
    
    if not problem_line.strip():
        return None
    
    # Find the expected indentation by looking at previous lines
    expected_indent = 0
    
    # Look backwards for context
    for i in range(max(0, error_line - 10), error_line):
        line = lines[i].rstrip('\n\r')
        if not line.strip():
            continue
        
        current_indent = get_indentation_level(line)
        
        # If line ends with colon, next line should be indented
        if line.strip().endswith(':'):
            expected_indent = current_indent + 4
            break
        # If line is part of a block, use its indent
        elif i == error_line - 1:
            # Previous line - check if it's a block starter
            if ':' in line:
                expected_indent = current_indent + 4
            else:
                expected_indent = current_indent
            break
    
    # Fix the problematic line
    current_indent = get_indentation_level(problem_line)
    if current_indent != expected_indent:
        fixed_line = " " * expected_indent + problem_line.lstrip() + "\n"
        fixed_lines[error_line] = fixed_line
        return "".join(fixed_lines)
    
    return None

def _fix_indent_strategy2(original_content: str, lines: List[str], error_line: int) -> Optional[str]:
    """Fix indentation by normalizing and re-applying based on AST structure."""
    # This is a fallback - try to fix by ensuring consistent indentation
    # around the error line
    
    if error_line >= len(lines):
        return None
    
    # Get the base indentation from the function/class context
    base_indent = 0
    for i in range(max(0, error_line - 20), error_line):
        line = lines[i].rstrip('\n\r')
        if line.strip():
            # Check if it's a function/class definition
            if re.match(r'^\s*(def|class|if|elif|else|for|while|with|try|except|finally)\s+', line):
                base_indent = get_indentation_level(line)
                # Next line should be indented
                if error_line == i + 1:
                    return None  # Let strategy 1 handle it
                break
    
    # Try to fix the error line
    problem_line = lines[error_line].rstrip('\n\r')
    if problem_line.strip():
        # Check if it should be indented based on context
        if error_line > 0:
            prev_line = lines[error_line - 1].rstrip('\n\r')
            if prev_line.strip().endswith(':'):
                # Should be indented
                expected = get_indentation_level(prev_line) + 4
                fixed_line = " " * expected + problem_line.lstrip() + "\n"
                fixed_lines = lines.copy()
                fixed_lines[error_line] = fixed_line
                return "".join(fixed_lines)
    
    return None
