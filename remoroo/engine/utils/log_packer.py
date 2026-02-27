"""Log packer for extracting errors and truncating command output for Judge LLM.

NOTE: Metric extraction and convergence analysis have been removed.
The Judge LLM now handles all metric extraction using ExperimentContract.
This module only handles error extraction (useful for highlighting) and output truncation.
"""
from __future__ import annotations
import re
from typing import Dict, Any, List

def extract_tracebacks(output: str) -> List[Dict[str, Any]]:
    """
    Extract tracebacks from command output across multiple languages (Python, JS/V8, Rust, C++).
    """
    lines = output.split('\n')
    tracebacks = []
    current_traceback = None
    traceback_lines = []
    in_traceback = False
    language_mode = None  # 'python', 'node', 'rust', 'gcc'
    
    # Generic error pattern
    error_pattern = re.compile(
        r'^(\w+Error|Exception|Warning|Error|Panic|fatal error|error):\s*(.+)$',
        re.IGNORECASE
    )
    
    # Python Location
    py_loc_pattern = re.compile(r'File\s+["\']([^"\']+)["\'],\s+line\s+(\d+)', re.IGNORECASE)
    # Node Location
    node_loc_pattern = re.compile(r'^\s+at\s+(?:.*?\s+)?\(?(.*?):(\d+):\d+\)?')
    # Rust panic location
    rust_loc_pattern = re.compile(r'panicked at.*?([^\\/]+\.rs):(\d+):(\d+)')
    
    for i, line in enumerate(lines, 1):
        stripped = line.strip()
        
        # 1. Detect Traceback Start
        is_python_start = stripped.startswith('Traceback (most recent call last):')
        is_node_start = bool(re.match(r'^[\w]*Error:\s', stripped)) and i < len(lines) and '    at ' in lines[i]
        is_rust_start = stripped.startswith("thread ") and "panicked at" in stripped
        
        if is_python_start or is_node_start or is_rust_start:
            if current_traceback and traceback_lines:
                current_traceback['full_traceback'] = '\n'.join(traceback_lines)
                tracebacks.append(current_traceback)
            
            lang = 'python' if is_python_start else 'node' if is_node_start else 'rust'
            current_traceback = {
                'error_type': None,
                'error_message': None,
                'file_location': None,
                'line_number': None,
                'full_traceback': None,
                'traceback_start_line': i,
                'language': lang
            }
            traceback_lines = [line]
            in_traceback = True
            language_mode = lang
            
            # Immediately extract info if possible
            if is_node_start:
                m = re.match(r'^([\w]*Error):\s*(.+)', stripped)
                if m:
                    current_traceback['error_type'] = m.group(1)
                    current_traceback['error_message'] = m.group(2)
            elif is_rust_start:
                current_traceback['error_type'] = 'Panic'
                msg_match = re.search(r'panicked at \'(.*?)\'', stripped)
                current_traceback['error_message'] = msg_match.group(1) if msg_match else "Rust Panic"
                loc_match = rust_loc_pattern.search(stripped)
                if loc_match:
                    current_traceback['file_location'] = loc_match.group(1)
                    current_traceback['line_number'] = int(loc_match.group(2))
            continue
            
        if in_traceback:
            traceback_lines.append(line)
            
            if language_mode == 'python':
                loc_m = py_loc_pattern.search(line)
                if loc_m and not current_traceback.get('file_location'):
                    current_traceback['file_location'] = loc_m.group(1)
                    try: current_traceback['line_number'] = int(loc_m.group(2))
                    except ValueError: pass
                
                err_m = error_pattern.match(stripped)
                if err_m and not loc_m:
                    current_traceback['error_type'] = err_m.group(1)
                    current_traceback['error_message'] = err_m.group(2)
                    current_traceback['full_traceback'] = '\n'.join(traceback_lines)
                    tracebacks.append(current_traceback)
                    in_traceback = False
                    current_traceback = None
                    traceback_lines = []
            
            elif language_mode == 'node':
                loc_m = node_loc_pattern.search(line)
                if loc_m and not current_traceback.get('file_location'):
                    current_traceback['file_location'] = loc_m.group(1)
                    try: current_traceback['line_number'] = int(loc_m.group(2))
                    except ValueError: pass
                # Node tracebacks end when indentation stops
                if not line.startswith('    ') and stripped:
                    current_traceback['full_traceback'] = '\n'.join(traceback_lines[:-1]) # Don't include this non-traceback line
                    tracebacks.append(current_traceback)
                    in_traceback = False
                    current_traceback = None
                    traceback_lines = []
                    
            elif language_mode == 'rust':
                # Rust traces end at empty lines
                if not stripped:
                    current_traceback['full_traceback'] = '\n'.join(traceback_lines)
                    tracebacks.append(current_traceback)
                    in_traceback = False
                    current_traceback = None
                    traceback_lines = []
                    
    # GC remainder
    if current_traceback and traceback_lines:
        current_traceback['full_traceback'] = '\n'.join(traceback_lines)
        tracebacks.append(current_traceback)
        
    return tracebacks

def extract_errors(output: str) -> Dict[str, Any]:
    """
    Extract all types of errors from command output (generic - not specific to any experiment).
    Returns dict with error_summary, error_count, critical_errors, and error_details.
    """
    lines = output.split('\n')
    errors = []
    error_keywords = [
        'error', 'exception', 'failed', 'failure', 'fatal', 'critical',
        'traceback', 'syntaxerror', 'indentationerror', 'keyerror', 'importerror',
        'valueerror', 'typeerror', 'attributeerror', 'runtimeerror', 'nameerror'
    ]
    
    # Extract tracebacks (Python-specific but common)
    tracebacks = extract_tracebacks(output)
    
    # Extract standalone error lines (non-traceback errors)
    for i, line in enumerate(lines, 1):
        line_lower = line.lower()
        
        # Skip if this line is part of a traceback we already captured
        is_in_traceback = any(
            tb.get('traceback_start_line', 0) <= i <= tb.get('traceback_start_line', 0) + 20
            for tb in tracebacks
        )
        
        if is_in_traceback:
            continue
        
        # Check for error keywords
        has_error_keyword = any(keyword in line_lower for keyword in error_keywords)
        
        if has_error_keyword:
            # Try to extract error type and message
            error_match = re.search(
                r'(\w+Error|Exception|Warning|Error):\s*(.+)',
                line,
                re.IGNORECASE
            )
            
            if error_match:
                error_type = error_match.group(1)
                error_message = error_match.group(2)
            else:
                # C++/GCC single line errors
                gcc_match = re.search(r'^([^:]+):(\d+):(?:\d+:)?\s+(error|fatal error):\s+(.*)$', line)
                if gcc_match:
                    error_type = "GCC_Error"
                    error_message = f"{gcc_match.group(3)}: {gcc_match.group(4)}"
                    # Sneak in a faux traceback object for GCC if needed, or just standalone
                else:
                    error_type = "Error"
                    error_message = line.strip()
            
            errors.append({
                'line_number': i,
                'line_content': line.strip(),
                'error_type': error_type,
                'error_message': error_message
            })
    
    # Combine tracebacks and standalone errors
    all_errors = []
    
    # Add tracebacks
    for tb in tracebacks:
        all_errors.append({
            'type': 'traceback',
            'error_type': tb.get('error_type', 'UnknownError'),
            'error_message': tb.get('error_message', ''),
            'file_location': tb.get('file_location', ''),
            'line_number': tb.get('line_number'),
            'traceback_start_line': tb.get('traceback_start_line'),
            'full_traceback': tb.get('full_traceback', '')
        })
    
    # Add standalone errors
    for err in errors:
        all_errors.append({
            'type': 'standalone',
            'error_type': err.get('error_type', 'Error'),
            'error_message': err.get('error_message', ''),
            'line_number': err.get('line_number'),
            'line_content': err.get('line_content', '')
        })
    
    # Categorize errors by severity (generic classification)
    critical_error_types = [
        'SyntaxError', 'IndentationError', 'ImportError', 'ModuleNotFoundError',
        'KeyError', 'AttributeError', 'NameError', 'TypeError', 'ValueError',
        'RuntimeError', 'FatalError', 'CriticalError'
    ]
    
    critical_errors = [
        err for err in all_errors
        if err.get('error_type') in critical_error_types
    ]
    
    return {
        'error_count': len(all_errors),
        'critical_error_count': len(critical_errors),
        'errors': all_errors,
        'critical_errors': critical_errors,
        'tracebacks': tracebacks
    }

def pack_logs_for_judge(
    command_output: str,
    success_criteria: Dict[str, Any] = None,
    max_summary_lines: int = 50,
    max_full_output_chars: int = 20000
) -> Dict[str, Any]:
    """
    Pack command output logs for Judge LLM.
    
    NOTE: Metric extraction and convergence analysis have been removed.
    The Judge LLM handles all metric extraction using ExperimentContract.
    This function only provides:
    - Error extraction (useful for highlighting issues)
    - Output truncation (to fit in prompts)
    
    Args:
        command_output: Full command output
        success_criteria: Deprecated - kept for backward compatibility, not used
        max_summary_lines: Deprecated - kept for backward compatibility, not used
        max_full_output_chars: Maximum characters to include in full_output (truncates if needed)
    
    Returns:
        {
            "error_summary": str,  # Highlighted summary of errors and tracebacks
            "full_output": str,  # Full output (truncated if needed)
            "error_info": Dict  # Error extraction results (for debugging)
        }
    """
    # Extract errors and tracebacks
    error_info = extract_errors(command_output)
    
    # Build error summary
    error_summary_lines = []
    error_summary_lines.append("\n❌ ERRORS AND TRACEBACKS DETECTED:")
    error_summary_lines.append("=" * 70)
    
    if error_info.get('error_count', 0) > 0:
        error_count = error_info.get('error_count', 0)
        critical_count = error_info.get('critical_error_count', 0)
        
        error_summary_lines.append(f"\nTotal errors detected: {error_count}")
        if critical_count > 0:
            error_summary_lines.append(f"⚠️  Critical errors: {critical_count}")
        
        # Show critical errors first
        critical_errors = error_info.get('critical_errors', [])
        if critical_errors:
            error_summary_lines.append("\n🔴 CRITICAL ERRORS (must be fixed):")
            for err in critical_errors[:10]:  # Show up to 10 critical errors
                error_type = err.get('error_type', 'UnknownError')
                error_message = err.get('error_message', '')
                file_location = err.get('file_location', '')
                line_number = err.get('line_number', '')
                
                error_summary_lines.append(f"\n  {error_type}: {error_message}")
                if file_location:
                    error_summary_lines.append(f"    Location: {file_location}" + (f":{line_number}" if line_number else ""))
                
                # Show first few lines of traceback if available
                if err.get('type') == 'traceback' and err.get('full_traceback'):
                    tb_lines = err['full_traceback'].split('\n')
                    # Show last 5 lines of traceback (usually contains the error)
                    for tb_line in tb_lines[-5:]:
                        if tb_line.strip():
                            error_summary_lines.append(f"    {tb_line}")
        
        # Show other errors
        all_errors = error_info.get('errors', [])
        non_critical_errors = [e for e in all_errors if e not in critical_errors]
        if non_critical_errors:
            error_summary_lines.append(f"\n⚠️  Other errors ({len(non_critical_errors)}):")
            for err in non_critical_errors[:5]:  # Show up to 5 other errors
                error_type = err.get('error_type', 'Error')
                error_message = (err.get('error_message') or '')[:100]  # Truncate long messages, handle None
                if error_message:
                    error_summary_lines.append(f"  - {error_type}: {error_message}")
    else:
        error_summary_lines.append("  ✅ No errors detected in output.")
    
    error_summary = "\n".join(error_summary_lines)
    
    # Truncate full output if needed
    full_output = command_output
    if len(full_output) > max_full_output_chars:
        # Keep last N characters (most recent output is most relevant)
        full_output = f"... [truncated, showing last {max_full_output_chars:,} characters of {len(command_output):,} total] ...\n\n" + full_output[-max_full_output_chars:]
    
    return {
        "error_summary": error_summary,
        "full_output": full_output,
        "error_info": error_info
    }

