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
    Extract Python tracebacks from command output.
    Returns list of dicts with traceback info: error_type, error_message, file_location, full_traceback.
    Generic - works for any Python error type.
    """
    lines = output.split('\n')
    tracebacks = []
    current_traceback = None
    traceback_lines = []
    in_traceback = False
    
    # Common Python error patterns (generic)
    error_pattern = re.compile(
        r'^(\w+Error|Exception|Warning|Error):\s*(.+)$',
        re.IGNORECASE
    )
    
    # Pattern to match file locations in tracebacks
    file_location_pattern = re.compile(
        r'File\s+["\']([^"\']+)["\'],\s+line\s+(\d+)',
        re.IGNORECASE
    )
    
    for i, line in enumerate(lines, 1):
        # Check if this line starts a traceback
        if line.strip().startswith('Traceback (most recent call last):'):
            # Save previous traceback if exists
            if current_traceback and traceback_lines:
                current_traceback['full_traceback'] = '\n'.join(traceback_lines)
                tracebacks.append(current_traceback)
            
            # Start new traceback
            current_traceback = {
                'error_type': None,
                'error_message': None,
                'file_location': None,
                'line_number': None,
                'full_traceback': None,
                'traceback_start_line': i
            }
            traceback_lines = [line]
            in_traceback = True
            continue
        
        if in_traceback:
            traceback_lines.append(line)
            
            # Check for file location
            file_match = file_location_pattern.search(line)
            if file_match and not current_traceback.get('file_location'):
                current_traceback['file_location'] = file_match.group(1)
                try:
                    current_traceback['line_number'] = int(file_match.group(2))
                except ValueError:
                    pass
            
            # Check for error type and message (usually the last line of traceback)
            error_match = error_pattern.match(line.strip())
            if error_match:
                current_traceback['error_type'] = error_match.group(1)
                current_traceback['error_message'] = error_match.group(2)
                # Traceback ends here
                in_traceback = False
                current_traceback['full_traceback'] = '\n'.join(traceback_lines)
                tracebacks.append(current_traceback)
                current_traceback = None
                traceback_lines = []
    
    # Handle case where traceback doesn't end with an error line
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
                # Generic error without specific type
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

