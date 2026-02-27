"""Signal extractors: parse tool outputs into structured signals on the Worker side.

These run BEFORE output crosses the wire to the Brain. The Brain receives
structured signals + a tail of raw output + a file reference.
"""
from __future__ import annotations

import json
import os
import re
from typing import Any, Dict, List, Optional


def extract_signals(stdout: str, stderr: str, exit_code: int) -> Dict[str, Any]:
    """Run all extractors over command output. Returns structured signals dict."""
    signals: Dict[str, Any] = {}

    test_results = extract_test_results(stdout + "\n" + stderr)
    if test_results:
        signals["test_results"] = test_results

    errors = extract_errors(stdout + "\n" + stderr)
    if errors:
        signals["errors"] = errors

    metrics = extract_metric_markers(stdout + "\n" + stderr)
    if metrics:
        signals["metrics"] = metrics

    tracebacks = extract_tracebacks(stderr + "\n" + stdout)
    if tracebacks:
        signals["tracebacks"] = tracebacks

    return signals


def extract_test_results(output: str) -> Optional[Dict[str, Any]]:
    """Parse test framework output (pytest, jest, go test, cargo test)."""
    # pytest: "5 passed, 2 failed, 1 error"
    m = re.search(
        r"=+\s*(?:(\d+)\s+passed)?"
        r"(?:,?\s*(\d+)\s+failed)?"
        r"(?:,?\s*(\d+)\s+error)?"
        r"(?:,?\s*(\d+)\s+warning)?"
        r"\s*(?:in\s+[\d.]+s)?\s*=+",
        output,
    )
    if not m and re.search(r"\d+\s+passed", output):
        m = re.search(r"(\d+)\s+passed(?:.*?(\d+)\s+failed)?(?:.*?(\d+)\s+error)?", output)

    if m:
        passed = int(m.group(1) or 0)
        failed = int(m.group(2) or 0)
        errors = int(m.group(3) or 0)
        total = passed + failed + errors
        result: Dict[str, Any] = {
            "framework": "pytest",
            "passed": passed,
            "failed": failed,
            "errors": errors,
            "total": total,
        }
        if total > 0:
            result["pass_rate"] = round(passed / total, 4)

        failures = []
        for fm in re.finditer(
            r"FAILED\s+([\w/.]+)::(\w+)",
            output,
        ):
            failures.append({"file": fm.group(1), "test": fm.group(2)})
        if failures:
            result["failures"] = failures[:20]
        return result

    # jest: "Tests: 2 failed, 5 passed, 7 total"
    jest_m = re.search(r"Tests:\s+(?:(\d+)\s+failed,\s+)?(\d+)\s+passed,\s+(\d+)\s+total", output)
    if jest_m:
        failed = int(jest_m.group(1) or 0)
        passed = int(jest_m.group(2))
        total = int(jest_m.group(3))
        return {
            "framework": "jest",
            "passed": passed,
            "failed": failed,
            "total": total,
            "pass_rate": round(passed / total, 4) if total > 0 else 0,
        }

    # go test: "ok" or "FAIL"
    go_ok = re.findall(r"^ok\s+\S+", output, re.MULTILINE)
    go_fail = re.findall(r"^FAIL\s+\S+", output, re.MULTILINE)
    if go_ok or go_fail:
        return {
            "framework": "go_test",
            "passed": len(go_ok),
            "failed": len(go_fail),
            "total": len(go_ok) + len(go_fail),
        }

    return None


def extract_errors(output: str) -> Optional[List[Dict[str, str]]]:
    """Extract compile/build errors."""
    errors = []

    # Python SyntaxError
    for m in re.finditer(
        r"File \"([^\"]+)\", line (\d+).*?\n\s*(.+)\n\s*\^+\n(\w+Error: .+)", output, re.DOTALL
    ):
        errors.append({
            "kind": "syntax_error",
            "file": m.group(1),
            "line": m.group(2),
            "message": m.group(4),
        })

    # TypeScript/JavaScript errors
    for m in re.finditer(r"([\w/.]+)\((\d+),(\d+)\):\s+error\s+(\w+):\s+(.+)", output):
        errors.append({
            "kind": "ts_error",
            "file": m.group(1),
            "line": m.group(2),
            "code": m.group(4),
            "message": m.group(5),
        })

    # GCC/Clang errors
    for m in re.finditer(r"([\w/.]+):(\d+):\d+:\s+error:\s+(.+)", output):
        errors.append({
            "kind": "compile_error",
            "file": m.group(1),
            "line": m.group(2),
            "message": m.group(3),
        })

    # Rust errors
    for m in re.finditer(r"error\[E\d+\]:\s+(.+)\n\s+-->\s+([\w/.]+):(\d+):\d+", output):
        errors.append({
            "kind": "rust_error",
            "file": m.group(2),
            "line": m.group(3),
            "message": m.group(1),
        })

    return errors[:20] if errors else None


def extract_metric_markers(output: str) -> Optional[Dict[str, Any]]:
    """Extract REMOROO_METRIC markers from output."""
    metrics: Dict[str, Any] = {}
    for m in re.finditer(r"REMOROO_METRIC\s+(\w+)\s*=\s*(.+)", output):
        name, value = m.group(1), m.group(2).strip()
        try:
            metrics[name] = float(value)
        except ValueError:
            metrics[name] = value
    return metrics if metrics else None


def extract_tracebacks(output: str) -> Optional[List[Dict[str, str]]]:
    """Extract Python tracebacks."""
    tracebacks = []
    tb_pattern = re.compile(
        r"Traceback \(most recent call last\):\n(.+?)\n(\w+(?:Error|Exception):\s*.+)",
        re.DOTALL,
    )
    for m in tb_pattern.finditer(output):
        stack = m.group(1).strip()
        error_line = m.group(2).strip()
        last_file = re.findall(r'File "([^"]+)", line (\d+)', stack)
        tb: Dict[str, str] = {"error": error_line}
        if last_file:
            tb["file"] = last_file[-1][0]
            tb["line"] = last_file[-1][1]
        tracebacks.append(tb)
    return tracebacks[:10] if tracebacks else None


def persist_output(
    run_id: str,
    seq: int,
    tool_name: str,
    stdout: str,
    stderr: str,
    base_dir: str,
) -> str:
    """Save full output to disk and return the file path."""
    output_dir = os.path.join(base_dir, ".remoroo", "runs", run_id, "outputs")
    os.makedirs(output_dir, exist_ok=True)
    filename = f"{seq:03d}_{tool_name}.txt"
    filepath = os.path.join(output_dir, filename)
    with open(filepath, "w", encoding="utf-8", errors="replace") as f:
        if stdout:
            f.write(stdout)
        if stderr:
            f.write("\n--- stderr ---\n")
            f.write(stderr)
    return filepath
