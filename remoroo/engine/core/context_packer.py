from __future__ import annotations
import os
import ast
import hashlib
import re
import csv
import json
import time
import random
import statistics
from typing import Dict, Any, List, Optional, Set, Tuple
from pathlib import Path

from jsonschema import validate
from ..utils.configs import DEFAULT_EXCLUDED_DIRS, DEFAULT_EXCLUDED_FILES
from ..utils.fs_utils import is_data_folder, ALWAYS_DATA_FOLDERS

# Summarizer version for cache invalidation
SUMMARIZER_VERSION = "1.0"

# Default excluded file extensions (compiled/binary artifacts)
DEFAULT_EXCLUDED_EXTENSIONS = {
    # Python bytecode
    ".pyc", ".pyo", ".pyd",
    # Compiled libraries
    ".so", ".dylib", ".dll",
    # Python packages
    ".egg-info",
    # Java
    ".class",
    # Object files
    ".o", ".obj",
    # Binaries
    ".exe", ".bin"
}

# Default excluded file patterns (wildcard matching)
DEFAULT_EXCLUDED_PATTERNS = [
    "*.pyc",
    "*.pyo",
    "*.pyd",
    "*.egg-info",
    "*.egg",
    "*.whl",
    "*.so",
    "*.dylib",
    "*.dll",
    "*.class",
    "*.o",
    "*.obj",
    "*.exe",
    "*.bin"
]

def _compute_sha256(file_path: str) -> str:
    """Compute SHA256 hash of a file."""
    sha256_hash = hashlib.sha256()
    with open(file_path, "rb") as f:
        for byte_block in iter(lambda: f.read(4096), b""):
            sha256_hash.update(byte_block)
    return sha256_hash.hexdigest()

def _should_include_file(
    relpath: str,
    repo_root: str,
    focus_files: List[str],
    deny_paths: List[str],
    deny_writing_data_folders: bool,
    allowed_data_folders: List[str]
) -> bool:
    """Determine if a file should be included in context pack."""
    abs_path = os.path.join(repo_root, relpath)
    
    # Always include focus files (even if in excluded directories)
    if relpath in focus_files:
        return True
    
    # Check deny paths (user-specified exclusions)
    for deny in deny_paths:
        if deny in relpath or deny in abs_path:
            return False
    
    # Check if path contains default excluded directories
    path_parts = relpath.split(os.sep)
    for part in path_parts:
        if part in DEFAULT_EXCLUDED_DIRS:
            return False
    
    # Additional check: exclude files in venv/ directories (even if venv is not in path parts directly)
    # This handles cases like "venv/lib/python3.10/site-packages/pkg_resources" or "remoroo_venvs/..."
    if "venv" in relpath.lower() or "remoroo_venvs" in relpath.lower() or "site-packages" in relpath.lower():
        return False
    
    # Check file extension against excluded extensions
    ext = os.path.splitext(relpath)[1].lower()
    if ext in DEFAULT_EXCLUDED_EXTENSIONS:
        return False
    
    # Check filename against excluded files
    filename = os.path.basename(relpath)
    if filename in DEFAULT_EXCLUDED_FILES:
        return False
    
    # Check filename against excluded patterns
    for pattern in DEFAULT_EXCLUDED_PATTERNS:
        # Simple pattern matching: convert "*.ext" to check if filename ends with ".ext"
        if pattern.startswith("*."):
            pattern_ext = pattern[1:]  # Remove "*"
            if filename.endswith(pattern_ext):
                return False
    
    # Additional check: exclude .DS_Store files explicitly
    if filename == ".DS_Store" or filename.endswith(".DS_Store"):
        return False
    
    # Additional check: exclude .pyc files with complex names (e.g., "setup.cpython-310.pyc")
    if ".pyc" in filename.lower() or filename.endswith(".pyc"):
        return False
    
    # Exclude benchmark case.json files (contain expected_outcome which should not be visible to LLMs)
    if filename == "case.json" or relpath.endswith("/case.json") or relpath.endswith("\\case.json"):
        return False
    
    # Check if it's a data folder (should be summarized, not included)
    if is_data_folder(abs_path, repo_root, allowed_data_folders):
        return False
    
    # Include common code/config/test files (Neutralized/Expanded for Polyglot support)
    if ext in [
        ".py", ".json", ".yaml", ".yml", ".toml", ".txt", ".md", ".sh", ".bash", ".zsh",
        ".js", ".ts", ".tsx", ".jsx", ".go", ".rs", ".java", 
        ".cpp", ".c", ".h", ".hpp", ".cc", ".cxx", ".hh", ".hxx",
        ".make", ".cmake", ".sql", ".proto", ".ini", ".cfg", ".xml"
    ]:
        return True
    
    return False

def _detect_file_format(file_path: str) -> str:
    """Detect file format/type based on extension and content."""
    ext = os.path.splitext(file_path)[1].lower()
    
    # Image formats
    if ext in ['.jpg', '.jpeg', '.png', '.gif', '.bmp', '.tiff', '.tif', '.webp', '.svg']:
        return 'image'
    
    # Data formats
    if ext in ['.csv', '.tsv']:
        return 'csv'
    if ext in ['.json', '.jsonl']:
        return 'json'
    if ext in ['.parquet']:
        return 'parquet'
    if ext in ['.h5', '.hdf5']:
        return 'hdf5'
    if ext in ['.pkl', '.pickle']:
        return 'pickle'
    if ext in ['.npz', '.npy']:
        return 'numpy'
    
    # Text formats
    if ext in ['.txt', '.md', '.log']:
        return 'text'
    
    # Archive formats
    if ext in ['.zip', '.tar', '.gz', '.bz2', '.xz']:
        return 'archive'
    
    # Video formats
    if ext in ['.mp4', '.avi', '.mov', '.mkv', '.webm']:
        return 'video'
    
    # Audio formats
    if ext in ['.mp3', '.wav', '.flac', '.ogg']:
        return 'audio'
    
    return 'unknown'

def _get_analysis_suggestions(file_format: str, path: str) -> List[str]:
    """
    Return generic analysis type suggestions based on file format.
    Used to guide LLMs on what kind of analysis might be useful for binary/large files.
    """
    suggestions_map = {
        'numpy': ['statistical_summary', 'shape_analysis', 'quality_assessment'],
        'pickle': ['content_summary', 'structure_analysis'],
        'hdf5': ['structure_analysis', 'dataset_summary'],
        'image': ['quality_assessment', 'metadata_extraction'],
        'image_folder': ['quality_assessment', 'distribution_analysis', 'error_detection'],
        'video': ['quality_assessment', 'metadata_extraction'],
        'audio': ['quality_assessment', 'metadata_extraction'],
        'archive': ['content_listing', 'structure_analysis'],
        'parquet': ['statistical_summary', 'schema_analysis'],
        'unknown': ['statistical_summary', 'content_analysis']
    }
    return suggestions_map.get(file_format, ['statistical_summary', 'content_analysis'])

def _extract_semantic_chunks(file_path: str, contents: str) -> List[Dict[str, Any]]:
    """
    Extract semantic chunks (imports, classes, functions) from a Python file using AST.
    Returns list of chunks with line ranges and metadata.
    """
    chunks = []
    
    try:
        tree = ast.parse(contents, filename=file_path)
    except (SyntaxError, ValueError):
        # If file is not valid Python, return as single chunk
        lines = contents.split('\n')
        return [{
            "type": "raw",
            "name": None,
            "content": contents,
            "line_range": [1, len(lines)],
            "relevance_score": 0.5  # Default score for non-Python files
        }]
    
    lines = contents.split('\n')
    
    class ChunkVisitor(ast.NodeVisitor):
        def __init__(self):
            self.chunks = []
            self.parent_stack = []  # Track parent nodes to detect nesting
            self.main_block_start = None  # Track if __name__ == '__main__' block
        
        def visit_Import(self, node):
            # Collect all imports at the top
            start_line = node.lineno
            end_line = node.end_lineno if hasattr(node, 'end_lineno') else node.lineno
            self.chunks.append({
                "type": "import",
                "name": None,
                "node": node,
                "line_range": [start_line, end_line],
                "relevance_score": 1.0  # Imports always included
            })
            self.generic_visit(node)
        
        def visit_ImportFrom(self, node):
            start_line = node.lineno
            end_line = node.end_lineno if hasattr(node, 'end_lineno') else node.lineno
            self.chunks.append({
                "type": "import",
                "name": node.module or "",
                "node": node,
                "line_range": [start_line, end_line],
                "relevance_score": 1.0  # Imports always included
            })
            self.generic_visit(node)
        
        def visit_If(self, node):
            # Check if this is `if __name__ == '__main__':`
            if isinstance(node.test, ast.Compare):
                if (isinstance(node.test.left, ast.Name) and 
                    node.test.left.id == '__name__' and
                    len(node.test.comparators) == 1 and
                    isinstance(node.test.comparators[0], ast.Constant) and
                    node.test.comparators[0].value == '__main__'):
                    # This is the main block - always include it (high priority for command generation)
                    start_line = node.lineno
                    end_line = node.end_lineno if hasattr(node, 'end_lineno') else node.lineno
                    self.chunks.append({
                        "type": "main_block",
                        "name": "__main__",
                        "node": node,
                        "line_range": [start_line, end_line],
                        "relevance_score": 1.0  # Main block always included (contains argparse, command entry points)
                    })
                    self.main_block_start = start_line
            self.generic_visit(node)
        
        def visit_ClassDef(self, node):
            self.parent_stack.append(node)
            start_line = node.lineno
            end_line = node.end_lineno if hasattr(node, 'end_lineno') else node.lineno
            self.chunks.append({
                "type": "class",
                "name": node.name,
                "node": node,
                "line_range": [start_line, end_line],
                "relevance_score": 0.5  # Default, will be scored later
            })
            self.generic_visit(node)
            self.parent_stack.pop()
        
        def visit_FunctionDef(self, node):
            # Check if we're inside a class or function (nested)
            is_nested = len(self.parent_stack) > 0 and isinstance(self.parent_stack[-1], (ast.ClassDef, ast.FunctionDef))
            
            if not is_nested:
                # Top-level function
                start_line = node.lineno
                end_line = node.end_lineno if hasattr(node, 'end_lineno') else node.lineno
                self.chunks.append({
                    "type": "function",
                    "name": node.name,
                    "node": node,
                    "line_range": [start_line, end_line],
                    "relevance_score": 0.5  # Default, will be scored later
                })
            else:
                # Method or nested function - include as part of parent class/function
                # We'll handle this by including the entire parent
                pass
            
            # Visit children (for nested functions)
            self.parent_stack.append(node)
            self.generic_visit(node)
            self.parent_stack.pop()
    
    visitor = ChunkVisitor()
    visitor.visit(tree)
    
    # Extract content for each chunk
    for chunk in visitor.chunks:
        start, end = chunk["line_range"]
        # Get lines (1-indexed, so subtract 1 for list index)
        chunk_lines = lines[start-1:end]
        chunk["content"] = '\n'.join(chunk_lines)
        # Remove node reference (not JSON serializable)
        chunk.pop("node", None)
    
    # If no chunks found (empty file or only comments), return full file
    if not visitor.chunks:
        return [{
            "type": "raw",
            "name": None,
            "content": contents,
            "line_range": [1, len(lines)],
            "relevance_score": 0.5
        }]
    
    return visitor.chunks


def _score_chunk_relevance(chunk: Dict[str, Any], goal: Optional[str] = None, focus_files: List[str] = None, file_path: str = None) -> float:
    """
    Score a chunk's relevance to the goal.
    Returns a score between 0.0 and 1.0.
    """
    # Imports always get max score
    if chunk["type"] == "import":
        return 1.0
    
    # Main block (if __name__ == '__main__') always gets max score
    # This contains argparse definitions and command entry points - critical for command generation
    if chunk["type"] == "main_block":
        return 1.0
    
    # Focus files get high score
    if focus_files and file_path and file_path in focus_files:
        return 0.9
    
    # If no goal provided, use default score
    if not goal:
        return chunk.get("relevance_score", 0.5)
    
    goal_lower = goal.lower()
    chunk_name = chunk.get("name", "").lower()
    chunk_content = chunk.get("content", "").lower()
    
    # Score based on name matching
    name_score = 0.0
    if chunk_name:
        # Extract keywords from goal
        goal_keywords = re.findall(r'\b\w+\b', goal_lower)
        for keyword in goal_keywords:
            if keyword in chunk_name:
                name_score += 0.3
            if keyword in chunk_content:
                name_score += 0.2
    
    # Score based on content matching
    content_score = 0.0
    goal_keywords = re.findall(r'\b\w+\b', goal_lower)
    for keyword in goal_keywords:
        if len(keyword) > 3:  # Ignore short words
            count = chunk_content.count(keyword)
            if count > 0:
                content_score += min(0.2, count * 0.05)
    
    # Combine scores
    total_score = min(1.0, name_score + content_score + 0.3)  # Base score of 0.3
    
    # Boost for certain types
    if chunk["type"] == "class":
        total_score = min(1.0, total_score + 0.1)
    elif chunk["type"] == "function":
        # Functions with goal-related names get higher score
        if any(keyword in chunk_name for keyword in goal_keywords if len(keyword) > 3):
            total_score = min(1.0, total_score + 0.2)
    
    return total_score


def _chunk_file_semantically(
    file_path: str,
    contents: str,
    goal: Optional[str] = None,
    focus_files: List[str] = None,
    max_chars_per_file: int = 50000,
    min_relevance_threshold: float = 0.3
) -> Dict[str, Any]:
    """
    Chunk a file semantically, including only relevant chunks.
    Returns a dict with 'chunks' (list) and 'metadata' (dict).
    """
    # Extract semantic chunks
    all_chunks = _extract_semantic_chunks(file_path, contents)
    
    # Score chunks
    for chunk in all_chunks:
        chunk["relevance_score"] = _score_chunk_relevance(chunk, goal, focus_files, file_path)
    
    # Sort by relevance (highest first)
    all_chunks.sort(key=lambda x: x["relevance_score"], reverse=True)
    
    # Select chunks to include
    included_chunks = []
    total_chars = 0
    included_imports = False
    
    for chunk in all_chunks:
        chunk_chars = len(chunk.get("content", ""))
        
        # Always include imports (but only once)
        if chunk["type"] == "import":
            if not included_imports:
                included_chunks.append(chunk)
                total_chars += chunk_chars
                included_imports = True
            continue
        
        # Always include main_block (if __name__ == '__main__') - contains argparse and entry points
        if chunk["type"] == "main_block":
            included_chunks.append(chunk)
            total_chars += chunk_chars
            continue
        
        # Check if we've exceeded the limit
        if total_chars + chunk_chars > max_chars_per_file:
            break
        
        # Include if above threshold or if it's a focus file
        if chunk["relevance_score"] >= min_relevance_threshold or (focus_files and file_path in focus_files):
            included_chunks.append(chunk)
            total_chars += chunk_chars
    
    # Sort included chunks by line number to preserve order
    included_chunks.sort(key=lambda x: x["line_range"][0])
    
    # Build final content from included chunks
    final_content = '\n'.join(chunk["content"] for chunk in included_chunks)
    
    # Metadata
    metadata = {
        "total_chunks": len(all_chunks),
        "included_chunks": len(included_chunks),
        "excluded_chunks": len(all_chunks) - len(included_chunks),
        "total_chars": total_chars,
        "original_chars": len(contents),
        "coverage": f"{len(included_chunks)}/{len(all_chunks)} chunks"
    }
    
    # If we included all chunks, return full content
    if len(included_chunks) == len(all_chunks):
        return {
            "path": file_path,
            "contents": contents,
            "chunked": False,
            "metadata": metadata
        }
    
    # Otherwise return chunked content
    return {
        "path": file_path,
        "contents": final_content,
        "chunked": True,
        "chunks": included_chunks,
        "metadata": metadata
    }


def _get_cache_path(repo_root: str, relpath: str, size_bytes: int, mtime_ns: int) -> str:
    """Get cache file path for a summary."""
    cache_dir = os.path.join(repo_root, ".remoroo_cache", "summaries")
    os.makedirs(cache_dir, exist_ok=True)
    
    # Create deterministic cache key
    cache_key = f"{relpath}_{size_bytes}_{mtime_ns}_{SUMMARIZER_VERSION}"
    cache_key_hash = hashlib.sha256(cache_key.encode()).hexdigest()[:16]
    safe_relpath = relpath.replace(os.sep, "_").replace("/", "_").replace("\\", "_")
    safe_relpath = "".join(c for c in safe_relpath if c.isalnum() or c in "._-")[:50]
    
    return os.path.join(cache_dir, f"{safe_relpath}_{cache_key_hash}.json")


def _load_cached_summary(cache_path: str) -> Optional[Dict[str, Any]]:
    """Load cached summary if it exists."""
    if os.path.exists(cache_path):
        try:
            with open(cache_path, 'r', encoding='utf-8') as f:
                return json.load(f)
        except (OSError, json.JSONDecodeError):
            pass
    return None


def _save_cached_summary(cache_path: str, summary: Dict[str, Any]) -> None:
    """Save summary to cache."""
    try:
        os.makedirs(os.path.dirname(cache_path), exist_ok=True)
        with open(cache_path, 'w', encoding='utf-8') as f:
            json.dump(summary, f, indent=2)
    except OSError:
        pass  # Cache failures are non-fatal


def summarize_csv(
    file_path: str,
    repo_root: str,
    budget_chars: int = 8000,
    seed: int = 42
) -> Dict[str, Any]:
    """
    Summarize a CSV file with bounded budget.
    Returns: {kind: "csv", path, summary (text), stats (dict), size_bytes, mtime}
    """
    abs_path = os.path.abspath(file_path)
    relpath = os.path.relpath(abs_path, repo_root)
    
    # Get file metadata
    stat = os.stat(abs_path)
    size_bytes = stat.st_size
    mtime_ns = stat.st_mtime_ns if hasattr(stat, 'st_mtime_ns') else int(stat.st_mtime * 1e9)
    
    # Check cache
    cache_path = _get_cache_path(repo_root, relpath, size_bytes, mtime_ns)
    cached = _load_cached_summary(cache_path)
    if cached:
        return cached
    
    summary_parts = []
    stats = {"size_bytes": size_bytes}
    chars_used = 0
    budget_remaining = budget_chars
    is_large_file = size_bytes > budget_chars * 100  # Flag for large files (skip detailed stats)
    
    try:
        # Always read first few lines to extract structure (bounded, ~few KB regardless of file size)
        with open(abs_path, 'r', encoding='utf-8', errors='ignore') as f:
            reader = csv.reader(f)
            
            # Read first row
            try:
                first_row = next(reader)
            except StopIteration:
                first_row = []
            
            if not first_row:
                summary_parts.append(f"Empty CSV file: {relpath}")
                result = {
                    "kind": "csv",
                    "path": relpath,
                    "summary": "\n".join(summary_parts),
                    "stats": stats,
                    "size_bytes": size_bytes,
                    "mtime": stat.st_mtime
                }
                _save_cached_summary(cache_path, result)
                return result
            
            # Detect if first row is header or data
            # Heuristic: if first row contains mostly non-numeric strings (or mix), likely header
            # If first row contains mostly numeric values, likely data (no header)
            has_header = False
            header = []
            
            # Check if first row looks like column names (has non-numeric strings, reasonable length)
            non_numeric_count = 0
            for cell in first_row[:10]:  # Check first 10 cells
                cell_str = str(cell).strip()
                if cell_str:
                    try:
                        float(cell_str)
                    except ValueError:
                        non_numeric_count += 1
            
            # If >30% of first 10 cells are non-numeric, likely header
            # Also check if values look like column names (short, alphanumeric, no long numeric strings)
            if non_numeric_count >= 3 or (len(first_row) > 0 and len(str(first_row[0])) < 50 and not str(first_row[0]).replace('.', '').replace('-', '').isdigit()):
                has_header = True
                header = first_row
                # Read next row as first data row
                try:
                    first_data_row = next(reader)
                except StopIteration:
                    first_data_row = []
            else:
                # No header, first row is data
                has_header = False
                header = []
                first_data_row = first_row
            
            # Read additional sample rows (bounded: max 5 rows total)
            sample_rows = [first_data_row] if first_data_row else []
            row_count = 0
            
            # Count rows for large files (estimate if needed)
            if is_large_file:
                # For large files, just read a few sample rows and estimate
                for i, row in enumerate(reader):
                    if i < 4:  # Get up to 4 more rows (5 total)
                        sample_rows.append(row)
                    row_count += 1
                    if i >= 100:  # Sample first 100 rows to estimate
                        break
                
                # Estimate total rows based on file size and average row size
                if sample_rows:
                    avg_row_size = sum(len(str(row)) for row in sample_rows) / len(sample_rows)
                    if avg_row_size > 0:
                        estimated_rows = int(size_bytes / avg_row_size)
                        stats["row_count"] = estimated_rows
                        stats["row_count_estimated"] = True
                    else:
                        stats["row_count"] = row_count
                else:
                    stats["row_count"] = 0
            else:
                # For smaller files, read more rows for better stats
                all_rows = []
                for i, row in enumerate(reader):
                    if i < 5:
                        sample_rows.append(row)
                    all_rows.append(row)
                    row_count += 1
                    if row_count >= 10000:
                        break
                
                if row_count >= 10000:
                    # Estimate based on file size
                    if all_rows:
                        avg_row_size = sum(len(str(row)) for row in all_rows[:100]) / min(100, len(all_rows))
                        estimated_rows = int(size_bytes / avg_row_size) if avg_row_size > 0 else row_count
                        stats["row_count"] = estimated_rows
                        stats["row_count_estimated"] = True
                    else:
                        stats["row_count"] = row_count
                else:
                    stats["row_count"] = row_count
            
            # Build summary - always include structure info
            summary_parts.append(f"CSV: {relpath}")
            summary_parts.append(f"Size: {size_bytes:,} bytes, Rows: {stats.get('row_count', 'unknown')}")
            
            # Column information - explicitly label header status
            num_columns = len(header) if header else (len(first_data_row) if first_data_row else 0)
            stats["num_columns"] = num_columns  # Always store column count
            
            if has_header and header:
                summary_parts.append(f"Structure: ✅ HAS HEADER ROW, {num_columns} columns")
                # Show column names prominently (truncated if many)
                if num_columns <= 30:
                    # Show all column names
                    header_str = ', '.join([f"'{col}'" for col in header])
                    summary_parts.append(f"COLUMN NAMES: {header_str}")
                    stats["column_names"] = header  # Store for easy access
                else:
                    # Show first 20 and indicate more
                    header_str = ', '.join([f"'{col}'" for col in header[:20]])
                    all_header_str = ', '.join([f"'{col}'" for col in header])
                    summary_parts.append(f"COLUMN NAMES ({num_columns} total): {header_str} ... and {num_columns - 20} more")
                    summary_parts.append(f"All column names: {all_header_str}")
                    stats["column_names"] = header  # Store for easy access
                stats["has_header"] = True
            else:
                summary_parts.append(f"Structure: ⚠️ NO HEADER ROW, {num_columns} columns")
                summary_parts.append(f"CRITICAL: This CSV has NO column names - all columns are positional (0-indexed)")
                summary_parts.append(f"DO NOT use column names like 'label', 'target', etc. - they don't exist!")
                summary_parts.append(f"Use positional access: data.iloc[:, 0] (first column), data.iloc[:, -1] (last column)")
                if num_columns > 0:
                    summary_parts.append(f"Or assign names after loading: data.columns = ['col0', 'col1', ..., 'col{num_columns-1}']")
                stats["has_header"] = False
                stats["columns_are_positional"] = True
            
            chars_used = len("\n".join(summary_parts))
            budget_remaining = budget_chars - chars_used
            
            # Infer types from sample rows (always do this, even for large files)
            if sample_rows and budget_remaining > 500:
                column_types = {}
                max_cols_to_check = min(20, num_columns)
                
                for col_idx in range(max_cols_to_check):
                    values = []
                    for row in sample_rows:
                        if col_idx < len(row):
                            values.append(str(row[col_idx]).strip())
                    
                    if not values:
                        continue
                    
                    # Try to infer type
                    col_type = "string"
                    numeric_count = 0
                    for val in values:
                        if val:
                            try:
                                float(val)
                                numeric_count += 1
                            except ValueError:
                                pass
                    
                    if numeric_count >= len(values) * 0.8:
                        col_type = "numeric"
                    elif len(set(values)) < len(values) * 0.5:
                        col_type = "categorical"
                    
                    if has_header and col_idx < len(header):
                        column_types[header[col_idx]] = col_type
                    else:
                        column_types[f"col_{col_idx}"] = col_type
                
                if column_types:
                    type_summary = ", ".join([f"{k}:{v}" for k, v in list(column_types.items())[:10]])
                    summary_parts.append(f"Column types (first {min(10, len(column_types))}): {type_summary}")
                    stats["column_types"] = column_types
                    chars_used = len("\n".join(summary_parts))
                    budget_remaining = budget_chars - chars_used
            
            # Sample rows - always show at least first row (bounded)
            if sample_rows and budget_remaining > 300:
                summary_parts.append("\nSample rows:")
                max_rows_to_show = 3 if not is_large_file else 2
                for i, row in enumerate(sample_rows[:max_rows_to_show]):
                    # Show first few columns, truncate long values
                    row_preview = []
                    max_cols_to_show = min(10, len(row))
                    for col_idx in range(max_cols_to_show):
                        cell_str = str(row[col_idx])[:30]  # Truncate each cell
                        row_preview.append(cell_str)
                    
                    row_str = ", ".join(row_preview)
                    if len(row) > max_cols_to_show:
                        row_str += f" ... ({len(row)} total columns)"
                    
                    summary_parts.append(f"  Row {i+1}: {row_str[:300]}")  # Truncate entire row
                    chars_used = len("\n".join(summary_parts))
                    budget_remaining = budget_chars - chars_used
                    if budget_remaining < 200:
                        break
            
            # Numeric stats (skip for large files to save time, but include for smaller files)
            if not is_large_file and budget_remaining > 2000:
                try:
                    import pandas as pd
                    # Read only first N rows for stats
                    df_sample = pd.read_csv(abs_path, nrows=1000)
                    numeric_cols = df_sample.select_dtypes(include=['number']).columns.tolist()[:20]
                    
                    if numeric_cols:
                        summary_parts.append(f"\nNumeric stats ({len(numeric_cols)} columns, first 1000 rows):")
                        for col in numeric_cols[:5]:  # Limit to 5 columns in summary
                            col_data = df_sample[col].dropna()
                            if len(col_data) > 0:
                                stats_text = f"  {col}: min={col_data.min():.2f}, max={col_data.max():.2f}, mean={col_data.mean():.2f}"
                                summary_parts.append(stats_text)
                                chars_used = len("\n".join(summary_parts))
                                budget_remaining = budget_chars - chars_used
                                if budget_remaining < 500:
                                    break
                except ImportError:
                    pass  # pandas not available, skip stats
                except Exception:
                    pass  # Error reading, skip stats
            elif is_large_file:
                summary_parts.append("\nNote: File is large, detailed numeric stats skipped (structure info above is sufficient)")
            
            # Truncate if over budget
            summary_text = "\n".join(summary_parts)
            if len(summary_text) > budget_chars:
                summary_text = summary_text[:budget_chars] + "... (truncated)"
    
    except Exception as e:
        summary_text = f"Error summarizing CSV {relpath}: {str(e)[:200]}"
        stats["error"] = str(e)[:200]
    
    result = {
        "kind": "csv",
        "path": relpath,
        "summary": summary_text,
        "stats": stats,
        "size_bytes": size_bytes,
        "mtime": stat.st_mtime
    }
    
    _save_cached_summary(cache_path, result)
    return result


def summarize_h5(file_path: str, repo_root: str, budget_chars: int = 3000) -> Dict[str, Any]:
    """
    Summarize an H5 file (Keras model or HDF5 data).
    Never loads weights, only architecture/config.
    """
    abs_path = os.path.abspath(file_path)
    relpath = os.path.relpath(abs_path, repo_root)
    
    stat = os.stat(abs_path)
    size_bytes = stat.st_size
    mtime_ns = stat.st_mtime_ns if hasattr(stat, 'st_mtime_ns') else int(stat.st_mtime * 1e9)
    
    # Check cache
    cache_path = _get_cache_path(repo_root, relpath, size_bytes, mtime_ns)
    cached = _load_cached_summary(cache_path)
    if cached:
        return cached
    
    summary_parts = []
    stats = {"size_bytes": size_bytes}
    
    summary_parts.append(f"H5 file: {relpath}")
    summary_parts.append(f"Size: {size_bytes:,} bytes")
    
    # Try to extract architecture/config (never load weights)
    try:
        # Try Keras first
        try:
            from tensorflow.keras.models import load_model
            # Load model without weights (this still loads architecture)
            # Actually, we can't load without weights in Keras, so we'll use h5py instead
            raise ImportError("Use h5py for safe inspection")
        except ImportError:
            pass
        
        # Use h5py for safe inspection
        try:
            import h5py
            with h5py.File(abs_path, 'r') as f:
                summary_parts.append(f"Groups/keys: {list(f.keys())[:10]}")
                
                # Check if it's a Keras model
                if 'model_weights' in f or 'keras_version' in f.attrs:
                    summary_parts.append("Type: Keras model")
                    if 'model_config' in f.attrs:
                        try:
                            import json
                            config = json.loads(f.attrs['model_config'])
                            if 'config' in config:
                                layers = config['config'].get('layers', [])
                                layer_names = [l.get('class_name', 'Unknown') for l in layers[:10]]
                                summary_parts.append(f"Layers ({len(layers)}): {', '.join(layer_names)}")
                                if len(layers) > 10:
                                    summary_parts.append(f"... and {len(layers) - 10} more layers")
                        except Exception:
                            pass
                
                # List top-level groups
                groups = list(f.keys())[:5]
                if groups:
                    summary_parts.append(f"Top-level groups: {', '.join(groups)}")
        except ImportError:
            summary_parts.append("h5py not available - cannot inspect structure")
        except Exception as e:
            summary_parts.append(f"Error inspecting H5: {str(e)[:200]}")
            stats["error"] = str(e)[:200]
    
    except Exception as e:
        summary_parts.append(f"Error summarizing H5: {str(e)[:200]}")
        stats["error"] = str(e)[:200]
    
    summary_text = "\n".join(summary_parts)
    if len(summary_text) > budget_chars:
        summary_text = summary_text[:budget_chars] + "... (truncated)"
    
    result = {
        "kind": "h5",
        "path": relpath,
        "summary": summary_text,
        "stats": stats,
        "size_bytes": size_bytes,
        "mtime": stat.st_mtime
    }
    
    _save_cached_summary(cache_path, result)
    return result


def summarize_json(
    file_path: str,
    repo_root: str,
    budget_chars: int = 5000
) -> Dict[str, Any]:
    """
    Summarize a JSON file (config or data).
    Extracts structure, keys, types, and sample values.
    Returns: {kind: "json", path, summary (text), stats (dict), size_bytes, mtime}
    """
    abs_path = os.path.abspath(file_path)
    relpath = os.path.relpath(abs_path, repo_root)
    
    stat = os.stat(abs_path)
    size_bytes = stat.st_size
    mtime_ns = stat.st_mtime_ns if hasattr(stat, 'st_mtime_ns') else int(stat.st_mtime * 1e9)
    
    # Check cache
    cache_path = _get_cache_path(repo_root, relpath, size_bytes, mtime_ns)
    cached = _load_cached_summary(cache_path)
    if cached:
        return cached
    
    summary_parts = []
    stats = {"size_bytes": size_bytes}
    
    summary_parts.append(f"JSON file: {relpath}")
    summary_parts.append(f"Size: {size_bytes:,} bytes")
    
    try:
        with open(abs_path, 'r', encoding='utf-8', errors='ignore') as f:
            content = f.read()
            
        data = json.loads(content)
        
        # Detect structure type
        if isinstance(data, dict):
            stats["type"] = "object"
            stats["key_count"] = len(data)
            summary_parts.append(f"Type: Object (dictionary) with {len(data)} top-level keys")
            
            # List top-level keys (truncate if many)
            keys = list(data.keys())[:20]
            if len(keys) <= 20:
                summary_parts.append(f"Keys: {', '.join(str(k) for k in keys)}")
            else:
                summary_parts.append(f"Keys ({len(data)}): {', '.join(str(k) for k in keys)} ... and {len(data) - 20} more")
            
            # Show structure (types of values)
            key_types = {}
            for key in keys[:15]:  # Check first 15 keys
                value = data[key]
                if isinstance(value, dict):
                    key_types[key] = f"object({len(value)} keys)"
                elif isinstance(value, list):
                    key_types[key] = f"array({len(value)} items)"
                elif isinstance(value, (int, float)):
                    key_types[key] = "number"
                elif isinstance(value, bool):
                    key_types[key] = "boolean"
                elif isinstance(value, str):
                    key_types[key] = f"string({len(value)} chars)"
                else:
                    key_types[key] = type(value).__name__
            
            if key_types:
                type_summary = ", ".join([f"{k}:{v}" for k, v in list(key_types.items())[:10]])
                summary_parts.append(f"Key types (first {min(10, len(key_types))}): {type_summary}")
                stats["key_types"] = key_types
            
            # Show sample values for important keys (short strings, small numbers)
            sample_values = {}
            for key in keys[:10]:
                value = data[key]
                if isinstance(value, (str, int, float, bool)) or value is None:
                    val_str = str(value)
                    if len(val_str) <= 50:
                        sample_values[key] = val_str
                    else:
                        sample_values[key] = val_str[:47] + "..."
                elif isinstance(value, list) and len(value) > 0:
                    first_item = value[0]
                    if isinstance(first_item, (str, int, float, bool)):
                        sample_values[key] = f"[{first_item}, ...] ({len(value)} items)"
            
            if sample_values and len("\n".join(summary_parts)) < budget_chars - 500:
                summary_parts.append(f"\nSample values:")
                for key, val in list(sample_values.items())[:5]:
                    summary_parts.append(f"  {key}: {val}")
        
        elif isinstance(data, list):
            stats["type"] = "array"
            stats["item_count"] = len(data)
            summary_parts.append(f"Type: Array (list) with {len(data)} items")
            
            # Show item types
            if len(data) > 0:
                first_item = data[0]
                item_type = type(first_item).__name__
                summary_parts.append(f"Item type: {item_type}")
                
                # If items are objects, show structure of first item
                if isinstance(first_item, dict) and len(first_item) > 0:
                    first_keys = list(first_item.keys())[:10]
                    summary_parts.append(f"First item keys: {', '.join(str(k) for k in first_keys)}")
        
        else:
            # Primitive value (string, number, boolean, null)
            stats["type"] = type(data).__name__
            summary_parts.append(f"Type: {stats['type']}")
            if isinstance(data, str) and len(data) <= 200:
                summary_parts.append(f"Value: {data}")
            elif not isinstance(data, str):
                summary_parts.append(f"Value: {data}")
    
    except json.JSONDecodeError as e:
        summary_parts.append(f"⚠️ Invalid JSON: {str(e)[:200]}")
        stats["error"] = str(e)[:200]
    except Exception as e:
        summary_parts.append(f"Error reading JSON: {str(e)[:200]}")
        stats["error"] = str(e)[:200]
    
    summary_text = "\n".join(summary_parts)
    if len(summary_text) > budget_chars:
        summary_text = summary_text[:budget_chars] + "... (truncated)"
    
    result = {
        "kind": "json",
        "path": relpath,
        "summary": summary_text,
        "stats": stats,
        "size_bytes": size_bytes,
        "mtime": stat.st_mtime
    }
    
    _save_cached_summary(cache_path, result)
    return result


def summarize_jsonl(
    file_path: str,
    repo_root: str,
    budget_chars: int = 5000
) -> Dict[str, Any]:
    """
    Summarize a JSONL file (line-delimited JSON, each line is a JSON object).
    Extracts structure from first few lines to understand schema.
    Returns: {kind: "jsonl", path, summary (text), stats (dict), size_bytes, mtime}
    """
    abs_path = os.path.abspath(file_path)
    relpath = os.path.relpath(abs_path, repo_root)
    
    stat = os.stat(abs_path)
    size_bytes = stat.st_size
    mtime_ns = stat.st_mtime_ns if hasattr(stat, 'st_mtime_ns') else int(stat.st_mtime * 1e9)
    
    # Check cache
    cache_path = _get_cache_path(repo_root, relpath, size_bytes, mtime_ns)
    cached = _load_cached_summary(cache_path)
    if cached:
        return cached
    
    summary_parts = []
    stats = {"size_bytes": size_bytes}
    
    summary_parts.append(f"JSONL file (line-delimited JSON): {relpath}")
    summary_parts.append(f"Size: {size_bytes:,} bytes")
    
    try:
        # Read first few lines to understand structure
        sample_objects = []
        line_count = 0
        with open(abs_path, 'r', encoding='utf-8', errors='ignore') as f:
            for i, line in enumerate(f):
                if i >= 10:  # Sample first 10 lines
                    break
                line = line.strip()
                if not line:
                    continue
                try:
                    obj = json.loads(line)
                    sample_objects.append(obj)
                    line_count += 1
                except json.JSONDecodeError:
                    pass
        
        # Count total lines (estimate for large files)
        if line_count < 10:
            # File is small, count all lines
            with open(abs_path, 'r', encoding='utf-8', errors='ignore') as f:
                total_lines = sum(1 for line in f if line.strip())
        else:
            # Estimate based on file size and average line length
            avg_line_size = size_bytes / (line_count + 1)
            total_lines = int(size_bytes / avg_line_size) if avg_line_size > 0 else line_count
            stats["line_count_estimated"] = True
        
        stats["total_lines"] = total_lines
        summary_parts.append(f"Lines (JSON objects): ~{total_lines}")
        
        # Analyze structure from sample objects
        if sample_objects:
            # Check if all objects have same structure (same keys)
            first_obj = sample_objects[0]
            if isinstance(first_obj, dict):
                all_keys = set(first_obj.keys())
                consistent_keys = True
                for obj in sample_objects[1:]:
                    if isinstance(obj, dict):
                        if set(obj.keys()) != all_keys:
                            consistent_keys = False
                            all_keys.update(obj.keys())
                    else:
                        consistent_keys = False
                
                stats["type"] = "object_per_line"
                stats["key_count"] = len(all_keys)
                summary_parts.append(f"Structure: Object per line, {len(all_keys)} unique keys")
                
                if consistent_keys:
                    summary_parts.append(f"✅ Consistent schema across lines")
                    keys_list = sorted(list(all_keys))[:20]
                    summary_parts.append(f"Keys: {', '.join(str(k) for k in keys_list)}")
                    if len(all_keys) > 20:
                        summary_parts.append(f"... and {len(all_keys) - 20} more keys")
                else:
                    summary_parts.append(f"⚠️ Schema varies across lines (union of all keys shown)")
                    keys_list = sorted(list(all_keys))[:20]
                    summary_parts.append(f"All keys (union): {', '.join(str(k) for k in keys_list)}")
                    if len(all_keys) > 20:
                        summary_parts.append(f"... and {len(all_keys) - 20} more keys")
                
                # Show key types from first object
                key_types = {}
                for key in list(all_keys)[:15]:
                    value = first_obj.get(key)
                    if isinstance(value, dict):
                        key_types[key] = f"object({len(value)} keys)"
                    elif isinstance(value, list):
                        key_types[key] = f"array({len(value)} items)"
                    elif isinstance(value, (int, float)):
                        key_types[key] = "number"
                    elif isinstance(value, bool):
                        key_types[key] = "boolean"
                    elif isinstance(value, str):
                        key_types[key] = f"string({len(value)} chars)"
                    else:
                        key_types[key] = type(value).__name__
                
                if key_types:
                    type_summary = ", ".join([f"{k}:{v}" for k, v in list(key_types.items())[:10]])
                    summary_parts.append(f"Key types (from first line): {type_summary}")
                    stats["key_types"] = key_types
                
                # Show sample values
                if len(summary_parts) < budget_chars - 300:
                    summary_parts.append(f"\nSample values (first line):")
                    for key in list(all_keys)[:5]:
                        val = first_obj.get(key)
                        if isinstance(val, (str, int, float, bool)) or val is None:
                            val_str = str(val)
                            if len(val_str) <= 50:
                                summary_parts.append(f"  {key}: {val_str}")
            
            elif isinstance(first_obj, list):
                stats["type"] = "array_per_line"
                summary_parts.append(f"Structure: Array per line (variable length)")
                if len(first_obj) > 0:
                    summary_parts.append(f"First line array length: {len(first_obj)}, item type: {type(first_obj[0]).__name__}")
            else:
                stats["type"] = type(first_obj).__name__
                summary_parts.append(f"Structure: {stats['type']} per line")
    
    except Exception as e:
        summary_parts.append(f"Error reading JSONL: {str(e)[:200]}")
        stats["error"] = str(e)[:200]
    
    summary_text = "\n".join(summary_parts)
    if len(summary_text) > budget_chars:
        summary_text = summary_text[:budget_chars] + "... (truncated)"
    
    result = {
        "kind": "jsonl",
        "path": relpath,
        "summary": summary_text,
        "stats": stats,
        "size_bytes": size_bytes,
        "mtime": stat.st_mtime
    }
    
    _save_cached_summary(cache_path, result)
    return result


def summarize_yaml(
    file_path: str,
    repo_root: str,
    budget_chars: int = 5000
) -> Dict[str, Any]:
    """
    Summarize a YAML file (config or data).
    Extracts structure similar to JSON.
    Returns: {kind: "yaml", path, summary (text), stats (dict), size_bytes, mtime}
    """
    abs_path = os.path.abspath(file_path)
    relpath = os.path.relpath(abs_path, repo_root)
    
    stat = os.stat(abs_path)
    size_bytes = stat.st_size
    mtime_ns = stat.st_mtime_ns if hasattr(stat, 'st_mtime_ns') else int(stat.st_mtime * 1e9)
    
    # Check cache
    cache_path = _get_cache_path(repo_root, relpath, size_bytes, mtime_ns)
    cached = _load_cached_summary(cache_path)
    if cached:
        return cached
    
    summary_parts = []
    stats = {"size_bytes": size_bytes}
    
    summary_parts.append(f"YAML file: {relpath}")
    summary_parts.append(f"Size: {size_bytes:,} bytes")
    
    try:
        try:
            import yaml
        except ImportError:
            # If yaml not available, try to read as text
            with open(abs_path, 'r', encoding='utf-8', errors='ignore') as f:
                lines = f.readlines()[:20]  # Read first 20 lines
                summary_parts.append(f"YAML parser not available, showing first 20 lines:")
                summary_parts.append("".join(lines[:20]))
                if len(lines) > 20:
                    summary_parts.append(f"... ({len(lines)} total lines)")
                summary_text = "\n".join(summary_parts)
                if len(summary_text) > budget_chars:
                    summary_text = summary_text[:budget_chars] + "... (truncated)"
                result = {
                    "kind": "yaml",
                    "path": relpath,
                    "summary": summary_text,
                    "stats": stats,
                    "size_bytes": size_bytes,
                    "mtime": stat.st_mtime
                }
                _save_cached_summary(cache_path, result)
                return result
        
        with open(abs_path, 'r', encoding='utf-8', errors='ignore') as f:
            data = yaml.safe_load(f)
        
        # Similar structure detection as JSON
        if isinstance(data, dict):
            stats["type"] = "object"
            stats["key_count"] = len(data)
            summary_parts.append(f"Type: Object (dictionary) with {len(data)} top-level keys")
            
            keys = list(data.keys())[:20]
            if len(keys) <= 20:
                summary_parts.append(f"Keys: {', '.join(str(k) for k in keys)}")
            else:
                summary_parts.append(f"Keys ({len(data)}): {', '.join(str(k) for k in keys)} ... and {len(data) - 20} more")
            
            # Show key types
            key_types = {}
            for key in keys[:15]:
                value = data[key]
                if isinstance(value, dict):
                    key_types[key] = f"object({len(value)} keys)"
                elif isinstance(value, list):
                    key_types[key] = f"array({len(value)} items)"
                elif isinstance(value, (int, float)):
                    key_types[key] = "number"
                elif isinstance(value, bool):
                    key_types[key] = "boolean"
                elif isinstance(value, str):
                    key_types[key] = f"string({len(value)} chars)"
                else:
                    key_types[key] = type(value).__name__
            
            if key_types:
                type_summary = ", ".join([f"{k}:{v}" for k, v in list(key_types.items())[:10]])
                summary_parts.append(f"Key types (first {min(10, len(key_types))}): {type_summary}")
                stats["key_types"] = key_types
        
        elif isinstance(data, list):
            stats["type"] = "array"
            stats["item_count"] = len(data)
            summary_parts.append(f"Type: Array (list) with {len(data)} items")
            if len(data) > 0:
                first_item = data[0]
                summary_parts.append(f"Item type: {type(first_item).__name__}")
        
        else:
            stats["type"] = type(data).__name__
            summary_parts.append(f"Type: {stats['type']}")
            if isinstance(data, str) and len(data) <= 200:
                summary_parts.append(f"Value: {data}")
    
    except Exception as e:
        summary_parts.append(f"Error reading YAML: {str(e)[:200]}")
        stats["error"] = str(e)[:200]
    
    summary_text = "\n".join(summary_parts)
    if len(summary_text) > budget_chars:
        summary_text = summary_text[:budget_chars] + "... (truncated)"
    
    result = {
        "kind": "yaml",
        "path": relpath,
        "summary": summary_text,
        "stats": stats,
        "size_bytes": size_bytes,
        "mtime": stat.st_mtime
    }
    
    _save_cached_summary(cache_path, result)
    return result


def summarize_image_folder(folder_path: str, repo_root: str, budget_chars: int = 4000) -> Dict[str, Any]:
    """
    Summarize an image folder: structure, formats, counts, dimensions (if available).
    Returns: {kind: "image_folder", path, summary (text), stats (dict)}
    """
    abs_folder = os.path.join(repo_root, folder_path)
    if not os.path.isdir(abs_folder):
        return {
            "kind": "image_folder",
            "folder_path": folder_path,
            "summary": f"Image folder not found: {folder_path}",
            "stats": {}
        }
    
    summary_parts = []
    stats = {"folder_path": folder_path}
    
    # Collect image files
    image_extensions = {'.jpg', '.jpeg', '.png', '.gif', '.bmp', '.tiff', '.tif', '.webp', '.svg'}
    image_files = []
    format_counts = {}
    
    for root, dirs, filenames in os.walk(abs_folder):
        # Skip excluded directories (use centralized config)
        dirs[:] = [d for d in dirs if d not in DEFAULT_EXCLUDED_DIRS]
        
        for filename in filenames:
            ext = os.path.splitext(filename)[1].lower()
            if ext in image_extensions:
                relpath = os.path.relpath(os.path.join(root, filename), repo_root)
                image_files.append({
                    "path": relpath,
                    "filename": filename,
                    "ext": ext,
                    "size": os.path.getsize(os.path.join(root, filename))
                })
                format_counts[ext] = format_counts.get(ext, 0) + 1
    
    stats["total_images"] = len(image_files)
    stats["formats"] = format_counts
    
    summary_parts.append(f"Image folder: {folder_path}")
    summary_parts.append(f"Total images: {len(image_files)}")
    
    # Show format breakdown
    if format_counts:
        format_list = [f"{count} {fmt}" for fmt, count in sorted(format_counts.items())]
        summary_parts.append(f"Formats: {', '.join(format_list)}")
    
    # Show folder structure (first 2 levels)
    if len(image_files) > 0:
        # Group by subdirectory
        by_dir = {}
        for img in image_files:
            dir_path = os.path.dirname(img["path"])
            if dir_path == folder_path or dir_path == ".":
                dir_key = "root"
            else:
                # Get relative subdirectory
                dir_key = os.path.relpath(dir_path, folder_path)
                dir_key = dir_key.split(os.sep)[0]  # First level only
            
            if dir_key not in by_dir:
                by_dir[dir_key] = []
            by_dir[dir_key].append(img)
        
        summary_parts.append(f"\nFolder structure ({len(by_dir)} subdirectories/root):")
        for dir_key in sorted(by_dir.keys())[:10]:  # Show first 10 directories
            count = len(by_dir[dir_key])
            dir_name = dir_key if dir_key != "root" else folder_path
            summary_parts.append(f"  {dir_name}: {count} image(s)")
            if len(by_dir) > 10:
                remaining = sum(len(by_dir[k]) for k in list(by_dir.keys())[10:])
                summary_parts.append(f"  ... and {remaining} more images in other directories")
                break
    
    # Try to get image dimensions for a few sample images (if PIL available)
    if len(image_files) > 0 and len(summary_parts) < budget_chars - 500:
        try:
            from PIL import Image
            sample_count = min(3, len(image_files))
            dimensions = []
            for img in image_files[:sample_count]:
                try:
                    img_path = os.path.join(repo_root, img["path"])
                    with Image.open(img_path) as im:
                        dimensions.append(f"{os.path.basename(img['path'])}: {im.width}x{im.height}")
                except Exception:
                    pass
            
            if dimensions:
                summary_parts.append(f"\nSample dimensions (first {sample_count}):")
                for dim in dimensions:
                    summary_parts.append(f"  {dim}")
                stats["sample_dimensions"] = dimensions
        except ImportError:
            pass  # PIL not available
    
    summary_text = "\n".join(summary_parts)
    if len(summary_text) > budget_chars:
        summary_text = summary_text[:budget_chars] + "... (truncated)"
    
    result = {
        "kind": "image_folder",
        "folder_path": folder_path,
        "summary": summary_text,
        "stats": stats
    }
    
    return result


def _summarize_data_folder(folder_path: str, repo_root: str) -> Dict[str, Any]:
    """Summarize a data folder without embedding contents, including format detection and sample content."""
    abs_folder = os.path.join(repo_root, folder_path)
    if not os.path.isdir(abs_folder):
        return {
            "folder_path": folder_path,
            "files": [],
            "total_bytes": 0,
            "formats": {}
        }
    
    files = []
    total_bytes = 0
    formats = {}  # Count files by format
    
    for root, dirs, filenames in os.walk(abs_folder):
        for filename in filenames:
            file_path = os.path.join(root, filename)
            relpath = os.path.relpath(file_path, repo_root)
            try:
                size = os.path.getsize(file_path)
                file_format = _detect_file_format(file_path)
                
                # Extract sample content for text-based formats (for command generation)
                sample_content = None
                if file_format in ['csv', 'json', 'jsonl', 'text'] and size < 500000:  # Only for reasonably-sized text files
                    try:
                        with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
                            if file_format == 'csv':
                                # Read first line (header) and maybe one data line
                                first_line = f.readline().strip()
                                if first_line:
                                    # Truncate if too long (e.g., pixel data)
                                    if len(first_line) > 200:
                                        sample_content = first_line[:200] + "... (truncated, contains pixel/numeric data)"
                                    else:
                                        second_line = f.readline().strip()
                                        if second_line and len(second_line) < 200:
                                            sample_content = f"{first_line}\n{second_line[:200]}"
                                        else:
                                            sample_content = first_line
                            elif file_format in ['json', 'jsonl']:
                                # Read first line (or first 500 chars)
                                first_line = f.readline().strip()
                                if first_line:
                                    sample_content = first_line[:500] + ("..." if len(first_line) > 500 else "")
                            elif file_format == 'text':
                                # Read first 3 lines
                                lines = [f.readline().strip() for _ in range(3)]
                                lines = [l for l in lines if l]
                                if lines:
                                    sample_content = '\n'.join(lines[:3])
                                    if len(sample_content) > 500:
                                        sample_content = sample_content[:500] + "..."
                    except (OSError, UnicodeDecodeError):
                        pass  # Skip if can't read
                
                # Generate command-usable path (relative to repo root, can be used in --data_path, etc.)
                # Use ./ prefix for clarity in commands
                command_path = f"./{relpath}" if not relpath.startswith('./') else relpath
                
                # Determine if this file requires code-based analysis (binary formats or large files)
                binary_formats = {'numpy', 'pickle', 'hdf5', 'image', 'video', 'audio', 'archive'}
                requires_analysis = (file_format in binary_formats) or (size > 10000000)  # >10MB
                
                file_entry = {
                    "relpath": relpath,
                    "command_path": command_path,  # Path usable in command arguments
                    "size_bytes": size,
                    "format": file_format,
                    "sample_content": sample_content,  # First few lines for text formats
                    "requires_analysis": requires_analysis
                }
                
                # Add analysis suggestions for files that require analysis
                if requires_analysis:
                    file_entry["analysis_suggestions"] = _get_analysis_suggestions(file_format, relpath)
                
                files.append(file_entry)
                total_bytes += size
                
                # Count formats
                formats[file_format] = formats.get(file_format, 0) + 1
            except OSError:
                pass
    
    return {
        "folder_path": folder_path,
        "files": files,
        "total_bytes": total_bytes,
        "formats": formats,
        "file_count": len(files)
    }

def _extract_error_summaries_from_outcomes(outcomes: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Extract concise error summaries from command outcomes.
    Returns list of error summaries with: error_type, error_message, affected_file, cmd, exit_code.
    """
    from ..utils.log_packer import extract_errors
    
    error_summaries = []
    seen_errors = set()  # Track unique errors to avoid duplicates
    
    for outcome in outcomes:
        exit_code = outcome.get("exit_code", 0)
        stderr = outcome.get("stderr", "")
        stdout = outcome.get("stdout", "")
        cmd = outcome.get("cmd", "")
        
        # Only process failed commands (non-zero exit code)
        if exit_code == 0:
            continue
        
        # Combine stderr and stdout for error detection
        combined_output = stderr + "\n" + stdout if stderr else stdout
        
        if not combined_output.strip():
            continue
        
        # Extract errors using log_packer (local copy in utils)
        error_info = extract_errors(combined_output)
        critical_errors = error_info.get("critical_errors", [])
        all_errors = error_info.get("errors", [])
        
        # Process critical errors first (most important)
        for err in critical_errors[:3]:  # Limit to top 3 critical errors per command
            error_type = err.get("error_type", "Error")
            error_message = err.get("error_message") or ""  # Handle None
            file_location = err.get("file_location", "")
            
            # Create unique key to avoid duplicates
            error_key = f"{error_type}:{error_message[:50]}"
            if error_key in seen_errors:
                continue
            seen_errors.add(error_key)
            
            # Extract affected file from file_location or cmd
            affected_file = None
            if file_location:
                # Extract just the filename from full path
                affected_file = os.path.basename(file_location)
            elif cmd:
                # Try to extract file from command (e.g., "python <file>.py" -> "<file>.py")
                import re
                match = re.search(r'(\w+\.py)', cmd)
                if match:
                    affected_file = match.group(1)
            
            # Truncate error message to keep it concise
            error_message_short = error_message[:200] if len(error_message) > 200 else error_message
            
            error_summaries.append({
                "error_type": error_type,
                "error_message": error_message_short,
                "affected_file": affected_file,
                "cmd": cmd[:100] if cmd else None,  # Truncate command
                "exit_code": exit_code,
                "source": "stderr" if stderr else "stdout"
            })
        
        # If no critical errors, look for any errors
        if not critical_errors and all_errors:
            for err in all_errors[:2]:  # Limit to top 2 non-critical errors per command
                error_type = err.get("error_type", "Error")
                error_message = err.get("error_message") or ""  # Handle None
                
                error_key = f"{error_type}:{error_message[:50]}"
                if error_key in seen_errors:
                    continue
                seen_errors.add(error_key)
                
                # Extract affected file
                affected_file = None
                if cmd:
                    import re
                    match = re.search(r'(\w+\.py)', cmd)
                    if match:
                        affected_file = match.group(1)
                
                error_message_short = error_message[:200] if len(error_message) > 200 else error_message
                
                error_summaries.append({
                    "error_type": error_type,
                    "error_message": error_message_short,
                    "affected_file": affected_file,
                    "cmd": cmd[:100] if cmd else None,
                    "exit_code": exit_code,
                    "source": "stderr" if stderr else "stdout"
                })

    return error_summaries[:10]  # Limit to top 10 errors total


def build_context_pack(
    repo_root: str,
    turn_index: int,
    focus_files: List[str],
    previous_context_pack: Optional[Dict[str, Any]] = None,
    max_files: int = 25,
    max_total_bytes: int = 200000,
    max_total_chars: int = 200000,
    deny_paths: List[str] = None,
    deny_writing_data_folders: bool = True,
    allowed_data_folders: List[str] = None,
    goal: Optional[str] = None,
    use_semantic_chunking: bool = True,
    max_chars_per_file: int = 50000,
    min_relevance_threshold: float = 0.3,
    previous_turn_outcomes: Optional[List[Dict[str, Any]]] = None,
    max_code_chars: Optional[int] = None,
    max_data_chars: Optional[int] = None,
    system_diagram: Optional[Dict[str, Any]] = None
) -> Dict[str, Any]:
    """
    Build a context pack for a turn.
    
    Args:
        previous_turn_outcomes: List of command outcomes from previous turn (for error extraction)
        max_code_chars: Maximum characters for code files (defaults to max_total_chars * 0.8)
        max_data_chars: Maximum characters for data summaries (defaults to max_total_chars * 0.2)
    """
    build_start_time = time.time()
    deny_paths = deny_paths or []
    allowed_data_folders = allowed_data_folders or []
    
    # Set up two-bucket budgets
    if max_code_chars is None:
        max_code_chars = int(max_total_chars * 0.8)  # 80% for code
    if max_data_chars is None:
        max_data_chars = int(max_total_chars * 0.2)  # 20% for data
    
    # Normalize allowed_data_folders - strip repo name prefix if present
    normalized_allowed_folders = []
    repo_name = os.path.basename(os.path.abspath(repo_root))
    for allowed in allowed_data_folders:
        # If path starts with repo name, strip it
        if allowed.startswith(repo_name + "/") or allowed.startswith(repo_name + os.sep):
            normalized = allowed[len(repo_name)+1:]
            normalized_allowed_folders.append(normalized)
        else:
            normalized_allowed_folders.append(allowed)
    allowed_data_folders = normalized_allowed_folders
    
    files = []
    data_summaries = []  # This will now contain CSV/H5 summaries, not folder summaries
    data_folder_summaries = []  # Keep old folder summaries separate
    total_bytes = 0
    code_chars = 0
    data_chars = 0
    total_chars = 0
    included_paths: Set[str] = set()

    # PRIORITY: Always include focus files (files actively being edited/viewed)
    # This prevents them from being skipped by "max_files" limits if they appear late in directory traversal.
    if focus_files:
        print(f"  🔍 Prioritizing {len(focus_files)} focus file(s)...")
        for relpath in focus_files:
            if not relpath or relpath in included_paths:
                continue
            
            abs_path = os.path.join(repo_root, relpath)
            if not os.path.exists(abs_path):
                continue
                
            # Process focus file (force inclusion even if limits strictly met, but count towards them)
            # We treat them as if they are normal files, but proccessed first.
            try:
                with open(abs_path, 'r', encoding='utf-8', errors='replace') as f:
                    content = f.read()
            except Exception:
                continue # Skip if unreadable
            
            # Apply semantic chunking if requested (and relevant for file type)
            if use_semantic_chunking and relpath.endswith('.py'):
                chunked_data = _chunk_file_semantically(
                    relpath, content, goal, focus_files, max_chars_per_file, min_relevance_threshold
                )
                file_entry = {
                    "path": relpath,
                    "contents": chunked_data["contents"],
                    "chunked": chunked_data.get("chunked", False),
                    "metadata": chunked_data.get("metadata", {})
                }
                char_count = len(file_entry["contents"])
            else:
                # Standard inclusion
                if len(content) > max_chars_per_file:
                     content = content[:max_chars_per_file] + "\n... (truncated)"
                
                file_entry = {
                    "path": relpath,
                    "contents": content
                }
                char_count = len(content)
            
            files.append(file_entry)
            included_paths.add(relpath)
            
            # Update counters
            total_chars += char_count
            total_bytes += len(content.encode('utf-8'))
            if is_data_folder(abs_path, repo_root, allowed_data_folders):
                data_chars += char_count
            else:
                code_chars += char_count
    
    # First, explicitly summarize data folders (old style - for backward compatibility)
    # Also check for image folders and summarize them specially
    for allowed in allowed_data_folders:
        if os.path.isabs(allowed):
            abs_allowed = os.path.abspath(allowed)
        else:
            abs_allowed = os.path.abspath(os.path.join(repo_root, allowed))
        
        if os.path.isdir(abs_allowed):
            folder_path = os.path.relpath(abs_allowed, repo_root)
            
            # Check if this folder contains mostly images
            image_extensions = {'.jpg', '.jpeg', '.png', '.gif', '.bmp', '.tiff', '.tif', '.webp', '.svg'}
            image_count = 0
            total_files = 0
            # IMPORTANT: do not shadow the outer `files` list used for context_pack["files"].
            for root, dirs, filenames_in_dir in os.walk(abs_allowed):
                for filename in filenames_in_dir:
                    ext = os.path.splitext(filename)[1].lower()
                    total_files += 1
                    if ext in image_extensions:
                        image_count += 1
                if total_files > 50:  # Sample first 50 files
                    break
            
            # If >50% images, use image folder summarization
            if total_files > 0 and image_count / total_files > 0.5 and data_chars < max_data_chars:
                try:
                    image_summary = summarize_image_folder(folder_path, repo_root, budget_chars=min(4000, max_data_chars - data_chars))
                    summary_text = image_summary.get("summary", "")
                    summary_chars = len(summary_text)
                    
                    if data_chars + summary_chars <= max_data_chars:
                        data_summaries.append(image_summary)
                        data_chars += summary_chars
                        total_chars += summary_chars
                        continue  # Skip regular folder summary for image folders
                except Exception:
                    pass  # Fall through to regular folder summary
            
            # Regular folder summary (for non-image folders or if image summarization failed)
            if folder_path not in [ds.get("folder_path") for ds in data_folder_summaries]:
                data_folder_summaries.append(_summarize_data_folder(folder_path, repo_root))
    
    # Collect files to include
    for root, dirs, filenames in os.walk(repo_root):
        # Build set of excluded directories (default + from deny_paths)
        excluded_dirs = DEFAULT_EXCLUDED_DIRS.copy()
        
        # Extract directory names from deny_paths
        if deny_paths:
            for deny in deny_paths:
                # Remove trailing slashes and extract dir name
                dir_name = deny.rstrip("/").rstrip("\\").split("/")[-1].split("\\")[-1]
                if dir_name:
                    excluded_dirs.add(dir_name)
        
        # Skip hidden directories and excluded directories
        dirs[:] = [d for d in dirs if d not in excluded_dirs and not d.startswith(".")]
        
        for filename in filenames:
            relpath = os.path.relpath(os.path.join(root, filename), repo_root)
            
            if not _should_include_file(relpath, repo_root, focus_files, deny_paths, deny_writing_data_folders, allowed_data_folders):
                continue
            
            abs_path = os.path.join(repo_root, relpath)
            if not os.path.isfile(abs_path):
                continue
            
            # Check if it's a data folder (summarize CSV/H5 files instead of including raw)
            if is_data_folder(abs_path, repo_root, allowed_data_folders):
                # Check if it's a CSV or H5 file that should be summarized
                file_ext = os.path.splitext(relpath)[1].lower()
                
                if file_ext == '.csv' and data_chars < max_data_chars:
                    # Summarize CSV
                    try:
                        csv_summary = summarize_csv(abs_path, repo_root, budget_chars=min(8000, max_data_chars - data_chars))
                        summary_text = csv_summary.get("summary", "")
                        summary_chars = len(summary_text)
                        
                        if data_chars + summary_chars <= max_data_chars:
                            data_summaries.append(csv_summary)
                            data_chars += summary_chars
                            total_chars += summary_chars
                    except Exception:
                        pass  # Skip if summarization fails
                    continue
                
                elif file_ext in ['.h5', '.hdf5'] and data_chars < max_data_chars:
                    # Summarize H5
                    try:
                        h5_summary = summarize_h5(abs_path, repo_root, budget_chars=min(3000, max_data_chars - data_chars))
                        summary_text = h5_summary.get("summary", "")
                        summary_chars = len(summary_text)
                        
                        if data_chars + summary_chars <= max_data_chars:
                            data_summaries.append(h5_summary)
                            data_chars += summary_chars
                            total_chars += summary_chars
                    except Exception:
                        pass  # Skip if summarization fails
                    continue
                
                elif file_ext == '.json' and data_chars < max_data_chars:
                    # Summarize JSON (single JSON object/array)
                    try:
                        json_summary = summarize_json(abs_path, repo_root, budget_chars=min(5000, max_data_chars - data_chars))
                        summary_text = json_summary.get("summary", "")
                        summary_chars = len(summary_text)
                        
                        if data_chars + summary_chars <= max_data_chars:
                            data_summaries.append(json_summary)
                            data_chars += summary_chars
                            total_chars += summary_chars
                    except Exception:
                        pass  # Skip if summarization fails
                    continue
                
                elif file_ext == '.jsonl' and data_chars < max_data_chars:
                    # Summarize JSONL (line-delimited JSON - each line is a JSON object)
                    try:
                        jsonl_summary = summarize_jsonl(abs_path, repo_root, budget_chars=min(5000, max_data_chars - data_chars))
                        summary_text = jsonl_summary.get("summary", "")
                        summary_chars = len(summary_text)
                        
                        if data_chars + summary_chars <= max_data_chars:
                            data_summaries.append(jsonl_summary)
                            data_chars += summary_chars
                            total_chars += summary_chars
                    except Exception:
                        pass  # Skip if summarization fails
                    continue
                
                elif file_ext in ['.yaml', '.yml'] and data_chars < max_data_chars:
                    # Summarize YAML
                    try:
                        yaml_summary = summarize_yaml(abs_path, repo_root, budget_chars=min(5000, max_data_chars - data_chars))
                        summary_text = yaml_summary.get("summary", "")
                        summary_chars = len(summary_text)
                        
                        if data_chars + summary_chars <= max_data_chars:
                            data_summaries.append(yaml_summary)
                            data_chars += summary_chars
                            total_chars += summary_chars
                    except Exception:
                        pass  # Skip if summarization fails
                    continue
                
                # For other files in data folders, use old folder summary approach
                folder_path = None
                for allowed in allowed_data_folders:
                    if os.path.isabs(allowed):
                        abs_allowed = os.path.abspath(allowed)
                    else:
                        abs_allowed = os.path.abspath(os.path.join(repo_root, allowed))
                    try:
                        rel = os.path.relpath(abs_path, abs_allowed)
                        if not rel.startswith(".."):
                            folder_path = os.path.relpath(abs_allowed, repo_root)
                            break
                    except ValueError:
                        pass
                
                # Also check always-on data folders
                if not folder_path:
                    path_parts = relpath.split(os.sep)
                    for i, part in enumerate(path_parts):
                        if part in ALWAYS_DATA_FOLDERS:
                            folder_path = os.sep.join(path_parts[:i+1])
                            break
                
                if folder_path and folder_path not in [ds["folder_path"] for ds in data_folder_summaries]:
                    data_folder_summaries.append(_summarize_data_folder(folder_path, repo_root))
                continue
            
            try:
                size = os.path.getsize(abs_path)
                
                with open(abs_path, "r", encoding="utf-8", errors="replace") as f:
                    contents = f.read()
                
                # Determine if we should use semantic chunking
                is_python_file = relpath.endswith('.py')
                is_large_file = len(contents) > max_chars_per_file
                is_focus_file = relpath in focus_files
                
                # For now, always include whole chunks for .py files (no semantic chunking)
                # This keeps things simple at the beginning
                should_chunk = False
                
                if should_chunk:
                    # Use semantic chunking
                    chunked_result = _chunk_file_semantically(
                        relpath,
                        contents,
                        goal=goal,
                        focus_files=focus_files,
                        max_chars_per_file=max_chars_per_file,
                        min_relevance_threshold=min_relevance_threshold
                    )
                    chunked_contents = chunked_result["contents"]
                    chunked_size = len(chunked_contents.encode('utf-8'))
                    
                    # Check if chunked version fits within limits
                    if total_bytes + chunked_size > max_total_bytes:
                        continue
                    if total_chars + len(chunked_contents) > max_total_chars:
                        continue
                    if len(files) >= max_files:
                        continue
                    
                    file_entry = {
                        "path": relpath,
                        "hash": _compute_sha256(abs_path),
                        "sha256": _compute_sha256(abs_path),
                        "size_bytes": chunked_size,
                        "contents": chunked_contents,
                        "chunked": True,
                        "chunk_metadata": chunked_result.get("metadata", {})
                    }
                    
                    # Include chunks info if available
                    if "chunks" in chunked_result:
                        file_entry["chunks"] = chunked_result["chunks"]
                    
                    # Check code budget
                    chunked_chars = len(chunked_contents)
                    if code_chars + chunked_chars > max_code_chars:
                        continue
                    if total_chars + chunked_chars > max_total_chars:
                        continue
                    
                    files.append(file_entry)
                    included_paths.add(relpath)
                    total_bytes += chunked_size
                    code_chars += chunked_chars
                    total_chars += chunked_chars
                else:
                    # Use full file content (original behavior)
                    content_chars = len(contents)
                    if code_chars + content_chars > max_code_chars:
                        continue
                    if total_bytes + size > max_total_bytes:
                        continue
                    if total_chars + content_chars > max_total_chars:
                        continue
                    if len(files) >= max_files:
                        continue
                    
                    files.append({
                        "path": relpath,
                        "hash": _compute_sha256(abs_path),
                        "sha256": _compute_sha256(abs_path),
                        "size_bytes": size,
                        "contents": contents,
                        "chunked": False
                    })
                    included_paths.add(relpath)
                    total_bytes += size
                    code_chars += content_chars
                    total_chars += content_chars
            except (OSError, UnicodeDecodeError, SyntaxError):
                # If semantic chunking fails, fall back to full content
                try:
                    size = os.path.getsize(abs_path)
                    if total_bytes + size > max_total_bytes:
                        continue
                    
                    with open(abs_path, "r", encoding="utf-8", errors="replace") as f:
                        contents = f.read()
                    
                    if total_chars + len(contents) > max_total_chars:
                        continue
                    if len(files) >= max_files:
                        continue
                    
                    # Check code budget
                    content_chars = len(contents)
                    if code_chars + content_chars > max_code_chars:
                        continue
                    if total_chars + content_chars > max_total_chars:
                        continue
                    
                    files.append({
                        "path": relpath,
                        "hash": _compute_sha256(abs_path),
                        "sha256": _compute_sha256(abs_path),
                        "size_bytes": size,
                        "contents": contents,
                        "chunked": False
                    })
                    included_paths.add(relpath)
                    total_bytes += size
                    code_chars += content_chars
                    total_chars += content_chars
                except (OSError, UnicodeDecodeError):
                    pass
    
    # Build diff vs previous context pack
    diff = {
        "added_files": [],
        "removed_files": [],
        "modified_files": []
    }
    
    if previous_context_pack:
        prev_paths = {f["path"]: f["sha256"] for f in previous_context_pack.get("files", []) if isinstance(f, dict) and "path" in f and "sha256" in f}
        curr_paths = {f["path"]: f["sha256"] for f in files if isinstance(f, dict) and "path" in f and "sha256" in f}
        
        for path, sha256 in curr_paths.items():
            if path not in prev_paths:
                diff["added_files"].append(path)
            elif prev_paths[path] != sha256:
                diff["modified_files"].append(path)
        
        for path in prev_paths:
            if path not in curr_paths:
                diff["removed_files"].append(path)
    
    # Extract error summaries from previous turn's command outcomes
    error_summaries = []
    if previous_turn_outcomes:
        error_summaries = _extract_error_summaries_from_outcomes(previous_turn_outcomes)
    
    # Combine data summaries (CSV/H5) with folder summaries (backward compatibility)
    all_data_summaries = data_summaries + data_folder_summaries
    
    build_time_ms = int((time.time() - build_start_time) * 1000)
    
    # Build system diagram summary if provided
    system_diagram_summary = None
    if system_diagram:
        components = system_diagram.get("components", [])
        interfaces = system_diagram.get("interfaces", [])
        flows = system_diagram.get("flows", [])
        mermaid = system_diagram.get("mermaid", "")
        
        system_diagram_summary = {
            "summary": f"System has {len(components)} components, {len(interfaces)} interfaces, {len(flows)} flows",
            "component_count": len(components),
            "interface_count": len(interfaces),
            "flow_count": len(flows),
            "mermaid": mermaid,  # Include full mermaid (LLM will compress if needed)
            "components": [{"id": c.get("id"), "name": c.get("name"), "type": c.get("type")} for c in components[:20]]  # Top 20
        }
    
    context_pack = {
        "turn_index": turn_index,
        "files": files,
        "data_summaries": all_data_summaries,  # CSV/H5 summaries + folder summaries
        "diff": diff,
        "error_summaries": error_summaries,  # Errors from previous turn's command execution
        "system_diagram": system_diagram_summary,  # System architecture diagram
        "stats": {
            "total_files": len(files),
            "total_bytes": total_bytes,
            "total_chars": total_chars,
            "code_chars": code_chars,
            "data_chars": data_chars,
            "data_folders": len(data_folder_summaries),
            "num_data_summaries": len(data_summaries),  # CSV/H5 summaries only
            "error_count": len(error_summaries),
            "pack_build_time_ms": build_time_ms
        }
    }
    
    return context_pack

def validate_context_pack(context_pack: Dict[str, Any], schema: Dict[str, Any]) -> None:
    """Validate context pack against schema."""
    validate(context_pack, schema)

