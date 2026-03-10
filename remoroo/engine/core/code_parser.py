"""
Tree-sitter based code parser for semantic chunking.
Industry standard approach used by GitHub Copilot, Sourcegraph, etc.
"""
from pathlib import Path
from typing import List, Dict, Any, Optional
import traceback

# Tree-sitter imports with graceful fallback
try:
    from tree_sitter_language_pack import get_parser
    TREE_SITTER_AVAILABLE = True
except ImportError:
    TREE_SITTER_AVAILABLE = False
    print("⚠️  tree-sitter not available, falling back to AST parsing")

# Language mapping for tree-sitter
LANGUAGE_MAP = {
    '.py': 'python',
    '.js': 'javascript',
    '.jsx': 'javascript',
    '.ts': 'typescript',
    '.tsx': 'typescript',
    '.go': 'go',
    '.rs': 'rust',
    '.java': 'java',
    '.cpp': 'cpp',
    '.cc': 'cpp',
    '.cxx': 'cpp',
    '.c': 'c',
    '.h': 'c',
    '.cs': 'c_sharp',
    '.rb': 'ruby',
    '.php': 'php',
    '.swift': 'swift',
    '.kt': 'kotlin',
    '.scala': 'scala',
}

def extract_code_blocks_treesitter(
    file_path: str,
    content: str
) -> List[Dict[str, Any]]:
    """
    Extract semantic code blocks (functions, classes, imports) using tree-sitter.
    
    This is the industry-standard approach for code parsing, providing:
    - Robust error recovery (handles syntax errors gracefully)
    - Multi-language support (Python, JS, Go, Rust, etc.)
    - Precise byte-accurate boundaries
    - Complete function extraction (no mid-function cuts)
    
    Returns:
        List of chunks with metadata:
        - type: 'import', 'function', 'class', 'main_block', 'raw'
        - name: identifier name (or None)
        - content: complete code block text
        - line_range: [start_line, end_line] (1-indexed)
        - relevance_score: float (0.0-1.0, will be scored later)
    """
    if not TREE_SITTER_AVAILABLE:
        return _fallback_to_raw(content)
    
    ext = Path(file_path).suffix
    language = LANGUAGE_MAP.get(ext)
    
    if not language:
        # Non-supported language, return full content
        return _fallback_to_raw(content)
    
    try:
        # Get language-specific parser (tree-sitter-languages convenience API)
        parser = get_parser(language)
        tree = parser.parse(bytes(content, 'utf8'))
    except Exception as e:
        # Parser error, fallback to full content
        print(f"⚠️  Tree-sitter parsing failed for {file_path}: {e}")
        return _fallback_to_raw(content)
    
    if not tree or not tree.root_node:
        return _fallback_to_raw(content)
    
    blocks = []
    
    # Extract imports (always include)
    blocks.extend(_extract_imports(tree.root_node, content))
    
    # Extract main block (if __name__ == '__main__') for Python
    if language == 'python':
        main_block = _extract_main_block(tree.root_node, content)
        if main_block:
            blocks.append(main_block)
    
    # Extract top-level functions
    blocks.extend(_extract_functions(tree.root_node, content, language))
    
    # Extract classes (includes methods)
    blocks.extend(_extract_classes(tree.root_node, content, language))
    
    # If no blocks found, return full content
    if not blocks:
        return _fallback_to_raw(content)
    
    return blocks


def _fallback_to_raw(content: str) -> List[Dict[str, Any]]:
    """Fallback: return full content as single chunk."""
    lines = content.split('\n')
    return [{
        'type': 'raw',
        'name': None,
        'content': content,
        'line_range': [1, len(lines)],
        'relevance_score': 0.5
    }]


def _extract_imports(node, content: str) -> List[Dict[str, Any]]:
    """Extract import statements."""
    imports = []
    
    for child in node.children:
        if child.type in [
            'import_statement',        # Python: import foo
            'import_from_statement',   # Python: from foo import bar
            'import_declaration',      # Go: import "foo"
            'use_declaration',         # Rust: use foo::bar
            'include_directive',       # C/C++: #include <foo>
        ]:
            imports.append({
                'type': 'import',
                'name': None,
                'content': content[child.start_byte:child.end_byte],
                'line_range': [child.start_point[0] + 1, child.end_point[0] + 1],
                'relevance_score': 1.0  # Always include imports
            })
    
    return imports


def _extract_main_block(node, content: str) -> Optional[Dict[str, Any]]:
    """Extract Python's if __name__ == '__main__' block."""
    for child in node.children:
        if child.type == 'if_statement':
            # Check if this is the main block
            condition = child.child_by_field_name('condition')
            if condition and 'name' in content[condition.start_byte:condition.end_byte]:
                main_text = content[condition.start_byte:condition.end_byte]
                if '__name__' in main_text and '__main__' in main_text:
                    return {
                        'type': 'main_block',
                        'name': '__main__',
                        'content': content[child.start_byte:child.end_byte],
                        'line_range': [child.start_point[0] + 1, child.end_point[0] + 1],
                        'relevance_score': 1.0  # Always include main block
                    }
    
    return None


def _extract_functions(node, content: str, language: str) -> List[Dict[str, Any]]:
    """Extract top-level functions (not nested in classes)."""
    functions = []
    
    function_types = {
        'python': 'function_definition',
        'javascript': 'function_declaration',
        'typescript': 'function_declaration',
        'go': 'function_declaration',
        'rust': 'function_item',
        'java': 'method_declaration',
        'cpp': 'function_definition',
        'c': 'function_definition',
    }
    
    func_type = function_types.get(language)
    if not func_type:
        return functions
    
    for child in node.children:
        if child.type == func_type:
            func_name = _get_function_name(child, content, language)
            functions.append({
                'type': 'function',
                'name': func_name,
                'content': content[child.start_byte:child.end_byte],
                'line_range': [child.start_point[0] + 1, child.end_point[0] + 1],
                'relevance_score': 0.5  # Will be scored later based on goal
            })
    
    return functions


def _extract_classes(node, content: str, language: str) -> List[Dict[str, Any]]:
    """Extract classes (includes all methods)."""
    classes = []
    
    class_types = {
        'python': 'class_definition',
        'javascript': 'class_declaration',
        'typescript': 'class_declaration',
        'go': 'type_declaration',  # Go uses structs
        'rust': 'impl_item',       # Rust uses impl blocks
        'java': 'class_declaration',
        'cpp': 'class_specifier',
        'c_sharp': 'class_declaration',
    }
    
    class_type = class_types.get(language)
    if not class_type:
        return classes
    
    for child in node.children:
        if child.type == class_type:
            class_name = _get_class_name(child, content, language)
            classes.append({
                'type': 'class',
                'name': class_name,
                'content': content[child.start_byte:child.end_byte],
                'line_range': [child.start_point[0] + 1, child.end_point[0] + 1],
                'relevance_score': 0.5  # Will be scored later based on goal
            })
    
    return classes


def _get_function_name(node, content: str, language: str) -> str:
    """Extract function name from node."""
    # Try to find the name field
    name_node = node.child_by_field_name('name')
    if name_node:
        return content[name_node.start_byte:name_node.end_byte]
    
    # Fallback: look for identifier in children
    for child in node.children:
        if child.type == 'identifier':
            return content[child.start_byte:child.end_byte]
    
    return 'unknown'


def _get_class_name(node, content: str, language: str) -> str:
    """Extract class name from node."""
    # Try to find the name field
    name_node = node.child_by_field_name('name')
    if name_node:
        return content[name_node.start_byte:name_node.end_byte]
    
    # Fallback: look for identifier in children
    for child in node.children:
        if child.type in ['identifier', 'type_identifier']:
            return content[child.start_byte:child.end_byte]
    
    return 'unknown'


def supports_tree_sitter(file_path: str) -> bool:
    """Check if tree-sitter is available and supports this file type."""
    if not TREE_SITTER_AVAILABLE:
        return False
    
    ext = Path(file_path).suffix
    return ext in LANGUAGE_MAP
