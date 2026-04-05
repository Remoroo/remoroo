"""Shared Textual CSS for unified Remoroo TUI (GitHub-dark aligned with tui_run.py)."""

# Named tokens (documentation / future code use)
REMOROO_COLORS = {
    "background": "#0d1117",
    "surface_deep": "#010409",
    "surface_raised": "#161b22",
    "surface_muted": "#21262d",
    "border": "#30363d",
    "accent": "#58a6ff",
    "accent_strong": "#1f6feb",
    "accent_bright": "#79c0ff",
    "accent_sidebar": "#388bfd",
    "text": "#e6edf3",
    "text_muted": "#8b949e",
    "text_soft": "#c9d1d9",
}

REMOROO_UNIFIED_SCREENS_CSS = """
Screen {
    background: #0d1117;
    color: #e6edf3;
    layout: vertical;
    overflow-y: hidden;
}
Header {
    dock: none;
    height: 1;
    background: #161b22;
}
Footer {
    dock: none;
    background: #161b22;
}
#brand-strip {
    height: 1;
    background: #010409;
    color: #79c0ff;
    text-style: bold;
    border-bottom: solid #1f6feb;
    padding: 0 1;
}
#context-strip {
    height: auto;
    min-height: 1;
    max-height: 3;
    background: #161b22;
    color: #58a6ff;
    text-style: bold;
    border-bottom: solid #30363d;
    padding: 0 1;
}
#context-hint {
    height: auto;
    background: #161b22;
    color: #8b949e;
    padding: 0 1 1 1;
}
#closing-banner {
    height: auto;
    background: #21262d;
    color: #79c0ff;
    text-style: bold;
    padding: 0 1;
    border-top: solid #1f6feb;
}
#wizard-body {
    height: 1fr;
    padding: 0 1;
}
#wizard-meta {
    height: auto;
    background: #21262d;
    color: #8b949e;
    padding: 0 1;
}
TextArea:focus {
    border: tall #1f6feb;
}
Input:focus {
    border: tall #1f6feb;
}
TextArea {
    min-height: 3;
    background: #010409;
    border: tall #30363d;
}
Input {
    background: #010409;
    border: tall #30363d;
}
#action-row {
    height: auto;
    align: center middle;
    padding: 1;
}
Button {
    margin-right: 1;
}
Button.-primary {
    background: #1f6feb;
    color: #ffffff;
    text-style: bold;
}
DataTable {
    height: 1fr;
    background: #010409;
}
DataTable > .datatable--header {
    background: #21262d;
    color: #79c0ff;
    text-style: bold;
}
#results-summary {
    border: tall #30363d;
    padding: 1;
    margin: 1;
    background: #010409;
}
#patch-dialog {
    align: center middle;
    background: #0d1117 60%;
}
#patch-panel {
    width: 70;
    height: auto;
    max-height: 16;
    background: #161b22;
    border: tall #388bfd;
    padding: 1 2;
}
LoadingIndicator {
    color: #58a6ff;
}
OptionList {
    height: 1fr;
    background: #0d1117;
    border: tall #30363d;
    scrollbar-color: #388bfd;
    scrollbar-background: #21262d;
}
OptionList:focus > .option-list--option-highlighted {
    background: #1f6feb;
    color: #ffffff;
    text-style: bold;
}
"""
