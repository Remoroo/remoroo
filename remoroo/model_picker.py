"""Pre-run Textual UI to pick v2 LLM model tier (Haiku / Sonnet / Opus)."""
from __future__ import annotations

from typing import List, Optional, Tuple

from textual import on
from textual.app import App, ComposeResult
from textual.binding import Binding
from textual.containers import Vertical
from textual.widgets import Footer, Header, OptionList, Static
from textual.widgets.option_list import Option

from .branding import BRAND_MARKUP_MODEL_PICKER

# Labels + model id sent as POST /runs `model` (empty = server default from V2_AGENT_DEFAULTS)
CHOICES: List[Tuple[str, str]] = [
    ("Default (Haiku 4.5)", ""),
    ("Claude Haiku 4.5 — fast / cheap", "anthropic/claude-haiku-4.5"),
    ("Claude Sonnet 4.5 — balanced", "anthropic/claude-sonnet-4.5"),
    ("Claude Opus 4.7 — flagship", "anthropic/claude-opus-4.7"),
    ("Claude Opus 4.6 — previous flagship", "anthropic/claude-opus-4.6"),
]


class ModelPickApp(App[Optional[str]]):
    """Returns model id or None for default."""

    TITLE = "Remoroo"
    SUB_TITLE = "choose model"

    CSS = """
    Screen { background: #0d1117; color: #e6edf3; }
    #brand-strip {
        dock: top;
        height: 1;
        background: #010409;
        color: #79c0ff;
        text-style: bold;
        border-bottom: solid #1f6feb;
        padding: 0 1;
    }
    #hint { height: 2; padding: 0 1; color: #8b949e; }
    OptionList { height: 1fr; border: tall #30363d; }
    Footer { background: #161b22; }
    """

    BINDINGS = [
        Binding("escape", "default", "Default", show=True),
    ]

    def compose(self) -> ComposeResult:
        yield Header()
        yield Static(BRAND_MARKUP_MODEL_PICKER, id="brand-strip", markup=True)
        yield Static(
            "Choose the agent model. [bold]Enter[/] selects; [bold]Esc[/] uses default (Haiku).\n"
            "Mid-run changes are not supported — pick before starting the run.",
            id="hint",
        )
        yield OptionList(
            *[Option(label, id=str(i)) for i, (label, _) in enumerate(CHOICES)],
            id="opts",
        )
        yield Footer()

    def on_mount(self) -> None:
        self.query_one("#brand-strip", Static).update(BRAND_MARKUP_MODEL_PICKER)
        self.query_one("#opts", OptionList).focus()

    @on(OptionList.OptionSelected, "#opts")
    def _on_selected(self, event: OptionList.OptionSelected) -> None:
        _, mid = CHOICES[event.option_index]
        self.exit(mid if mid else None)

    def action_default(self) -> None:
        self.exit(None)


def pick_model_interactive() -> Optional[str]:
    """Blocking UI; returns None to keep server default."""
    return ModelPickApp().run()
