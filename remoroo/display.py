"""Rich terminal UI for Remoroo runs.

Replaces the old Live-dashboard approach with a streaming event-card
pattern: each agent event is rendered as a styled Rich renderable that
scrolls by naturally.  Debug events are hidden unless ``verbose=True``.
"""
from __future__ import annotations

import textwrap
from typing import Any, Optional

from rich.console import Console
from rich.panel import Panel
from rich.rule import Rule
from rich.text import Text
from rich.syntax import Syntax
from rich.table import Table


class RunDisplay:
    """Renders ``AgentEvent`` objects to a Rich Console."""

    def __init__(self, console: Optional[Console] = None, verbose: bool = False):
        self.console = console or Console()
        self.verbose = verbose

    # ── public API ───────────────────────────────────────────────────

    def render_event(self, d: dict) -> None:
        """Render a brain agent event from a plain dict (no brain imports).

        The dict comes directly from the transport payload with keys like
        ``kind``, ``text``, ``turn_num``, etc.
        """
        kind = d.get("kind", "")
        handler = getattr(self, f"_render_{kind}", None)
        if handler:
            handler(d)

    def print(self, *args: Any, **kwargs: Any) -> None:
        """Pass-through for WorkerService output_callback compatibility."""
        self.console.print(*args, **kwargs)

    # ── event renderers (all take a plain dict, no brain imports) ────

    def _render_run_started(self, d: dict) -> None:
        tbl = Table(show_header=False, box=None, padding=(0, 1))
        tbl.add_column("key", style="dim")
        tbl.add_column("val")
        tbl.add_row("Goal", d.get("goal", ""))
        metric = d.get("metric", "")
        if d.get("goal_only"):
            metric += "  [dim](goal-only mode)[/dim]"
        if metric:
            tbl.add_row("Metric", metric)
        self.console.print()
        self.console.print(Panel(
            tbl,
            title=f"[bold]Remoroo Run [cyan]{d.get('run_id', '')}[/cyan][/bold]",
            border_style="cyan",
            padding=(0, 1),
        ))
        self.console.print()

    def _render_run_resumed(self, d: dict) -> None:
        self.console.print(
            Text.assemble(
                (" Resumed ", "bold black on yellow"),
                (f"  {d.get('history_len', 0)} messages, {d.get('actions_used', 0)} actions", ""),
            )
        )

    def _render_run_completed(self, d: dict) -> None:
        verdict = d.get("verdict", "unknown")
        if verdict == "success":
            style = "bold green"
            icon = "+"
        elif verdict == "partial":
            style = "bold yellow"
            icon = "~"
        else:
            style = "bold red"
            icon = "x"

        cost = d.get("total_cost_usd", 0.0)
        self.console.print()
        self.console.print(Panel(
            Text.assemble(
                (f" [{icon}] ", style),
                (f"Run finished: {verdict}", style),
                (f"  |  {d.get('total_actions', 0)} actions  |  ${cost:.2f}", "dim"),
            ),
            border_style="cyan",
        ))
        evidence = d.get("evidence", "")
        if evidence:
            self.console.print(f"  [dim]{evidence[:200]}[/dim]")
        self.console.print()

    def _render_turn_started(self, d: dict) -> None:
        tool_names = d.get("tool_names", [])
        if tool_names:
            counts: dict[str, int] = {}
            seen: list[str] = []
            for n in tool_names:
                counts[n] = counts.get(n, 0) + 1
                if n not in seen:
                    seen.append(n)
            parts = [f"{n} x{counts[n]}" if counts[n] > 1 else n for n in seen]
            tool_list = ", ".join(parts)
        else:
            tool_list = ""
        self.console.print(Rule(
            f"[bold]Turn {d.get('turn_num', '?')}[/bold]  {d.get('tool_count', 0)} tool(s): {tool_list}",
            style="dim",
        ))

    def _render_assistant_message(self, d: dict) -> None:
        text = (d.get("text") or "").strip()
        if not text:
            return
        self.console.print(Panel(
            text,
            title="[bold blue]Autonomous Engineering[/bold blue]",
            border_style="blue",
            padding=(0, 1),
        ))

    def _render_tool_call_started(self, d: dict) -> None:
        name = d.get("tool_name", "")
        summary = d.get("args_summary", "")
        if _is_plumbing(name, summary):
            return
        icon = _TOOL_ICONS.get(name, ">>")

        if name == "bash":
            self.console.print(Panel(
                Syntax(summary, "bash", theme="monokai", word_wrap=True) if summary else Text("(empty)"),
                title=f"[bold yellow]{icon} bash[/bold yellow]",
                border_style="yellow",
                padding=(0, 1),
            ))
        elif name in ("read_file", "get_snippet"):
            self.console.print(Text.assemble(
                (f"  {icon} ", "dim"), (name, "cyan"), ("  ", ""), (summary, ""),
            ))
        elif name in ("edit_file", "create_file"):
            self.console.print(Text.assemble(
                (f"  {icon} ", "dim"), (name, "magenta"), ("  ", ""), (summary, ""),
            ))
        elif name == "think":
            self.console.print(Panel(
                textwrap.shorten(summary, 200, placeholder="..."),
                title=f"[dim]{icon} think[/dim]",
                border_style="dim",
                padding=(0, 1),
            ))
        else:
            line = Text.assemble((f"  {icon} ", "dim"), (name, "green"))
            if summary:
                line.append(f"  {summary}", style="dim")
            self.console.print(line)

    def _render_tool_call_completed(self, d: dict) -> None:
        if _is_plumbing(d.get("tool_name", ""), d.get("output_preview", "")):
            return
        if not d.get("success", True):
            self.console.print(Text.assemble(
                ("  x ", "bold red"),
                (d.get("tool_name", ""), "red"),
                (" failed: ", "red"),
                (d.get("output_preview", "")[:160], "dim red"),
            ))

    def _render_file_edited(self, d: dict) -> None:
        path = d.get("path", "")
        if ".remoroo/" in path:
            return
        self.console.print(Text.assemble(
            ("  ~ ", "bold magenta"),
            (path, "magenta"),
            ("  ", ""),
            (d.get("summary", ""), "dim"),
        ))

    def _render_file_created(self, d: dict) -> None:
        path = d.get("path", "")
        if ".remoroo/" in path:
            return
        self.console.print(Text.assemble(
            ("  + ", "bold green"),
            (path, "green"),
            (f"  ({d.get('size_bytes', 0)} bytes)", "dim"),
        ))

    def _render_bash_job_started(self, d: dict) -> None:
        cmd = d.get("command", "")
        self.console.print(Text.assemble(
            ("  > ", "bold yellow"),
            ("started ", "dim"),
            (cmd[:100], "yellow"),
            (f"  (job={d.get('job_id', '')})", "dim"),
        ))

    def _render_bash_job_completed(self, d: dict) -> None:
        exit_code = d.get("exit_code", -1)
        style = "green" if exit_code == 0 else "red"
        icon = "+" if exit_code == 0 else "x"
        elapsed = d.get("elapsed_s", 0.0)
        self.console.print(Text.assemble(
            (f"  {icon} ", f"bold {style}"),
            (f"job={d.get('job_id', '')}", style),
            (f" exited {exit_code}", f"bold {style}"),
            (f"  ({elapsed:.1f}s)", "dim"),
        ))
        tail = d.get("stdout_tail", "")
        if tail:
            for line in tail.strip().splitlines()[-5:]:
                self.console.print(f"    [dim]{line}[/dim]")

    def _render_watch_mode_entered(self, d: dict) -> None:
        self.console.print(
            f"  [dim]Watching {d.get('job_count', 0)} job(s) (max {d.get('max_wait_s', 0):.0f}s)...[/dim]"
        )

    def _render_watch_mode_wake(self, d: dict) -> None:
        self.console.print(Text.assemble(
            ("  ~ ", "bold cyan"),
            ("wake: ", "cyan"),
            (d.get("reason", "")[:200], ""),
        ))

    def _render_status(self, d: dict) -> None:
        level = d.get("level", "info")
        msg = d.get("message", "")
        if level == "debug":
            if self.verbose:
                self.console.print(f"  [dim]{msg}[/dim]")
            return
        if level == "error":
            self.console.print(f"  [bold red]! {msg}[/bold red]")
        elif level == "warning":
            self.console.print(f"  [yellow]~ {msg}[/yellow]")
        else:
            self.console.print(f"  [dim]{msg}[/dim]")


# ── helpers ──────────────────────────────────────────────────────────

_PLUMBING_TOOLS = frozenset({
    "read_workspace_memory", "write_workspace_memory",
    "store_workspace_memory", "read_local_memory", "write_local_memory",
})


def _is_plumbing(tool_name: str, args_or_path: str = "") -> bool:
    """Return True for internal bookkeeping calls that should not be shown."""
    if tool_name in _PLUMBING_TOOLS:
        return True
    if tool_name in ("read_file", "write_file", "create_file") and ".remoroo/" in args_or_path:
        return True
    return False


# ── constants ────────────────────────────────────────────────────────

_TOOL_ICONS = {
    "bash": "$",
    "read_file": "R",
    "edit_file": "E",
    "create_file": "+",
    "get_snippet": "S",
    "grep": "G",
    "glob": "G",
    "list_dir": "L",
    "list_repo": "L",
    "scan_repo": "?",
    "think": "T",
    "metric_gate": "M",
    "iterate": "↻",
    "done": "D",
    "plan": "P",
    "note": "N",
    "store": "W",
    "recall": "R",
    "delegate": ">",
    "web_search": "W",
    "record_lesson": "L",
}

