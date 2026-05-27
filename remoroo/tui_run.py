"""Full-screen Textual TUI for `remoroo run --local` (Binsider-style layout).

Scrollable timeline (OptionList), assistant + tool RichLogs, sticky header/footer,
optional raw pane. Worker/SSE threads push UI work into a queue; the main thread
pumps it on a timer so ``call_from_thread`` never blocks the worker on each bash
line (avoids stdout pipe deadlock).

**Stopping vs resuming**

- **q (quit):** kills local worker children (``kill_all_background_jobs``) and stops the
  sandbox, then ``POST /runs/{id}/abort`` (run FAILED). ``stop_flag`` ends poll/SSE;
  that run id is finished unless you start a new run (checkpoint replay is separate).

- **Ctrl+d (detach):** tmux-style — ``POST /detach`` sets a graceful-detach flag so the
  run stays **RUNNING** (or **PAUSED**) without zombie-fail on missing heartbeat; reattach
  with ``remoroo run --local --resume <run_id>`` (heartbeat clears the flag).

- **p (pause):** cooperative pause via CP + Redis; press **p** again (or API) to
  resume the **same** run without leaving the TUI.
"""
from __future__ import annotations

import json
import logging
import os
import queue
import threading
import time
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

from textual import on, work
from textual.app import App, ComposeResult
from textual.screen import Screen
from textual.binding import Binding
from textual.containers import Horizontal, Vertical
from textual.widgets import Footer, Header, OptionList, RichLog, Static
from textual.widgets.option_list import Option

from .branding import BRAND_MARKUP_LINE
from .display import _TOOL_ICONS, _is_plumbing

_log = logging.getLogger(__name__)


@dataclass
class ToolModel:
    name: str
    summary: str
    status: str = "pending"
    call_id: str = ""  # matches brain ToolCallStarted/Completed (required when same tool repeats in one turn)
    log_lines: List[str] = field(default_factory=list)
    exit_code: Optional[int] = None


@dataclass
class TurnModel:
    turn_num: int
    option_id: str
    planned_label: str
    assistant: str = ""
    tools: List[ToolModel] = field(default_factory=list)
    worker_notes: List[str] = field(default_factory=list)


@dataclass
class BudgetTuiState:
    """Billing/budget snapshot from POST /runs for the sticky budget strip."""

    requested_wall_time_s: float
    effective_wall_time_s: float
    model_tier: str = "haiku"
    multiplier: int = 1
    clamped: bool = False
    overage: bool = False
    projected_overage_credits: Optional[int] = None
    projected_overage_usd: Optional[float] = None
    credits_available: Optional[int] = None
    credits_reserved: Optional[int] = None
    affordable_h: Optional[Dict[str, float]] = None


def _budget_timeline_badge(b: Optional[BudgetTuiState]) -> str:
    """Short always-visible status for the timeline pane (left column)."""
    if b is None:
        return "[dim]│[/]  [dim]budget n/a[/]"
    eff_h = b.effective_wall_time_s / 3600.0
    if b.overage:
        return f"[dim]│[/]  [bold #f0883e]OVERAGE {eff_h:.1f}h[/]"
    if b.clamped:
        return f"[dim]│[/]  [bold yellow]CLAMPED {eff_h:.1f}h[/]"
    return f"[dim]│[/]  [bold green]WITHIN BALANCE {eff_h:.1f}h[/]"


def _budget_footer_lines(b: Optional[BudgetTuiState]) -> str:
    """Two-line max budget strip above Textual Footer (no LIVE/help duplication)."""
    def _cr_inline(bb: BudgetTuiState) -> str:
        if bb.credits_available is None or bb.credits_reserved is None:
            return ""
        return (
            f"  [dim]·[/]  [bold green]{bb.credits_available}[/][dim]/[/][cyan]{bb.credits_reserved}[/] [dim]cr[/]"
        )

    if b is None:
        return "[bold yellow]▶ BUDGET[/]  [dim]n/a[/]  [dim](--resume)[/]"
    req_h = b.requested_wall_time_s / 3600.0
    eff_h = b.effective_wall_time_s / 3600.0
    tier = (b.model_tier or "haiku").capitalize()
    mul = b.multiplier
    cr = _cr_inline(b)
    if b.overage:
        oc = b.projected_overage_credits
        ou = float(b.projected_overage_usd or 0.0)
        oc_s = str(oc) if oc is not None else "?"
        line1 = (
            f"[bold #f0883e]▶ OVERAGE[/]  [bold #ffa657]+{oc_s} cr[/] ~[bold #ffa657]${ou:.2f}[/]  "
            f"[dim]·[/]  full ask [bold]{req_h:.1f}h[/]{cr}"
        )
    elif b.clamped:
        line1 = (
            f"[bold yellow]▶ CLAMPED[/]  [dim]{req_h:.1f}h[/][dim]→[/][bold #58a6ff]{eff_h:.1f}h[/]  "
            f"[dim]·[/]  {tier} [dim]{mul}×[/]{cr}"
        )
    else:
        line1 = (
            f"[bold green]▶ OK[/]  [bold #58a6ff]{eff_h:.1f}h[/] cap  [dim]·[/]  {tier} [dim]{mul}×[/]{cr}"
        )
    aff = b.affordable_h or {}
    line2 = ""
    if b.clamped and aff:
        h, s, o = aff.get("haiku"), aff.get("sonnet"), aff.get("opus")
        if h is not None and s is not None and o is not None:
            line2 = (
                f"[dim]Afford @ balance:[/]  [cyan]{h:g}[/]h H  [dim]/[/]  [cyan]{s:g}[/]h S  [dim]/[/]  "
                f"[cyan]{o:g}[/]h O"
            )
    if line2:
        return line1 + "\n" + line2
    return line1


@dataclass
class RunTuiModel:
    run_id: str
    repo_short: str
    engine: str
    goal: str = ""
    metric: str = ""
    sse_ok: bool = False
    turns: List[TurnModel] = field(default_factory=list)
    follow_live: bool = True
    raw_lines: List[str] = field(default_factory=list)
    show_raw: bool = False
    ui_paused: bool = False
    now_text: str = "Connecting…"
    last_tool_short: str = "—"
    current_sink: Optional[List[str]] = None
    orphan_worker: List[str] = field(default_factory=list)
    last_metrics_snapshot: Dict[str, Any] = field(default_factory=dict)
    episode_index: int = 0
    # Assistant text that arrived before turn_started (brain emits assistant_message first)
    pending_assistant_buffer: str = ""
    budget: Optional[BudgetTuiState] = None


class RemorooRunScreen(Screen[Dict[str, Any]]):
    """Live run timeline + logs; dismisses with ``result_box`` when the run ends."""

    CSS = """
    Screen {
        background: #0d1117;
        color: #e6edf3;
        layout: vertical;
        overflow-y: hidden;
    }
    /* Single vertical flow: separate body/chrome layers each fill the screen, so top
       strips were painting over #main-split; a taller goal/metrics bar hid assistant text. */
    Header {
        dock: none;
        height: 1;
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
    #title-strip {
        height: 1;
        background: #161b22;
        color: #58a6ff;
        text-style: bold;
        border-bottom: solid #30363d;
        padding: 0 1;
    }
    #goal-strip {
        height: auto;
        max-height: 4;
        background: #21262d;
        color: #8b949e;
        border-bottom: solid #30363d;
        padding: 0 1;
    }
    #main-split {
        height: 1fr;
        min-height: 12;
    }
    #timeline-pane {
        width: 36;
        min-width: 24;
        background: #0d1117;
        border-right: wide #388bfd;
    }
    #timeline-header {
        height: auto;
        min-height: 1;
        max-height: 3;
        background: #161b22;
        color: #58a6ff;
        text-style: bold;
        padding: 0 1;
        border-bottom: solid #1f6feb;
    }
    #timeline-pane OptionList {
        height: 1fr;
    }
    OptionList {
        height: 1fr;
        background: #0d1117;
        border: none;
        scrollbar-color: #388bfd;
        scrollbar-background: #21262d;
        scrollbar-size-vertical: 1;
    }
    OptionList > .option-list--option {
        padding: 0 1;
    }
    OptionList:focus > .option-list--option-highlighted {
        background: #1f6feb;
        color: #ffffff;
        text-style: bold;
    }
    #detail-pane {
        width: 1fr;
        background: #0d1117;
    }
    #assistant-label, #tool-label {
        height: 1;
        background: #21262d;
        color: #79c0ff;
        text-style: bold;
        padding: 0 1;
        border-top: solid #30363d;
    }
    #assistant-label {
        border-top: none;
        color: #a5d6ff;
    }
    #assistant-log {
        height: 1fr;
        min-height: 5;
        background: #010409;
        border: tall #30363d;
        padding: 0 1;
    }
    #tool-log {
        height: 1fr;
        min-height: 6;
        background: #010409;
        border: tall #f0883e;
        padding: 0 1;
    }
    #raw-pane {
        height: 8;
        background: #161b22;
        border-top: solid #6e7681;
        display: none;
    }
    #raw-pane.visible {
        display: block;
    }
    #raw-label {
        height: 1;
        background: #21262d;
        color: #ffa657;
        padding: 0 1;
    }
    #raw-log {
        height: 1fr;
        background: #0d1117;
        border: tall #6e7681;
    }
    #bottom-bar {
        layout: vertical;
        width: 100%;
        height: auto;
    }
    #footer-strip {
        height: auto;
        min-height: 1;
        max-height: 4;
        width: 100%;
        background: #21262d;
        color: #c9d1d9;
        padding: 0 1;
        border-top: solid #388bfd;
    }
    #bottom-bar Footer {
        width: 100%;
        dock: none;
    }
    """

    BINDINGS = [
        Binding("q", "quit", "Quit", priority=True),
        Binding("p", "pause_toggle", "Pause"),
        Binding("ctrl+d", "detach", "Detach"),
        Binding("g", "go_live", "Live"),
        Binding("ctrl+r", "toggle_raw", "Raw"),
        Binding("question_mark", "help", "Help"),
    ]

    def __init__(
        self,
        *,
        model: RunTuiModel,
        result_box: Dict[str, Any],
        stop_flag: threading.Event,
        api_url: str,
        session_key: str,
        remote_run_id: str,
        artifact_dir: str,
        original_repo_path: str,
        engine: str,
        cache_env: bool,
        in_place: bool,
        server: Any,
    ) -> None:
        super().__init__()
        self.model = model
        self.result_box = result_box
        self.stop_flag = stop_flag
        self.api_url = api_url
        self.session_key = session_key
        self.remote_run_id = remote_run_id
        self.artifact_dir = artifact_dir
        self.original_repo_path = original_repo_path
        self.engine = engine
        self.cache_env = cache_env
        self.in_place = in_place
        self._server = server
        self._exec_repo_root = original_repo_path
        self._artifact_for_worker: Optional[str] = str(artifact_dir) if artifact_dir else None
        self._worker: Any = None
        self._highlighted_turn_index: Optional[int] = None
        # Canonical JSON fingerprints for events already applied (hydrate + SSE dedupe)
        self._replay_seen: set[str] = set()
        # Unbounded cross-thread UI queue — never block worker on Textual main thread
        self._ui_q: queue.SimpleQueue[tuple[str, Any]] = queue.SimpleQueue()
        # Coalesce detail-pane rebuilds: _worker_line used to call _refresh_detail_logs per
        # stdout line; the pump can drain 250 items per tick → hundreds of full RichLog clears/s.
        self._pending_detail_refresh: bool = False

    def compose(self) -> ComposeResult:
        yield Header(show_clock=True)
        yield Static(BRAND_MARKUP_LINE, id="brand-strip", markup=True)
        yield Static("", id="title-strip")
        yield Static("", id="goal-strip")
        with Horizontal(id="main-split"):
            with Vertical(id="timeline-pane"):
                yield Static("", id="timeline-header", markup=True)
                yield OptionList(id="timeline")
            with Vertical(id="detail-pane"):
                yield Static(
                    "[bold #58a6ff]Remoroo[/] [dim]·[/] Autonomous Engineer",
                    id="assistant-label",
                    markup=True,
                )
                # highlight=False: Pygments on every write is very slow for large assistant/tool text
                yield RichLog(id="assistant-log", wrap=True, highlight=False, markup=True)
                yield Static(
                    "[bold #58a6ff]Remoroo[/] [dim]·[/] Tool output",
                    id="tool-label",
                    markup=True,
                )
                yield RichLog(id="tool-log", wrap=True, highlight=False, markup=False)
        with Vertical(id="raw-pane"):
            yield Static(
                "[bold #58a6ff]Remoroo[/] [dim]·[/] Raw worker / debug",
                id="raw-label",
                markup=True,
            )
            yield RichLog(id="raw-log", wrap=True, highlight=True, markup=False)
        with Vertical(id="bottom-bar"):
            yield Static("", id="footer-strip", markup=True)
            yield Footer()

    def on_mount(self) -> None:
        self._refresh_brand()
        self._refresh_title()
        self._refresh_goal()
        self._refresh_footer()
        # Worker poll must start immediately — hydrate is best-effort in background (was blocking ≤45s).
        self.set_interval(0.04, self._pump_thread_ui)
        threading.Thread(target=self._sse_loop, name="remoroo-sse", daemon=True).start()
        threading.Thread(target=self._poll_loop, name="remoroo-poll", daemon=True).start()
        self._hydrate_timeline_async()
        self.call_after_refresh(self._focus_timeline)
        self.call_after_refresh(self._sync_pause_from_server)

    def _pump_thread_ui(self) -> None:
        """Drain worker/SSE updates on the main thread (non-blocking for worker thread)."""
        max_batch = 250
        for _ in range(max_batch):
            try:
                kind, payload = self._ui_q.get_nowait()
            except queue.Empty:
                break
            try:
                if kind == "worker":
                    self._worker_line(payload)
                elif kind == "agent_event":
                    self._apply_agent_event(payload)
                elif kind == "hydrate_batch":
                    for d in payload:
                        if not isinstance(d, dict) or "kind" not in d:
                            continue
                        fp = self._event_fingerprint(d)
                        if fp in self._replay_seen:
                            continue
                        self._replay_seen.add(fp)
                        self._apply_agent_event(d)
                    if self.model.turns:
                        self._refresh_footer()
                        try:
                            ol = self.query_one("#timeline", OptionList)
                            if ol.option_count and self.model.follow_live:
                                ol.highlighted = ol.option_count - 1
                        except Exception:
                            pass
                elif kind == "sse":
                    self._set_sse(payload)
                elif kind == "now":
                    self._set_now(payload)
            except Exception as e:
                _log.exception("TUI pump failed (kind=%s): %s", kind, e)
        if self._pending_detail_refresh:
            self._pending_detail_refresh = False
            if self.model.follow_live:
                self._refresh_detail_logs()

    def _focus_timeline(self) -> None:
        try:
            self.query_one("#timeline", OptionList).focus()
        except Exception:
            pass

    def _event_fingerprint(self, d: Dict[str, Any]) -> str:
        try:
            return json.dumps(d, sort_keys=True, default=str)
        except TypeError:
            return json.dumps({"kind": d.get("kind", ""), "repr": repr(d)})

    def _hydrate_timeline_async(self) -> None:
        """Fetch archived agent events without blocking poll/SSE (short timeout, fail-open)."""

        def _fetch() -> None:
            import requests as _req

            try:
                r = _req.get(
                    f"{self.api_url}/runs/{self.remote_run_id}/agent_events",
                    headers={"Authorization": f"Bearer {self.session_key}"},
                    timeout=12.0,
                )
                if r.status_code != 200:
                    return
                body = r.json()
                evs = body.get("events") or []
                if not isinstance(evs, list) or not evs:
                    return
                batch = [d for d in evs if isinstance(d, dict) and "kind" in d]
                if batch:
                    self._ui_q.put(("hydrate_batch", batch))
            except Exception as e:
                _log.debug("hydrate skipped: %s", e)

        threading.Thread(target=_fetch, name="remoroo-hydrate", daemon=True).start()

    def _refresh_brand(self) -> None:
        self.query_one("#brand-strip", Static).update(BRAND_MARKUP_LINE)

    def _refresh_title(self) -> None:
        m = self.model
        dot = "[bold green]●[/]" if m.sse_ok else "[bold red]○[/]"
        line = (
            f"{dot} [cyan]{m.run_id}[/]  "
            f"[dim]│[/]  [yellow]{m.repo_short}[/]  [dim]│[/]  [magenta]{m.engine}[/]"
        )
        self.query_one("#title-strip", Static).update(line)

    def _refresh_timeline_header(self) -> None:
        m = self.model
        badge = _budget_timeline_badge(m.budget)
        line = f"[bold #58a6ff]REMOROO[/] [dim]│[/] [bold]Turns[/]  {badge}"
        self.query_one("#timeline-header", Static).update(line)

    def _refresh_goal(self) -> None:
        m = self.model
        g = (m.goal or "—").replace("\n", " ")
        met = (m.metric or "").replace("\n", " ")
        if len(g) > 120:
            g = g[:117] + "…"
        if len(met) > 100:
            met = met[:97] + "…"
        ep = ""
        if getattr(m, "episode_index", 0) > 0:
            ep = f"[dim]Ep {m.episode_index}[/]  "
        snap = getattr(m, "last_metrics_snapshot", None) or {}
        snap_s = ""
        if snap:
            parts = [f"{k}={v}" for k, v in list(snap.items())[:10]]
            snap_s = "  [dim]│[/]  " + " ".join(parts)
            if len(snap_s) > 200:
                snap_s = snap_s[:197] + "…"
        line1 = f"{ep}[bold]{g}[/]"
        line2 = (
            f"[dim]Metric:[/] [italic]{met}[/]{snap_s}"
            if met
            else f"[dim]Metric:[/] —{snap_s}" if snap_s else ""
        )
        text = line1 + ("\n" + line2 if line2 else "")
        self.query_one("#goal-strip", Static).update(text)

    def _refresh_footer(self) -> None:
        m = self.model
        self.query_one("#footer-strip", Static).update(_budget_footer_lines(m.budget))
        self._refresh_timeline_header()

    def _current_turn(self) -> Optional[TurnModel]:
        return self.model.turns[-1] if self.model.turns else None

    def _visible_tools(self, turn: TurnModel) -> List[ToolModel]:
        return [t for t in turn.tools if not _is_plumbing(t.name, t.summary)]

    def _turn_prompt(self, turn: TurnModel) -> str:
        vis = self._visible_tools(turn)
        if vis:
            parts = []
            for t in vis:
                icon = _TOOL_ICONS.get(t.name, "•")
                st = ""
                if t.status == "running":
                    st = " [blink yellow]…[/]"
                elif t.status == "error":
                    st = " [red]✗[/]"
                elif t.status == "done":
                    st = " [green]✓[/]"
                parts.append(f"{icon} [bold]{t.name}[/]{st}")
            tools_s = " ".join(parts)
        else:
            tools_s = turn.planned_label or "…"
        return f"[bold cyan]T{turn.turn_num}[/]  [dim]│[/]  {tools_s}"

    def _turn_index_from_option_id(self, oid: str) -> Optional[int]:
        for i, t in enumerate(self.model.turns):
            if t.option_id == oid:
                return i
        return None

    def _active_detail_index(self) -> int:
        if self.model.follow_live and self.model.turns:
            return len(self.model.turns) - 1
        if self._highlighted_turn_index is not None:
            return self._highlighted_turn_index
        return len(self.model.turns) - 1 if self.model.turns else -1

    def _refresh_detail_logs(self) -> None:
        idx = self._active_detail_index()
        if idx < 0 or idx >= len(self.model.turns):
            return
        turn = self.model.turns[idx]
        alog = self.query_one("#assistant-log", RichLog)
        tlog = self.query_one("#tool-log", RichLog)
        alog.clear()
        tlog.clear()
        if turn.assistant.strip():
            for para in turn.assistant.strip().split("\n\n"):
                alog.write(para.strip())
                alog.write("")
        else:
            alog.write("[dim](no assistant message for this turn yet)[/]")
        parts: List[str] = []
        for t in self._visible_tools(turn):
            icon = _TOOL_ICONS.get(t.name, "•")
            st = t.status.upper()
            hdr = f"{icon} {t.name}  {st}"
            if t.summary:
                hdr += f"  {t.summary}"
            parts.append(hdr)
            if t.log_lines:
                parts.append("\n".join(t.log_lines[-400:]))
            parts.append("")
        if turn.worker_notes:
            parts.append("— worker —")
            parts.extend(turn.worker_notes[-80:])
        if parts:
            tlog.write("\n\n".join(parts))
        else:
            tlog.write("[dim](no tool output)[/]")

    def _append_timeline_option(self, turn: TurnModel) -> None:
        ol = self.query_one("#timeline", OptionList)
        ol.add_option(Option(self._turn_prompt(turn), id=turn.option_id))
        if self.model.follow_live:
            ol.highlighted = ol.option_count - 1

    def _update_timeline_prompt(self, turn: TurnModel) -> None:
        try:
            self.query_one("#timeline", OptionList).replace_option_prompt(
                turn.option_id, self._turn_prompt(turn)
            )
        except Exception:
            pass

    def _refresh_raw_visibility(self) -> None:
        raw = self.query_one("#raw-pane", Vertical)
        if self.model.show_raw:
            raw.add_class("visible")
        else:
            raw.remove_class("visible")

    # --- thread-safe ---

    def push_agent_event(self, d: Dict[str, Any]) -> None:
        self._ui_q.put(("agent_event", d))

    def push_sse_state(self, ok: bool) -> None:
        self._ui_q.put(("sse", ok))

    def worker_print(self, text: str) -> None:
        self._ui_q.put(("worker", text))

    def set_now(self, text: str) -> None:
        self._ui_q.put(("now", text))

    def finish_success(
        self,
        *,
        outcome: str,
        success: bool,
        partial_success: bool,
        final_result: Optional[Dict[str, Any]],
    ) -> None:
        # Must use App.call_from_thread — Screen has no such API.
        app = self.app
        if app is not None:
            app.call_from_thread(
                self._finish, outcome, success, partial_success, final_result
            )

    def finish_error(self, message: str) -> None:
        app = self.app
        if app is not None:
            app.call_from_thread(self._finish_err, message)

    def _set_sse(self, ok: bool) -> None:
        self.model.sse_ok = ok
        self._refresh_title()

    def _set_now(self, text: str) -> None:
        self.model.now_text = text
        self._refresh_footer()

    def _worker_line(self, text: str) -> None:
        m = self.model
        m.raw_lines.append(text)
        if len(m.raw_lines) > 3000:
            m.raw_lines = m.raw_lines[-2500:]
        if m.show_raw:
            self.query_one("#raw-log", RichLog).write(text)
        sink = m.current_sink
        if sink is not None:
            sink.append(text)
        else:
            turn = self._current_turn()
            if turn is not None:
                if not any(t.name == "bash" for t in turn.tools):
                    m.orphan_worker.append(text)
                    if len(m.orphan_worker) > 400:
                        m.orphan_worker = m.orphan_worker[-250:]
                else:
                    turn.worker_notes.append(text)
                    if len(turn.worker_notes) > 200:
                        turn.worker_notes = turn.worker_notes[-150:]
        if m.follow_live:
            self._pending_detail_refresh = True

    def _apply_agent_event(self, d: Dict[str, Any]) -> None:
        kind = d.get("kind", "")
        if kind == "run_started":
            self.model.goal = d.get("goal", "") or ""
            self.model.metric = d.get("metric", "") or ""
            if d.get("run_id"):
                self.model.run_id = d.get("run_id", self.model.run_id)
            self._refresh_goal()
            self._refresh_title()
        elif kind == "turn_started":
            tid = f"tid-{len(self.model.turns)}"
            turn = TurnModel(
                turn_num=int(d.get("turn_num", len(self.model.turns) + 1)),
                option_id=tid,
                planned_label=self._planned_from_turn(d),
            )
            self.model.turns.append(turn)
            self._append_timeline_option(turn)
            self.model.last_tool_short = f"turn {turn.turn_num}"
            if self.model.pending_assistant_buffer:
                turn.assistant = self.model.pending_assistant_buffer
                self.model.pending_assistant_buffer = ""
            self._refresh_footer()
            if self.model.follow_live:
                self._pending_detail_refresh = True
        elif kind == "assistant_message":
            text = (d.get("text") or "").strip()
            if not text:
                return
            turn = self._current_turn()
            if turn is None:
                b = self.model.pending_assistant_buffer
                self.model.pending_assistant_buffer = (b + "\n\n" + text).strip() if b else text
                return
            turn.assistant = (turn.assistant + "\n\n" + text) if turn.assistant else text
            if self.model.follow_live:
                self._pending_detail_refresh = True
        elif kind == "tool_call_started":
            name = d.get("tool_name", "")
            summary = d.get("args_summary", "") or ""
            if _is_plumbing(name, summary):
                return
            turn = self._current_turn()
            if turn is None:
                return
            cid = str(d.get("call_id") or "")
            tool = ToolModel(name=name, summary=summary, status="running", call_id=cid)
            turn.tools.append(tool)
            self.model.last_tool_short = f"{name} {summary[:40]}"
            if name == "bash":
                self.model.current_sink = tool.log_lines
                for line in self.model.orphan_worker:
                    tool.log_lines.append(line)
                self.model.orphan_worker.clear()
            self._update_timeline_prompt(turn)
            self._refresh_footer()
            if self.model.follow_live:
                self._pending_detail_refresh = True
        elif kind == "tool_call_completed":
            name = d.get("tool_name", "")
            data = d.get("data") or {}
            snap_updated = False
            if isinstance(data, dict):
                ms = data.get("metrics_snapshot")
                if isinstance(ms, dict) and ms:
                    self.model.last_metrics_snapshot = dict(ms)
                    snap_updated = True
                elif name == "metric_gate":
                    mg = data.get("metrics")
                    if isinstance(mg, dict) and mg:
                        self.model.last_metrics_snapshot = dict(mg)
                        snap_updated = True
            if snap_updated:
                self._refresh_goal()
            if _is_plumbing(name, d.get("output_preview", "")):
                return
            turn = self._current_turn()
            if turn is None:
                return
            cid = str(d.get("call_id") or "")
            matched = False
            if cid:
                for t in turn.tools:
                    if t.call_id == cid:
                        t.status = "done" if d.get("success", True) else "error"
                        if t.name == "bash" and self.model.current_sink is t.log_lines:
                            self.model.current_sink = None
                        matched = True
                        break
            if not matched:
                for t in reversed(turn.tools):
                    if t.name == name and t.status == "running":
                        t.status = "done" if d.get("success", True) else "error"
                        if name == "bash":
                            self.model.current_sink = None
                        break
            self._update_timeline_prompt(turn)
            if self.model.follow_live:
                self._pending_detail_refresh = True
        elif kind == "metrics_snapshot":
            mets = d.get("metrics") or {}
            if isinstance(mets, dict):
                self.model.last_metrics_snapshot = dict(mets)
            self._refresh_goal()
        elif kind == "iterate_episode":
            self.model.episode_index = int(d.get("episode_index", 0))
            sp = (d.get("summary_preview") or "").strip()
            turn = self._current_turn()
            if turn is not None and sp:
                turn.worker_notes.append(
                    f"[iterate · episode {self.model.episode_index}] {sp[:300]}"
                )
            self._refresh_goal()
            if self.model.follow_live:
                self._pending_detail_refresh = True
        elif kind == "awaiting_human_input":
            # Agent called ask_human and is blocking until POST /runs/{id}/answer
            # arrives. Surface the question prominently in the timeline so the
            # operator notices and can answer from another shell:
            #   remoroo answer <run_id> "<text>"
            q = (d.get("question") or "").strip()
            ctx_text = (d.get("context") or "").strip()
            default = (d.get("default") or "").strip()
            try:
                tmin = int(round(float(d.get("timeout_s") or 0) / 60.0))
            except Exception:
                tmin = 30
            rid = self.model.run_id or d.get("run_id", "")
            note = (
                f"❓ AWAITING ANSWER (timeout {tmin}m): {q}"
                + (f"\n   why: {ctx_text}" if ctx_text else "")
                + (f"\n   default: {default}" if default else "")
                + f"\n   answer with:  remoroo answer {rid} \"<your reply>\""
            )
            turn = self._current_turn()
            if turn is not None:
                turn.worker_notes.append(note)
            self.model.last_tool_short = f"awaiting answer (Q{int(d.get('call_index', 0)) + 1})"
            self._refresh_footer()
            if self.model.follow_live:
                self._pending_detail_refresh = True
        elif kind == "human_input_received":
            timed_out = bool(d.get("timed_out"))
            aborted = bool(d.get("aborted"))
            ans = (d.get("answer") or "").strip()
            wait_s = float(d.get("wait_s") or 0)
            if aborted:
                note = "🛑 ask_human aborted (run was aborted by operator)."
            elif timed_out:
                note = f"⏱  ask_human timed out after {wait_s:.0f}s — using default: {ans or '(none)'}"
            else:
                preview = ans if len(ans) <= 200 else (ans[:200] + "…")
                note = f"✅ Operator answered ({wait_s:.0f}s): {preview}"
            turn = self._current_turn()
            if turn is not None:
                turn.worker_notes.append(note)
            self.model.last_tool_short = "answered"
            self._refresh_footer()
            if self.model.follow_live:
                self._pending_detail_refresh = True

    def _planned_from_turn(self, d: Dict[str, Any]) -> str:
        names = d.get("tool_names") or []
        if not names:
            return ""
        counts: Dict[str, int] = {}
        order: List[str] = []
        for n in names:
            counts[n] = counts.get(n, 0) + 1
            if n not in order:
                order.append(n)
        parts = [f"{n}×{counts[n]}" if counts[n] > 1 else n for n in order]
        return ", ".join(parts)

    def _finish(
        self,
        outcome: str,
        success: bool,
        partial_success: bool,
        final_result: Optional[Dict[str, Any]],
    ) -> None:
        self.stop_flag.set()
        self.result_box["_cleanup_worker"] = self._worker
        self.result_box.update(
            outcome=outcome,
            success=success,
            partial_success=partial_success,
            final_result=final_result,
        )
        self.model.now_text = f"Done: {outcome}"
        self._refresh_footer()
        self.notify(f"Run finished: {outcome}", severity="information" if success else "warning")
        # Return AwaitComplete so App.call_from_thread → invoke() awaits pop_screen (unblocks push_screen_wait).
        return self.dismiss(self.result_box)

    def _finish_err(self, message: str) -> None:
        self.stop_flag.set()
        self.result_box["_cleanup_worker"] = self._worker
        self.result_box.update(
            outcome=f"ERROR: {message}",
            success=False,
            partial_success=False,
            final_result=None,
        )
        self.notify(message, severity="error")
        return self.dismiss(self.result_box)

    def action_quit(self) -> None:
        import requests as _req

        w = self._worker
        if w is not None:
            try:
                w.kill_all_background_jobs()
            except Exception:
                pass
            try:
                if getattr(w, "sandbox", None):
                    w.sandbox.stop()
            except Exception:
                pass
        try:
            _req.post(
                f"{self.api_url}/runs/{self.remote_run_id}/abort",
                headers={"Authorization": f"Bearer {self.session_key}"},
                timeout=8.0,
            )
        except Exception:
            pass
        self.stop_flag.set()
        self.result_box["_cleanup_worker"] = self._worker
        self.result_box.update(
            outcome="INTERRUPTED",
            success=False,
            partial_success=False,
            final_result=None,
        )
        self.dismiss(self.result_box)

    def _sync_pause_from_server(self) -> None:
        import requests as _req

        try:
            r = _req.get(
                f"{self.api_url}/runs/{self.remote_run_id}",
                headers={"Authorization": f"Bearer {self.session_key}"},
                timeout=8.0,
            )
            if r.status_code != 200:
                return
            st = (r.json().get("run") or {}).get("status")
            if st == "PAUSED":
                self.model.ui_paused = True
                self._refresh_footer()
        except Exception:
            pass

    def action_pause_toggle(self) -> None:
        import requests as _req

        h = {"Authorization": f"Bearer {self.session_key}"}
        try:
            if self.model.ui_paused:
                r = _req.post(f"{self.api_url}/runs/{self.remote_run_id}/resume", headers=h, timeout=15.0)
                if r.status_code == 200:
                    self.model.ui_paused = False
                    self.notify("Resumed", severity="information")
                else:
                    self.notify(f"Resume failed: HTTP {r.status_code}", severity="error")
            else:
                r = _req.post(f"{self.api_url}/runs/{self.remote_run_id}/pause", headers=h, timeout=15.0)
                if r.status_code == 200:
                    self.model.ui_paused = True
                    self.notify("Pause requested — agent stops after current step", severity="warning")
                else:
                    self.notify(f"Pause failed: HTTP {r.status_code}", severity="error")
        except Exception as e:
            self.notify(f"Pause/resume error: {e}", severity="error")
        self._refresh_footer()

    def action_detach(self) -> None:
        import requests as _req

        try:
            r = _req.post(
                f"{self.api_url}/runs/{self.remote_run_id}/detach",
                headers={"Authorization": f"Bearer {self.session_key}"},
                timeout=12.0,
            )
            if r.status_code != 200:
                self.notify(f"Detach API failed: HTTP {r.status_code}", severity="error")
        except Exception as e:
            self.notify(f"Detach failed: {e}", severity="error")
        self.model.ui_paused = False
        self.stop_flag.set()
        self.result_box["_cleanup_worker"] = self._worker
        self.result_box.update(
            outcome="DETACHED",
            success=False,
            partial_success=False,
            final_result=None,
        )
        self.model.now_text = "Detached (tmux-style — run stays up; reattach when ready)"
        self._refresh_footer()
        self.notify(
            "Detached: run stays RUNNING on server. Reattach: remoroo run --local --resume …",
            severity="information",
            timeout=7,
        )
        self.dismiss(self.result_box)

    def action_go_live(self) -> None:
        self.model.follow_live = True
        self._highlighted_turn_index = None
        ol = self.query_one("#timeline", OptionList)
        if ol.option_count:
            ol.highlighted = ol.option_count - 1
        self._refresh_footer()
        self._refresh_detail_logs()

    def action_toggle_raw(self) -> None:
        self.model.show_raw = not self.model.show_raw
        self._refresh_raw_visibility()
        if self.model.show_raw:
            rl = self.query_one("#raw-log", RichLog)
            rl.clear()
            for line in self.model.raw_lines[-500:]:
                rl.write(line)

    def action_help(self) -> None:
        self.notify(
            "Quit (q): abort server run, kill local bash/docker/venv children, stop sandbox. "
            "Detach (^d): tmux-style — run stays RUNNING; CP marks graceful detach until you "
            "reattach (--resume). Pause (p): cooperative hold. ^r raw · g live · arrows.",
            title="Remoroo — keys",
            timeout=12,
        )

    @on(OptionList.OptionHighlighted, "#timeline")
    def timeline_highlighted(self, event: OptionList.OptionHighlighted) -> None:
        opt = event.option_id
        if opt is None:
            return
        idx = self._turn_index_from_option_id(opt)
        if idx is None:
            return
        self._highlighted_turn_index = idx
        self.model.follow_live = idx == len(self.model.turns) - 1
        self._refresh_footer()
        self._refresh_detail_logs()

    def _make_worker(self) -> Any:
        from .engine.local_worker import WorkerService

        return WorkerService(
            repo_root=self._exec_repo_root,
            artifact_dir=self._artifact_for_worker,
            original_repo_root=self.original_repo_path,
            run_id=self.remote_run_id,
            engine=self.engine,
            persistence_dir=str(self.artifact_dir),
            output_callback=self.worker_print,
            cache_env=self.cache_env,
            in_place=self.in_place,
        )

    def _poll_loop(self) -> None:
        from .engine.protocol import ExecutionRequest, ExecutionResult

        time.sleep(0.08)
        if self.stop_flag.is_set():
            return
        self._worker = self._make_worker()
        w = self._worker
        last_processed_id = None
        last_result = None
        server = self._server

        while not self.stop_flag.is_set():
            try:
                step, _cm, _bm = server.get_next_step(timeout=10.0, run_id=self.remote_run_id)
            except Exception as e:
                self.finish_error(str(e))
                return
            if step is None:
                time.sleep(0.08)
                continue

            if step.request_id and step.request_id == last_processed_id:
                if last_result:
                    self.worker_print(f"[dim]Resending cached result for {step.request_id}[/]")
                    server.submit_result(last_result)
                continue

            if step.type == "workflow_complete":
                fr = step.payload or {}
                outcome = fr.get("outcome") or fr.get("decision", "UNKNOWN")
                success = fr.get("success") is True or outcome == "SUCCESS"
                partial = fr.get("partial_success") is True or outcome == "PARTIAL_SUCCESS"
                self.finish_success(
                    outcome=outcome, success=success, partial_success=partial, final_result=fr
                )
                return

            if step.type == "metrics_update":
                ack = ExecutionResult(success=True, data={})
                ack.request_id = step.request_id
                server.submit_result(ack)
                pl = step.payload or {}
                mets = pl.get("metrics")
                if isinstance(mets, dict):
                    self._ui_q.put((
                        "agent_event",
                        {"kind": "metrics_snapshot", "metrics": mets},
                    ))
                continue

            if step.type == "workflow_error":
                self.finish_error(step.payload.get("error", "workflow_error"))
                return

            self.set_now(step.type)
            try:
                result = w.handle_request(step)
                if not result.request_id:
                    result.request_id = step.request_id
            except Exception as e:
                result = ExecutionResult(success=False, error=str(e), request_id=step.request_id)

            if step.type == "create_working_copy" and result.success:
                new_root = result.data.get("working_path")
                is_in_place = result.data.get("in_place", False)
                if new_root and not is_in_place:
                    try:
                        w.handle_request(ExecutionRequest(type="cleanup_working_copy", payload={}))
                    except Exception:
                        pass
                    self._exec_repo_root = new_root
                    self._artifact_for_worker = None
                    self._worker = self._make_worker()
                    w = self._worker
                    self.worker_print(f"[bold yellow]Switched context:[/] [dim]{new_root}[/]")

            server.submit_result(result)
            last_processed_id = step.request_id
            last_result = result
            self.set_now("idle")

    def _sse_loop(self) -> None:
        import requests as _req

        # Long-lived SSE: a finite read timeout (e.g. 90s) fires during quiet periods or behind
        # proxies with idle limits, which used to exhaust max_retries and permanently drop the stream.
        backoff = 1.0
        max_backoff = 60.0
        fail_streak = 0
        while not self.stop_flag.is_set():
            try:
                url = f"{self.api_url}/runs/{self.remote_run_id}/stream"
                # Bounded read idle: CP emits ~5 Hz; reconnect on dead sockets without hanging forever.
                read_idle = float(os.getenv("REMOROO_SSE_READ_TIMEOUT", "180"))
                with _req.get(
                    url,
                    stream=True,
                    headers={"Authorization": f"Bearer {self.session_key}"},
                    timeout=(25, read_idle),
                ) as resp:
                    resp.raise_for_status()
                    backoff = 1.0
                    fail_streak = 0
                    self.push_sse_state(True)
                    pending = None
                    for raw_line in resp.iter_lines(decode_unicode=True):
                        if self.stop_flag.is_set():
                            return
                        if not raw_line or raw_line.startswith(":"):
                            pending = None
                            continue
                        if raw_line.startswith("event:"):
                            pending = raw_line[6:].strip()
                            continue
                        if raw_line.startswith("data:"):
                            data_str = raw_line[5:].strip()
                            if pending == "finish":
                                self.push_sse_state(False)
                                return
                            if pending == "agent_event" and data_str:
                                try:
                                    data = json.loads(data_str)
                                    if isinstance(data, dict) and "kind" in data:
                                        fp = self._event_fingerprint(data)
                                        if fp in self._replay_seen:
                                            pass
                                        else:
                                            self._replay_seen.add(fp)
                                            self.push_agent_event(data)
                                except (json.JSONDecodeError, ValueError):
                                    pass
                            pending = None
            except Exception:
                fail_streak += 1
                self.push_sse_state(False)
                if self.stop_flag.is_set():
                    return
                if fail_streak == 2:
                    self.worker_print(
                        "[dim]Live stream reconnecting — worker poll keeps the run going[/]"
                    )
                time.sleep(backoff)
                backoff = min(max_backoff, backoff * 1.5)


class _LegacyRunTuiApp(App[Dict[str, Any]]):
    """Single-screen host so ``RemorooRunScreen.dismiss`` returns the result dict."""

    TITLE = "Remoroo"
    SUB_TITLE = "Autonomous Engineering"
    CSS = RemorooRunScreen.CSS

    def __init__(self, run_screen: RemorooRunScreen) -> None:
        super().__init__()
        self._run_screen = run_screen

    async def on_mount(self) -> None:
        self._await_run_screen()

    @work(exclusive=True)
    async def _await_run_screen(self) -> None:
        rb = await self.push_screen_wait(self._run_screen)
        if not isinstance(rb, dict) or not rb:
            rb = getattr(self._run_screen, "result_box", {}) or {}
        self.exit(rb)


def run_remoroo_tui_session(
    *,
    server: Any,
    api_url: str,
    session_key: str,
    remote_run_id: str,
    repo_path: str,
    engine: str,
    artifact_dir: str,
    original_repo_path: str,
    cache_env: bool,
    in_place: bool,
    budget_ui: Optional[BudgetTuiState] = None,
) -> Dict[str, Any]:
    result_box: Dict[str, Any] = {}
    stop_flag = threading.Event()
    repo_short = str(repo_path).rstrip("/").split("/")[-1] or str(repo_path)
    model = RunTuiModel(
        run_id=remote_run_id,
        repo_short=repo_short,
        engine=engine,
        budget=budget_ui,
    )

    screen = RemorooRunScreen(
        model=model,
        result_box=result_box,
        stop_flag=stop_flag,
        api_url=api_url,
        session_key=session_key,
        remote_run_id=remote_run_id,
        artifact_dir=artifact_dir,
        original_repo_path=original_repo_path,
        engine=engine,
        cache_env=cache_env,
        in_place=in_place,
        server=server,
    )
    out = _LegacyRunTuiApp(screen).run()
    if not out:
        out = dict(result_box)
    if not out:
        out.update(outcome="UNKNOWN", success=False, partial_success=False, final_result=None)
    return out


# Backward-compatible name
RemorooRunApp = RemorooRunScreen
