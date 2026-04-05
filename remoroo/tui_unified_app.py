"""Single Textual app: wizard, model pick, prepare, live run, results + patch."""
from __future__ import annotations

import subprocess
import sys
import threading
from dataclasses import replace
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple, Union

from textual import events, on, work
from textual.app import App, ComposeResult
from textual.binding import Binding
from textual.containers import Horizontal, Vertical
from textual.message import Message
from textual.screen import Screen
from textual.widgets import Button, Footer, Header, LoadingIndicator, Static, TextArea

from textual.widgets import OptionList
from textual.widgets.option_list import Option

from .branding import (
    BRAND_MARKUP_CONNECTING,
    BRAND_MARKUP_CONFIRM,
    BRAND_MARKUP_MODEL_PICKER,
    BRAND_MARKUP_RESULTS,
    BRAND_MARKUP_WIZARD,
)
from .model_picker import CHOICES
from .run_local import (
    LocalRunResult,
    LocalWorkerContext,
    RunPrepareError,
    finalize_local_worker_session,
    prepare_local_worker_context,
)
from .run_summary_helpers import (
    apply_patch_to_repo,
    artifact_paths,
    exit_code_for_local_result,
    load_metrics_comparison_rows,
    outcome_style_category,
    should_prompt_patch,
)
from .tui_launch_config import LaunchConfig, WizardStep, skip_model_screen, wizard_steps_needed
from .tui_run import BudgetTuiState, RemorooRunScreen, RunTuiModel
from .tui_theme import REMOROO_UNIFIED_SCREENS_CSS


class WizardSubmit(Message):
    """Posted when the user presses Enter in a wizard prompt (continue, not newline)."""


class WizardPromptTextArea(TextArea):
    """Enter submits the step; Ctrl+J inserts a newline (for multi-line paste-style input)."""

    async def _on_key(self, event: events.Key) -> None:
        if self.read_only:
            return
        if event.key == "ctrl+j":
            event.stop()
            event.prevent_default()
            start, end = self.selection
            self._replace_via_keyboard("\n", start, end)
            return
        if event.key == "enter":
            event.stop()
            event.prevent_default()
            self.post_message(WizardSubmit())
            return
        await super()._on_key(event)


class WizardGoalScreen(Screen[Optional[str]]):
    """Dismiss with goal text, or None if cancelled."""

    BINDINGS = [Binding("escape", "cancel", "Cancel", show=True)]

    def compose(self) -> ComposeResult:
        yield Header(show_clock=True)
        yield Static(BRAND_MARKUP_WIZARD, id="brand-strip", markup=True)
        yield Static(
            "Goal (required) — Enter continues · Ctrl+J newline",
            id="context-strip",
        )
        with Vertical(id="wizard-body"):
            yield WizardPromptTextArea(id="goal-input", show_line_numbers=False)
        with Horizontal(id="action-row"):
            yield Button("Continue", variant="primary", id="btn-go", classes="-primary")
            yield Button("Cancel", id="btn-cancel")
        yield Footer()

    def on_mount(self) -> None:
        self.query_one("#goal-input", WizardPromptTextArea).focus()

    @on(WizardSubmit)
    def _submit_goal_enter(self) -> None:
        self._go()

    @on(Button.Pressed, "#btn-go")
    def _go(self) -> None:
        g = self.query_one("#goal-input", WizardPromptTextArea).text.strip()
        if not g:
            self.notify("Goal is required.", severity="error")
            return
        self.dismiss(g)

    @on(Button.Pressed, "#btn-cancel")
    def _cancel_btn(self) -> None:
        self.dismiss(None)

    def action_cancel(self) -> None:
        self.dismiss(None)


class WizardMetricsScreen(Screen[Optional[List[str]]]):
    """Dismiss with metrics list (empty = goal-only). None = cancel."""

    BINDINGS = [Binding("escape", "cancel", "Cancel", show=True)]

    def compose(self) -> ComposeResult:
        yield Header(show_clock=True)
        yield Static(BRAND_MARKUP_WIZARD, id="brand-strip", markup=True)
        yield Static(
            "Metrics (optional) — Enter continues · Ctrl+J newline · empty = goal-only",
            id="context-strip",
        )
        with Vertical(id="wizard-body"):
            yield WizardPromptTextArea(id="metrics-input", show_line_numbers=False)
        with Horizontal(id="action-row"):
            yield Button("Continue", id="btn-go", classes="-primary")
            yield Button("Cancel", id="btn-cancel")
        yield Footer()

    def on_mount(self) -> None:
        self.query_one("#metrics-input", WizardPromptTextArea).focus()

    @on(WizardSubmit)
    def _submit_metrics_enter(self) -> None:
        self._go()

    @on(Button.Pressed, "#btn-go")
    def _go(self) -> None:
        raw = self.query_one("#metrics-input", WizardPromptTextArea).text
        lines = [ln.strip() for ln in raw.splitlines() if ln.strip()]
        self.dismiss(lines)

    @on(Button.Pressed, "#btn-cancel")
    def _cancel(self) -> None:
        self.dismiss(None)

    def action_cancel(self) -> None:
        self.dismiss(None)


class WizardConfirmScreen(Screen[bool]):
    """Dismiss True = proceed, False = abort."""

    def __init__(self, cfg: LaunchConfig, *, mode: str) -> None:
        super().__init__()
        self._cfg = cfg
        self._mode = mode

    def compose(self) -> ComposeResult:
        yield Header(show_clock=True)
        yield Static(BRAND_MARKUP_CONFIRM, id="brand-strip", markup=True)
        if self._mode == "attach":
            body = (
                f"Attach to run [bold]{self._cfg.resume_run_id}[/]\n"
                f"Status: {self._cfg.attach_status or '—'}\n"
                f"Repository: {self._cfg.repo_path}\n"
            )
            if self._cfg.attach_goal_preview:
                body += f"Goal: {self._cfg.attach_goal_preview[:200]}\n"
            title = "Confirm attach"
        else:
            met = ", ".join(self._cfg.metrics_list) if self._cfg.metrics_list else "(goal-only)"
            body = (
                f"Repository: {self._cfg.repo_path}\n"
                f"Goal: {self._cfg.goal[:300]}{'…' if len(self._cfg.goal) > 300 else ''}\n"
                f"Metrics: {met}\n"
                f"Engine: {self._cfg.engine}  ·  budget: {self._cfg.max_wall_time_s / 3600:.1f} h\n"
            )
            title = "Confirm new run"
        yield Static(title, id="context-strip")
        with Vertical(id="wizard-body"):
            yield Static(body, id="confirm-body")
        with Horizontal(id="action-row"):
            yield Button("Proceed", id="btn-yes", classes="-primary")
            yield Button("Cancel", id="btn-no")
        yield Footer()

    @on(Button.Pressed, "#btn-yes")
    def _yes(self) -> None:
        self.dismiss(True)

    @on(Button.Pressed, "#btn-no")
    def _no(self) -> None:
        self.dismiss(False)


class ModelPickSuiteScreen(Screen[Optional[str]]):
    BINDINGS = [Binding("escape", "default", "Default", show=True)]

    def compose(self) -> ComposeResult:
        yield Header(show_clock=True)
        yield Static(BRAND_MARKUP_MODEL_PICKER, id="brand-strip", markup=True)
        yield Static(
            "Enter selects; Esc = default (Haiku).",
            id="context-strip",
        )
        yield OptionList(
            *[Option(label, id=str(i)) for i, (label, _) in enumerate(CHOICES)],
            id="opts",
        )
        yield Footer()

    def on_mount(self) -> None:
        self.query_one("#opts", OptionList).focus()

    @on(OptionList.OptionSelected, "#opts")
    def _sel(self, event: OptionList.OptionSelected) -> None:
        _, mid = CHOICES[event.option_index]
        self.dismiss(mid if mid else None)

    def action_default(self) -> None:
        self.dismiss(None)


PrepareOutcome = Union[LocalWorkerContext, str]


class PrepareWorkerScreen(Screen[PrepareOutcome]):
    def __init__(self, cfg: LaunchConfig) -> None:
        super().__init__()
        self._cfg = cfg

    def compose(self) -> ComposeResult:
        yield Header(show_clock=True)
        yield Static(BRAND_MARKUP_CONNECTING, id="brand-strip", markup=True)
        yield Static("Creating run on control plane…", id="context-strip")
        with Vertical(id="wizard-body"):
            yield Static("", id="prep-status")
            yield LoadingIndicator(id="spinner")
        yield Footer()

    def on_mount(self) -> None:
        c = self._cfg

        def bg() -> None:
            try:
                ctx = prepare_local_worker_context(
                    repo_path=c.repo_path,
                    goal=c.goal,
                    metrics=c.metrics_list,
                    brain_url=c.brain_url,
                    engine=c.engine,
                    verbose=c.verbose,
                    cache_env=c.cache_env,
                    in_place=c.in_place,
                    agentic=c.agentic,
                    engine_version=c.engine_version,
                    model=c.model,
                    resume_run_id=c.resume_run_id,
                    max_wall_time_s=c.max_wall_time_s,
                    allow_overage=c.allow_overage,
                )
                self.app.call_from_thread(self.dismiss, ctx)
            except RunPrepareError as e:
                self.app.call_from_thread(self.dismiss, e.message)
            except Exception as e:
                self.app.call_from_thread(self.dismiss, str(e))

        threading.Thread(target=bg, name="remoroo-prepare", daemon=True).start()


def _metrics_static_text(run_root: Optional[Path]) -> str:
    if run_root is None:
        return ""
    rows = load_metrics_comparison_rows(run_root)
    if not rows:
        return ""
    lines = ["[bold]Metrics[/]", "Metric | Baseline | Final | Delta", "—" * 40]
    for m, b, f, d in rows:
        lines.append(f"{m} | {b} | {f} | {d}")
    return "\n".join(lines)


class PatchConfirmScreen(Screen[bool]):
    """Dismiss True = user chose to apply (caller runs git apply); False = skip."""

    def __init__(self, repo: Path, patch: Path) -> None:
        super().__init__()
        self._repo = repo
        self._patch = patch

    def compose(self) -> ComposeResult:
        with Vertical(id="patch-dialog"):
            with Vertical(id="patch-panel"):
                yield Static("[bold]Apply patch?[/]", markup=True)
                yield Static(f"Repo: {self._repo}\n{self._patch.name}")
                with Horizontal(id="action-row"):
                    yield Button("No", id="p-no")
                    yield Button("Yes", id="p-yes", classes="-primary")
        yield Footer()

    @on(Button.Pressed, "#p-no")
    def _no(self) -> None:
        self.dismiss(False)

    @on(Button.Pressed, "#p-yes")
    def _yes(self) -> None:
        self.dismiss(True)


class LocalResultsScreen(Screen):
    """Outcome summary, optional patch prompt. Calls app.exit() directly when done."""

    def __init__(self, cfg: LaunchConfig, result: LocalRunResult) -> None:
        super().__init__()
        self._cfg = cfg
        self._result = result
        self._closing = False
        if result.run_root is None:
            raise ValueError(
                "LocalResultsScreen requires result.run_root from finalize_local_worker_session"
            )

    def compose(self) -> ComposeResult:
        yield Header(show_clock=True)
        yield Static(BRAND_MARKUP_RESULTS, id="brand-strip", markup=True)
        yield Static("Run finished", id="context-strip")
        yield Static(
            "Review the summary below, then press Done to exit and return to your shell.",
            id="context-hint",
        )
        with Vertical(id="wizard-body"):
            cat = outcome_style_category(self._result)
            style = "green" if cat == "success" else ("yellow" if cat in ("partial", "warning") else "red")
            root = self._result.run_root
            assert root is not None
            summary = (
                f"[bold {style}]{self._result.outcome}[/bold {style}]\n"
                f"Run ID: {self._result.run_id}\n"
                f"Artifacts: {root}"
            )
            yield Static(summary, id="results-summary", markup=True)
            mt = _metrics_static_text(root)
            if mt:
                yield Static(mt, id="metrics-block", markup=True)
            arts = artifact_paths(root)
            alines = []
            if arts.get("report"):
                alines.append(f"Report: {arts['report']}")
            if arts.get("diagram"):
                alines.append(f"Diagram: {arts['diagram']}")
            if arts.get("patch"):
                alines.append(f"Patch: {arts['patch']}")
            if alines:
                yield Static("\n".join(alines), id="artifact-lines")
        yield Static("", id="closing-banner")
        with Horizontal(id="action-row"):
            yield Button("Open artifact dir", id="btn-open")
            yield Button("Done", id="btn-done", classes="-primary")
        yield Footer()

    @on(Button.Pressed, "#btn-open")
    def _open(self) -> None:
        root = self._result.run_root
        assert root is not None
        p = str(root.resolve())
        try:
            import sys

            if sys.platform == "darwin":
                subprocess.run(["open", p], check=False)
            elif sys.platform.startswith("win"):
                subprocess.run(["explorer", p], check=False)
            else:
                subprocess.run(["xdg-open", p], check=False)
        except Exception:
            self.notify(f"Artifacts: {p}", title="Path")

    @on(Button.Pressed, "#btn-done")
    def _done(self) -> None:
        if self._closing:
            return
        self._closing = True
        for bid in ("#btn-done", "#btn-open"):
            try:
                self.query_one(bid, Button).disabled = True
            except Exception:
                pass

        root = self._result.run_root
        assert root is not None
        arts = artifact_paths(root)
        patch_p = arts.get("patch")
        needs_patch_prompt = patch_p and should_prompt_patch(
            self._result, True, no_patch=self._cfg.no_patch, yes=self._cfg.yes,
        )
        auto_apply = (
            patch_p
            and self._cfg.yes
            and (self._result.success or self._result.partial_success)
        )

        if auto_apply and patch_p:
            ok, err = apply_patch_to_repo(self._cfg.repo_path, patch_p)
            if ok:
                self.notify("Patch applied.", severity="information")
            else:
                self.notify(f"Patch failed: {err}", severity="error")

        if needs_patch_prompt and patch_p:
            self._ask_patch_then_exit(patch_p)
        else:
            self._exit_app()

    def _ask_patch_then_exit(self, patch_p: Path) -> None:
        """Push patch confirm as a callback-style screen, then exit."""

        def _on_patch_answer(apply: bool) -> None:
            if apply:
                ok, err = apply_patch_to_repo(self._cfg.repo_path, patch_p)
                if ok:
                    self.notify("Patch applied.", severity="information")
                else:
                    self.notify(f"Patch failed: {err}", severity="error")
            self._exit_app()

        self.app.push_screen(
            PatchConfirmScreen(self._cfg.repo_path, patch_p),
            callback=_on_patch_answer,
        )

    def _exit_app(self) -> None:
        """Exit the whole Textual app with the result tuple."""
        import os

        lr = self._result
        code = exit_code_for_local_result(lr)
        self.app.exit((lr, code))
        # Textual's shutdown hangs in _close_all/_prune after app.exit().
        # This safety thread ensures the CLI always returns to the shell.
        def _force_exit() -> None:
            import time
            time.sleep(2)
            try:
                echo_session_finished_line(lr, code)
                sys.stdout.flush()
                sys.stderr.flush()
            except Exception:
                pass
            os._exit(code)

        threading.Thread(target=_force_exit, daemon=True, name="force-exit").start()


class RemorooUnifiedApp(App[Tuple[LocalRunResult, int]]):
    TITLE = "Remoroo"
    SUB_TITLE = "Autonomous Engineering"
    CSS = REMOROO_UNIFIED_SCREENS_CSS

    def __init__(self, launch_cfg: LaunchConfig) -> None:
        super().__init__()
        self._cfg = launch_cfg

    async def on_mount(self) -> None:
        # push_screen_wait must run inside a @work coroutine (Textual worker context).
        self._run_unified_pipeline()

    @work(exclusive=True)
    async def _run_unified_pipeline(self) -> None:
        cfg = self._cfg
        for step in wizard_steps_needed(cfg):
            if step == WizardStep.GOAL:
                g = await self.push_screen_wait(WizardGoalScreen())
                if g is None:
                    self.exit((LocalRunResult(None, "", False, "CANCELLED"), 0))
                    return
                cfg = replace(cfg, goal=g)
            elif step == WizardStep.METRICS:
                m = await self.push_screen_wait(WizardMetricsScreen())
                if m is None:
                    self.exit((LocalRunResult(None, "", False, "CANCELLED"), 0))
                    return
                cfg = replace(cfg, metrics_list=m)
            elif step == WizardStep.CONFIRM_NEW:
                ok = await self.push_screen_wait(WizardConfirmScreen(cfg, mode="new"))
                if not ok:
                    self.exit((LocalRunResult(None, "", False, "CANCELLED"), 0))
                    return
            elif step == WizardStep.CONFIRM_ATTACH:
                ok = await self.push_screen_wait(WizardConfirmScreen(cfg, mode="attach"))
                if not ok:
                    self.exit((LocalRunResult(None, "", False, "CANCELLED"), 0))
                    return

        if not skip_model_screen(cfg):
            mid = await self.push_screen_wait(ModelPickSuiteScreen())
            cfg = replace(cfg, model=mid)

        prep = await self.push_screen_wait(PrepareWorkerScreen(cfg))
        if isinstance(prep, str):
            self.notify(prep, severity="error", timeout=12)
            self.exit(
                (
                    LocalRunResult(
                        None,
                        cfg.resume_run_id or "",
                        False,
                        "PREPARE_FAILED",
                    ),
                    1,
                )
            )
            return
        ctx = prep

        repo_short = str(ctx.original_repo_path).rstrip("/").split("/")[-1] or ctx.original_repo_path
        budget = ctx.budget_ui
        model = RunTuiModel(
            run_id=ctx.remote_run_id,
            repo_short=repo_short,
            engine=ctx.engine,
            budget=budget if isinstance(budget, BudgetTuiState) else None,
        )
        result_box: Dict[str, Any] = {}
        stop_flag = threading.Event()
        run_screen = RemorooRunScreen(
            model=model,
            result_box=result_box,
            stop_flag=stop_flag,
            api_url=ctx.api_url,
            session_key=ctx.session_key,
            remote_run_id=ctx.remote_run_id,
            artifact_dir=str(ctx.run_output_dir.resolve()),
            original_repo_path=ctx.original_repo_path,
            engine=ctx.engine,
            cache_env=ctx.cache_env,
            in_place=ctx.in_place,
            server=ctx.server,
        )
        rb = await self.push_screen_wait(run_screen)
        if not isinstance(rb, dict):
            rb = dict(result_box)
        lr = finalize_local_worker_session(ctx, rb)
        await self.push_screen(LocalResultsScreen(cfg, lr))


def echo_session_finished_line(lr: LocalRunResult, code: int) -> None:
    """After the TUI exits, print a single clear line on the real terminal (TTY only)."""
    if not sys.stdout.isatty():
        return
    try:
        import typer

        if code == 0:
            fg = typer.colors.GREEN
        elif code == 2:
            fg = typer.colors.YELLOW
        else:
            fg = typer.colors.RED
        rid = lr.run_id or "(no id)"
        typer.secho(
            f"Remoroo session ended · {lr.outcome} · exit {code} · run {rid}",
            fg=fg,
        )
        if lr.run_root is not None:
            typer.secho(f"Artifacts: {lr.run_root}", fg=typer.colors.BRIGHT_BLACK)
    except Exception:
        extra = f" · {lr.run_root}" if lr.run_root is not None else ""
        print(f"Remoroo session ended · exit {code}{extra}", file=sys.stdout)


def run_unified_local_session(cfg: LaunchConfig) -> Tuple[LocalRunResult, int]:
    out = RemorooUnifiedApp(cfg).run()
    if out is None:
        lr = LocalRunResult(None, cfg.resume_run_id or "", False, "UNKNOWN")
        return lr, 1
    if isinstance(out, tuple) and len(out) == 2:
        return out[0], out[1]
    lr = LocalRunResult(None, cfg.resume_run_id or "", False, "UNKNOWN")
    return lr, 1
