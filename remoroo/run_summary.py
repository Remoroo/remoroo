"""Post-run Rich output + patch prompt (shared by `remoroo run` and `remoroo attach`)."""
from __future__ import annotations

import json
import subprocess
from pathlib import Path
from typing import TYPE_CHECKING

import typer
from rich.console import Console
from rich.panel import Panel
from rich.table import Table

if TYPE_CHECKING:
    from .run_local import LocalRunResult


def display_local_run_result(
    result: "LocalRunResult",
    repo_path: Path,
    *,
    verbose: bool = False,
    no_patch: bool = False,
    yes: bool = False,
) -> None:
    console = Console()

    if result.success:
        outcome_color = "bright_green"
    elif getattr(result, "partial_success", False):
        outcome_color = "bright_yellow"
    elif result.outcome == "INTERRUPTED":
        outcome_color = "bright_black"
    elif result.outcome == "DETACHED":
        outcome_color = "cyan"
    elif (
        "ERROR" in result.outcome
        or "CRASH" in result.outcome
        or result.outcome in ("FAIL", "FAILED", "ABORT")
    ):
        outcome_color = "red"
    else:
        outcome_color = "bright_yellow"

    console.print("")
    console.print(
        Panel(
            f"[bold {outcome_color}]{result.outcome}[/bold {outcome_color}]\n"
            f"Run ID: [white]{result.run_id}[/white]\n"
            f"Artifacts: [cyan]{result.run_root}[/cyan]",
            title="[bold]Run Summary[/bold]",
            border_style=outcome_color,
        )
    )

    metrics_file = result.run_root / "metrics.json"
    baseline_file = result.run_root / "baseline_metrics.json"

    if metrics_file.exists():

        def clean_metrics_dict(d):
            clean = {}
            blacklist = ["created_at", "source", "version", "phase"]
            if "metrics" in d and isinstance(d["metrics"], dict):
                for k, v in d["metrics"].items():
                    if isinstance(v, (int, float)):
                        clean[k] = v
            if "metrics_with_units" in d and isinstance(d["metrics_with_units"], dict):
                for k, v in d["metrics_with_units"].items():
                    if isinstance(v, dict) and "value" in v:
                        val = v["value"]
                        if isinstance(val, (int, float)):
                            clean[k] = val
            for k, v in d.items():
                if k in blacklist or k in ["metrics", "metrics_with_units", "baseline_metrics", "target_files"]:
                    continue
                if isinstance(v, (int, float)) and k not in clean:
                    clean[k] = v
            return clean

        try:
            with open(metrics_file, "r") as f:
                final_metrics_raw = json.load(f)
            baseline_metrics_raw = {}
            if baseline_file.exists():
                with open(baseline_file, "r") as bf:
                    baseline_metrics_raw = json.load(bf)

            final_metrics = clean_metrics_dict(final_metrics_raw)
            baseline_metrics = clean_metrics_dict(baseline_metrics_raw)

            table = Table(title="\n📈 Detailed Performance", box=None)
            table.add_column("Metric", style="cyan")
            table.add_column("Baseline", justify="right", style="magenta")
            table.add_column("Final", justify="right", style="green")
            table.add_column("Progress", justify="right")

            for m_name, final_val in final_metrics.items():
                base_val = baseline_metrics.get(m_name, "N/A")
                progress = ""
                try:
                    f_v = float(final_val)
                    b_v = float(base_val)
                    diff = f_v - b_v
                    color = "green" if diff < 0 else "red"
                    progress = f"[{color}]{diff:+.4f}[/{color}]"
                except Exception:
                    pass
                table.add_row(m_name, str(base_val), str(final_val), progress)

            console.print(table)
        except Exception as e:
            if verbose:
                console.print(f"[dim]Note: Could not parse metrics: {e}[/dim]")

    report_path = result.run_root / "final_report.md"
    patch_path = result.run_root / "final_patch.diff"

    if report_path.exists():
        console.print(
            f"📄 [bold]Report:[/bold] [link=file://{report_path.absolute()}]{report_path.name}[/link]"
        )

    diagram_path = result.run_root / "system_diagram.md"
    if diagram_path.exists():
        console.print(
            f"🗺️  [bold]System Diagram:[/bold] [link=file://{diagram_path.absolute()}]{diagram_path.name}[/link]"
        )

    if patch_path.exists():
        console.print(
            f"🩹 [bold]Clean Patch:[/bold] [link=file://{patch_path.absolute()}]{patch_path.name}[/link]"
        )

    if (result.success or getattr(result, "partial_success", False)) and patch_path.exists():
        console.print("")
        if no_patch:
            should_apply = False
        elif yes:
            should_apply = True
        else:
            should_apply = typer.confirm(
                "Would you like to apply the generated patch to your local repository?",
                default=True,
            )

        if should_apply:
            try:
                is_git = (repo_path / ".git").exists()
                if is_git:
                    subprocess.run(["git", "apply", str(patch_path)], cwd=repo_path, check=True)
                else:
                    subprocess.run(["patch", "-p1", "-i", str(patch_path)], cwd=repo_path, check=True)

                console.print("[bold green]✅ Patch applied successfully![/bold green]")
            except Exception as e:
                console.print(f"[bold red]❌ Failed to apply patch:[/bold red] {e}")

    if result.success:
        raise typer.Exit(code=0)
    if getattr(result, "partial_success", False):
        raise typer.Exit(code=2)
    raise typer.Exit(code=1)
