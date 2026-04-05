"""Post-run Rich output + patch prompt (shared by `remoroo run` and `remoroo attach`)."""
from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

import typer
from rich.console import Console
from rich.panel import Panel
from rich.table import Table

from .run_summary_helpers import (
    apply_patch_to_repo,
    artifact_paths,
    exit_code_for_local_result,
    load_metrics_comparison_rows,
    outcome_rich_border_color,
    rich_progress_cell,
)

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
    outcome_color = outcome_rich_border_color(result)

    console.print("")
    art_line = (
        f"Artifacts: [cyan]{result.run_root}[/cyan]"
        if result.run_root is not None
        else "Artifacts: [dim]— (no run directory)[/dim]"
    )
    console.print(
        Panel(
            f"[bold {outcome_color}]{result.outcome}[/bold {outcome_color}]\n"
            f"Run ID: [white]{result.run_id}[/white]\n"
            f"{art_line}",
            title="[bold]Run Summary[/bold]",
            border_style=outcome_color,
        )
    )

    rows = (
        load_metrics_comparison_rows(result.run_root)
        if result.run_root is not None
        else []
    )
    if rows:
        try:
            table = Table(title="\n📈 Detailed Performance", box=None)
            table.add_column("Metric", style="cyan")
            table.add_column("Baseline", justify="right", style="magenta")
            table.add_column("Final", justify="right", style="green")
            table.add_column("Progress", justify="right")

            for m_name, base_val, final_val, _plain in rows:
                progress = rich_progress_cell(str(base_val), str(final_val))
                table.add_row(m_name, str(base_val), str(final_val), progress)

            console.print(table)
        except Exception as e:
            if verbose:
                console.print(f"[dim]Note: Could not parse metrics: {e}[/dim]")

    paths = (
        artifact_paths(result.run_root) if result.run_root is not None else {}
    )
    if paths.get("report"):
        p = paths["report"]
        console.print(
            f"📄 [bold]Report:[/bold] [link=file://{p.absolute()}]{p.name}[/link]"
        )
    if paths.get("diagram"):
        p = paths["diagram"]
        console.print(
            f"🗺️  [bold]System Diagram:[/bold] [link=file://{p.absolute()}]{p.name}[/link]"
        )
    if paths.get("patch"):
        p = paths["patch"]
        console.print(
            f"🩹 [bold]Clean Patch:[/bold] [link=file://{p.absolute()}]{p.name}[/link]"
        )

    patch_path = paths.get("patch")
    if (
        patch_path
        and (result.success or getattr(result, "partial_success", False))
    ):
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
            ok, err = apply_patch_to_repo(repo_path, patch_path)
            if ok:
                console.print("[bold green]✅ Patch applied successfully![/bold green]")
            else:
                console.print(f"[bold red]❌ Failed to apply patch:[/bold red] {err}")

    raise typer.Exit(code=exit_code_for_local_result(result))
