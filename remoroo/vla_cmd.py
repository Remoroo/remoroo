"""`remoroo vla {start|stop|restart|status|logs}` — manage the VLA policy server.

The server (LingBot-VLA by default) runs in ITS OWN venv, declared once in
`.remoroo/vla.yaml`; the task engine only ever talks HTTP to it. Same managed-service
discipline as `remoroo edge`: the agent restarts it through its ordinary command tool,
the operator by hand, and `remoroo task` starts it opportunistically.
"""
from __future__ import annotations

from pathlib import Path
from typing import Optional

import typer

from . import vla_service

vla_app = typer.Typer(no_args_is_help=True,
                      help="Manage the VLA policy server (runs in its own venv, "
                           "declared in .remoroo/vla.yaml).")


def _project(project: Optional[str]) -> Path:
    return Path(project).expanduser().resolve() if project else Path.cwd().resolve()


@vla_app.command("start")
def start(project: Optional[str] = typer.Option(None, "--project", "-p",
                                                help="Project dir (defaults to cwd).")):
    """Start the policy server (idempotent; a loading server is left to finish loading)."""
    res = vla_service.start(_project(project), echo=typer.echo)
    raise typer.Exit(code=0 if res.get("ok") else 1)


@vla_app.command("stop")
def stop(project: Optional[str] = typer.Option(None, "--project", "-p",
                                               help="Project dir (defaults to cwd).")):
    """Stop the policy server. Stays stopped (motion never depends on it)."""
    vla_service.stop(_project(project), echo=typer.echo)


@vla_app.command("restart")
def restart(project: Optional[str] = typer.Option(None, "--project", "-p",
                                                  help="Project dir (defaults to cwd).")):
    """Stop + start — the way to pick up a new checkpoint or a changed vla.yaml."""
    res = vla_service.restart(_project(project), echo=typer.echo)
    raise typer.Exit(code=0 if res.get("ok") else 1)


@vla_app.command("status")
def status(project: Optional[str] = typer.Option(None, "--project", "-p",
                                                 help="Project dir (defaults to cwd).")):
    """Configured? running? serving? — plus the resolved venv/checkpoint and log tail."""
    st = vla_service.status(_project(project))
    if not st["configured"]:
        typer.echo(f"vla: not configured (write {st['config']} to declare a policy server).")
        return
    state = ("serving" if st["listening"] else
             "loading" if st["loading"] else
             "running (silent)" if st["running"] else "stopped")
    typer.echo(f"vla: {state}  pid={st['pid']}  port={st['port']}  url={st['url']}")
    typer.echo(f"  runtime: {st['runtime']}  python: {st['python']}")
    typer.echo(f"  workdir: {st['workdir']}")
    typer.echo(f"  checkpoint: {st['checkpoint']}")
    typer.echo(f"  log: {st['log']}")
    for ln in st.get("last_log") or []:
        typer.echo(f"  | {ln}")


@vla_app.command("init")
def init(
    python: Optional[str] = typer.Option(None, "--python",
                                         help="Override: the LingBot venv's "
                                              "interpreter (auto-discovered)."),
    workdir: Optional[str] = typer.Option(None, "--workdir",
                                          help="Override: the lingbot-vla-v2 repo "
                                               "checkout (auto-discovered)."),
    checkpoint: Optional[str] = typer.Option(None, "--checkpoint",
                                             help="Weights dir (default: what "
                                                  "`remoroo models install` produced)."),
    qwen: Optional[str] = typer.Option(None, "--qwen",
                                       help="Qwen3-VL-4B-Instruct dir (sets QWEN3VL_PATH "
                                            "for the server + qwen_path for configs)."),
    port: int = typer.Option(8791, "--port"),
    project: Optional[str] = typer.Option(None, "--project", "-p"),
):
    """Write .remoroo/vla.yaml — the once-per-rig declaration everything else reads
    (`remoroo vla start`, `remoroo task` auto-start, models install, vla_apply_profile).
    Idempotent: refuses to overwrite an existing declaration."""
    import yaml

    from . import vla_service

    root = _project(project)
    p = vla_service.config_path(root)
    if p.exists():
        typer.secho(f"{p} already exists — edit it directly (or delete it first).",
                    fg=typer.colors.YELLOW)
        raise typer.Exit(code=1)
    # ZERO-FLAG PATH: ground-truth discovery (probe the ACTIVE venv + every python
    # findable for the runtime's package — same-venv installs are first-class), then
    # ASK for anything still missing, verifying every answer before accepting it.
    import sys as _sys

    def _ask(question: str, default: str) -> str:
        if not _sys.stdin.isatty():
            return ""                              # non-interactive: derivations only
        return typer.prompt(question, default=default or None)

    cfg = vla_service.wizard(root, ask=_ask, echo=typer.echo) or {}
    if not cfg.get("python") and not (python and workdir):
        typer.secho("could not find OR get a working answer for the VLA python "
                    "(nothing on this machine imports the runtime's package). Install "
                    "the VLA (any venv, including the edge/curobo one), then rerun.",
                    fg=typer.colors.RED)
        raise typer.Exit(code=2)
    cfg.setdefault("runtime", "lingbot")
    cfg.setdefault("extra_args", [])
    if python:
        cfg["python"] = python
    if workdir:
        cfg["workdir"] = workdir
    cfg["port"] = port
    if checkpoint:
        cfg["checkpoint"] = checkpoint
    if qwen:
        cfg["qwen_path"] = qwen
        cfg.setdefault("env", {})["QWEN3VL_PATH"] = qwen
    typer.secho(f"  python:     {cfg.get('python')}", fg=typer.colors.CYAN)
    typer.secho(f"  workdir:    {cfg.get('workdir')}", fg=typer.colors.CYAN)
    typer.secho(f"  checkpoint: {cfg.get('checkpoint', '(models install)')}",
                fg=typer.colors.CYAN)
    typer.secho(f"  qwen:       {cfg.get('qwen_path', '(unset)')}", fg=typer.colors.CYAN)
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(yaml.safe_dump(cfg, sort_keys=False), encoding="utf-8")
    typer.secho(f"✓ wrote {p}", fg=typer.colors.GREEN)
    typer.echo("  next: `remoroo models install` (weights) → `remoroo vla start`.")


@vla_app.command("dataset")
def dataset(
    task_slug: str = typer.Argument(..., help="Task slug whose episodes to ship."),
    dir: Optional[str] = typer.Option(None, "--dir",
                                      help="Episode dir (default "
                                           ".remoroo/task/export/<slug>)."),
    project: Optional[str] = typer.Option(None, "--project", "-p"),
):
    """Tar the task's exported LeRobot episodes and ship them to the control plane —
    the finetune job on the GPU box fetches them from there (the rig and that box
    never talk directly). Prints the dataset_url the agent passes to the finetune."""
    import io
    import tarfile

    import requests

    from .auth import ensure_logged_in
    from .configs import get_api_url

    root = _project(project)
    src = Path(dir) if dir else root / ".remoroo" / "task" / "export" / task_slug
    if not src.is_dir() or not any(src.iterdir()):
        typer.secho(f"no episodes at {src} — export them first (export_lerobot / the "
                    "agent's export step).", fg=typer.colors.RED)
        raise typer.Exit(code=2)
    client = ensure_logged_in()
    buf = io.BytesIO()
    with tarfile.open(fileobj=buf, mode="w:gz") as tf:
        tf.add(src, arcname="dataset")
    data = buf.getvalue()
    typer.secho(f"uploading {len(data) / 1e6:.1f} MB of episodes…", fg=typer.colors.CYAN)
    r = requests.post(f"{get_api_url().rstrip('/')}/robotics/vla/datasets", data=data,
                      headers={"Authorization": f"Bearer {client.get_token() or ''}",
                               "Content-Type": "application/gzip"}, timeout=600)
    r.raise_for_status()
    out = r.json()
    typer.secho(f"✓ dataset_url: {out['dataset_url']}", fg=typer.colors.GREEN)


@vla_app.command("pull")
def pull(
    job_id: str = typer.Argument(..., help="The finetune job id (from the cockpit "
                                           "or the agent)."),
    project: Optional[str] = typer.Option(None, "--project", "-p"),
):
    """Pull a finished finetune's checkpoint down to this rig (CP proxies the GPU box),
    unpack it under .remoroo/task/weights/, and say how to serve it."""
    import tarfile

    import requests

    from .auth import ensure_logged_in
    from .configs import get_api_url

    root = _project(project)
    client = ensure_logged_in()
    dest = root / ".remoroo" / "task" / "weights" / f"finetuned_{job_id}"
    dest.mkdir(parents=True, exist_ok=True)
    tar_path = dest / "checkpoint.tar.gz"
    with requests.get(f"{get_api_url().rstrip('/')}/robotics/vla/finetune/{job_id}"
                      "/artifact",
                      headers={"Authorization": f"Bearer {client.get_token() or ''}"},
                      stream=True, timeout=3600) as r:
        r.raise_for_status()
        with open(tar_path, "wb") as f:
            for chunk in r.iter_content(1 << 20):
                f.write(chunk)
    with tarfile.open(tar_path) as tf:
        tf.extractall(dest, filter="data")
    (dest / "REVISION").write_text(f"finetune:{job_id}", encoding="utf-8")
    typer.secho(f"✓ checkpoint at {dest}", fg=typer.colors.GREEN)
    typer.echo(f"  serve it: set `checkpoint: {dest / 'checkpoint'}` in "
               ".remoroo/vla.yaml, then `remoroo vla restart`.")


@vla_app.command("logs")
def logs(
    project: Optional[str] = typer.Option(None, "--project", "-p",
                                          help="Project dir (defaults to cwd)."),
    tail: int = typer.Option(0, "--tail", "-n",
                             help="Last N lines (0 = the FULL log — the default)."),
):
    """Print the policy server log (load progress, CUDA errors, request tracebacks)."""
    if tail > 0:
        lines = vla_service.tail(_project(project), n=tail)
        for ln in lines or ["(vla log is empty or not present yet)"]:
            typer.echo(ln)
        return
    txt = vla_service.read_log(_project(project))
    typer.echo(txt if txt else "(vla log is empty or not present yet)")


@vla_app.command("apply-profile")
def apply_profile(
    project: Optional[str] = typer.Option(None, "--project", "-p",
                                          help="Project dir (defaults to cwd)."),
):
    """Generate the vendor's OWN config files (robot config + cli yaml) from the
    authored remoroo_cell/vla_profile.yaml — standalone, no task run and no edge
    needed. Restart the server after (`remoroo vla restart`) so it loads them."""
    import json

    import yaml

    proj = _project(project)
    profile_path = proj / "remoroo_cell" / "vla_profile.yaml"
    if not profile_path.exists():
        typer.secho("❌ no remoroo_cell/vla_profile.yaml — author the embodiment "
                    "wiring first (cameras/state/actions).", fg=typer.colors.RED)
        raise typer.Exit(code=1)
    cfg = vla_service.load_config(proj)
    if not cfg or not cfg.get("workdir") or not cfg.get("checkpoint"):
        typer.secho("❌ .remoroo/vla.yaml needs workdir + checkpoint (declare the "
                    "server first: `remoroo vla init`).", fg=typer.colors.RED)
        raise typer.Exit(code=1)

    # the generator ships inside the bundled engine — one implementation, no drift
    import sys
    studio = Path(__file__).parent / "_studio"
    if str(studio) not in sys.path:
        sys.path.insert(0, str(studio))
    from task_engine.vla.profile import load_profile, write_lingbot_configs

    profile = load_profile(str(profile_path))
    config_path = None
    commission = proj / "remoroo_cell" / "vla" / "commission.json"
    if commission.exists():
        try:
            config_path = json.loads(commission.read_text()).get("config_path")
        except Exception:                      # noqa: BLE001
            pass
    checkpoint = cfg["checkpoint"]
    out = write_lingbot_configs(
        profile, workdir=cfg["workdir"], checkpoint=checkpoint,
        config_path=config_path,
        qwen_path=cfg.get("qwen_path") or str(Path(checkpoint).parent
                                              / "Qwen3-VL-4B-Instruct"),
        norm_stats_path=str(proj / ".remoroo" / "task" / "vla" / "norm_stats.json"))
    typer.secho(f"✓ robot config: {out['robot_config_path']}", fg=typer.colors.GREEN)
    typer.secho(f"✓ cli config:   {out['cli_config_path']}", fg=typer.colors.GREEN)
    typer.echo("  now: `remoroo vla restart` (the server reads these at reset).")


def _studio_path() -> None:
    import sys
    studio = Path(__file__).parent / "_studio"
    if str(studio) not in sys.path:
        sys.path.insert(0, str(studio))


@vla_app.command("prove")
def prove(
    project: Optional[str] = typer.Option(None, "--project", "-p",
                                          help="Project dir (defaults to cwd)."),
):
    """END-TO-END wire proof, standalone: pack a synthetic observation in the
    profile's exact schema, run ONE real inference round-trip against the serving
    policy server, report latency + action shape. No task run, no edge, no robot —
    if this passes, the whole config+wire chain is closed."""
    proj = _project(project)
    cfg = vla_service.load_config(proj)
    if not cfg:
        typer.secho("❌ no .remoroo/vla.yaml — `remoroo vla init` first.",
                    fg=typer.colors.RED)
        raise typer.Exit(code=1)
    _studio_path()
    import numpy as np
    from task_engine.vla.profile import load_profile
    from task_engine.vla.runtime import LingBotRuntime

    profile = load_profile(str(proj / "remoroo_cell" / "vla_profile.yaml"))
    rt = LingBotRuntime(server_url=f"http://127.0.0.1:{cfg.get('port', 8791)}",
                        profile=profile)
    obs = None
    if profile is not None:                      # full inference: their exact obs schema
        obs = {"observation.state": np.zeros(profile.state_dim, dtype=np.float32),
               "robo_name": profile.robo_name,
               "prompt": ["remoroo wire proof"]}
        for slot in profile.cameras:
            obs[f"observation.images.{slot}"] = np.zeros((256, 256, 3), dtype=np.uint8)
    proof = rt.prove(obs)
    typer.echo(f"  proof: {proof}")
    if proof.get("proven") == "inference":
        typer.secho(f"✓ WIRE PROVEN — {proof.get('latency_ms')}ms, "
                    f"{proof.get('action_rows')} action rows × "
                    f"{proof.get('action_dim')} dims.", fg=typer.colors.GREEN)
        raise typer.Exit(code=0)
    if proof.get("proven") == "handshake":
        typer.secho("◐ handshake only (no vla_profile.yaml — author it for full "
                    "inference proof).", fg=typer.colors.YELLOW)
        raise typer.Exit(code=0)
    typer.secho("❌ proof failed — the note above says what to fix.",
                fg=typer.colors.RED)
    raise typer.Exit(code=1)


def _edge_post(verb: str, body: dict, port: int = 7779, timeout: float = 30.0) -> dict:
    import json as _json
    import urllib.request
    req = urllib.request.Request(f"http://127.0.0.1:{port}/edge/task/{verb}",
                                 data=_json.dumps(body).encode(),
                                 headers={"Content-Type": "application/json"})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return _json.loads(r.read().decode())


@vla_app.command("rollout")
def rollout(
    instruction: str = typer.Argument(..., help="The task text the policy attempts."),
    steps: int = typer.Option(30, "--steps", help="Max policy steps (guarded)."),
    slug: str = typer.Option("vla_smoke", "--slug", help="Trial store slug."),
    port: int = typer.Option(7779, "--port", help="Edge port."),
):
    """One guarded VLA episode ON THE ROBOT, standalone: needs only a running edge
    (`remoroo edge start`) and a serving policy server. Every action goes through the
    GuardedExecutor clamps + safety envelope; the trial is recorded like any day-zero
    attempt. No `remoroo task`, no agent."""
    import time as _time

    # STEP 1: wait for the edge with a CHEAP verb — never by re-firing vla_load
    # (each vla_load runs a ~21s inference; 20s retries STACKED inferences behind the
    # vendor's event-loop-blocking handler until handshakes failed — rig 2026-07-15).
    ready = False
    for attempt in range(24):                  # ~4 min of VISIBLE waiting, never silent
        try:
            _edge_post("status", {}, port=port, timeout=10.0)
            ready = True
            break
        except Exception:                      # noqa: BLE001 - edge still warming
            typer.echo("  … waiting for the edge (cuRobo warmup on a fresh start "
                       f"takes ~2-3 min) [{(attempt + 1) * 10}s]")
    if not ready:
        typer.secho("❌ the edge never answered — `tail -f .remoroo/edge.log` to "
                    "see where it is.", fg=typer.colors.RED)
        raise typer.Exit(code=1)
    # STEP 2: ONE proof, generous timeout (a full inference on this board is ~21s)
    typer.echo("  proving the wire (one full inference, ~20-30s on this board)…")
    try:
        out = _edge_post("vla_load", {"runtime": "lingbot"}, port=port, timeout=150.0)
    except Exception as e:                     # noqa: BLE001
        typer.secho(f"❌ wire proof did not return ({type(e).__name__}) — check "
                    "`remoroo vla logs`; do NOT re-run in a loop (each attempt costs "
                    "a full inference).", fg=typer.colors.RED)
        raise typer.Exit(code=1)
    if "error" in out:
        typer.secho(f"❌ vla_load: {out['error']}", fg=typer.colors.RED)
        raise typer.Exit(code=1)
    typer.secho(f"✓ wire proven: {out.get('proof', {}).get('latency_ms')}ms",
                fg=typer.colors.GREEN)
    try:
        out = _edge_post("vla_rollout", {"task_slug": slug, "instruction": instruction,
                                         "max_steps": steps, "skip_reset": True},
                         port=port, timeout=60.0)
        job = out.get("job")
        while out.get("state") == "running":     # motion runs as a job: poll, never re-issue
            typer.echo(f"  … rolling out (job {job})")
            _time.sleep(10)
            out = _edge_post("job_status", {"job": job}, port=port)
            out = out.get("result") or out
    except Exception as e:                     # noqa: BLE001
        typer.secho(f"❌ edge stopped answering mid-rollout ({type(e).__name__}) — "
                    "the job keeps running on the edge; check `job_status` via the "
                    "edge log rather than re-issuing.", fg=typer.colors.RED)
        raise typer.Exit(code=1)
    if "error" in out:
        typer.secho(f"❌ rollout: {out['error']}", fg=typer.colors.RED)
        raise typer.Exit(code=1)
    ok_outcome = out.get("outcome") not in ("actor_exception", "envelope_violation")
    typer.secho(f"{'✓' if ok_outcome else '✗'} rollout done — trial "
                f"{out.get('trial_id')} outcome={out.get('outcome')}",
                fg=typer.colors.GREEN if ok_outcome else typer.colors.RED)
    for flag in out.get("anomalies") or []:    # the actor's own traceback tail
        typer.echo(f"    {flag}")
    typer.echo("  judge it with your eyes: the before/after frames are in "
               ".remoroo/task/frames/ (scene_snapshot), verdict flags say day0.")
