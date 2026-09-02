"""TaskService (COMP-32) — the JSON-verb dispatch behind /edge/task/<verb>, mirroring the
calib_engine service pattern: edge_real routes here; this class is CI-tested off-robot. Verbs
that touch the shared bridge are declared in BRIDGE_VERBS so edge_real serializes them under
its bridge lock (the proven calib discipline).

The live trial verb runs the AUTHORED cell task package: remoroo_cell.task_<slug> must expose
build_env(shared) -> GenericEnv and actor(env, ctx). Off-robot (no cell package) the verb
returns a stated error, never a crash.
"""
from __future__ import annotations

import importlib
import inspect
import json
import itertools
import sys
import threading
import time
from pathlib import Path
from typing import Any, Callable, Dict, Optional

from .params import Knobs
from .record import TrialStore
from .run_trial import TrialBudget, run_trial

BRIDGE_VERBS = {"trial_run", "vla_rollout", "scene_snapshot"}
# edge_real wraps these in its _bridge_lock. The long ones ALSO run as background jobs
# (_run_as_job): the HTTP reply returns fast while the service's one-motion-at-a-time
# lock guards the hardware for the job's whole life.


class TaskService:
    def __init__(self, *, data_dir: str = ".remoroo/task",
                 env_builder: Optional[Callable[[str], Any]] = None,
                 bridge: Optional[Any] = None,
                 stack_provider: Optional[Callable[[], Any]] = None) -> None:
        self.data_dir = Path(data_dir)
        self.data_dir.mkdir(parents=True, exist_ok=True)
        self._bridge = bridge
        self._stack_provider = stack_provider
        self._env_builder = env_builder or self._default_env_builder
        self._stores: Dict[str, TrialStore] = {}
        self._jobs: Dict[str, dict] = {}
        self._jobs_seq = itertools.count(1)
        self._motion_lock = threading.Lock()   # ONE physical thing at a time

    # ---- plumbing --------------------------------------------------------------------
    def _store(self, task_slug: str) -> TrialStore:
        if task_slug not in self._stores:
            self._stores[task_slug] = TrialStore(str(self.data_dir / "trials" / task_slug))
        return self._stores[task_slug]

    def _cell(self) -> Dict[str, Any]:
        """The parsed cell.yaml — THE contract (audit 2026-07-21: it was parsed in two
        stray corners and consumed by neither the authored package nor the envs; the
        b12f5aa1 gripper inversion lived exactly in that gap)."""
        try:
            import yaml
            return yaml.safe_load(Path("remoroo_cell/cell.yaml").read_text()) or {}
        except Exception:                              # noqa: BLE001 - off-robot: no cell
            return {}

    def _shared(self) -> Dict[str, Any]:
        """THE handoff to the authored cell package: build_env(shared) receives the LIVE
        surfaces here — the Bridge and the MotionStack — plus the task data dir, the
        parsed cell.yaml, and the cell's embodiment resolvers (up_of/approach_of/
        effector_of from groups[]). This is the contract; the package never reaches
        into the edge for accessors."""
        stack = None
        if self._stack_provider is not None:
            stack = self._stack_provider()             # may build/warm: that's the point
        cell = self._cell()
        from .env import resolvers_from_cell
        up_of, approach_of, effector_of = resolvers_from_cell(cell)
        return {"bridge": self._bridge, "stack": stack,
                "data_dir": str(self.data_dir),
                "calib": self._calib_surface(),
                "cell": cell,
                "resolvers": {"up_of": up_of, "approach_of": approach_of,
                              "effector_of": effector_of}}

    def _calib_surface(self) -> Dict[str, Any]:
        """The cell's calibration, resolved by the ENGINE (audit 2026-07-13: the agent
        burned ~10 turns reverse-engineering calibration/*.json + the URDF bake).
        Per camera: the saved solve (K/T_optical/whatever accept wrote) plus the baked
        optical FRAME name — live robot_from_cam is stack.link_pose(frame), which is
        correct for wrist AND static cameras once bake_calibration ran."""
        import json as _json
        out: Dict[str, Any] = {}
        root = Path("remoroo_cell/calibration")
        if not root.exists():
            return out
        for f in sorted(root.glob("*.json")):
            try:
                d = _json.loads(f.read_text(encoding="utf-8"))
            except Exception:                          # noqa: BLE001 - skip non-solves
                continue
            if isinstance(d, dict):
                d.setdefault("optical_frame", f"{f.stem}_optical_frame")
                out[f.stem] = d
        if out:
            out["_note"] = ("robot_from_cam(camera) = stack.link_pose("
                            "calib[camera]['optical_frame']); K/T_optical are the "
                            "saved solve — never re-derive these from raw files")
        return out

    def _default_env_builder(self, task_slug: str) -> Any:
        # HOT RELOAD: every trial imports the authored package FRESH, so the agent's
        # edit -> task_trial cycle needs NO edge restart (a restart also costs the
        # cuRobo warmup). Only the tiny task package reloads; the Bridge
        # (primitives.py) holds live device handles and still needs a real restart.
        pkg = f"remoroo_cell.task_{task_slug}"
        for name in [m for m in list(sys.modules)
                     if m == pkg or m.startswith(pkg + ".")]:
            del sys.modules[name]
        # The package is AUTHORED mid-session (after this process started): without this,
        # the FileFinder's directory cache can miss brand-new files entirely.
        importlib.invalidate_caches()
        mod = importlib.import_module(pkg)
        build = mod.build_env
        # build_env(shared) is the contract; a zero-arg build_env stays supported.
        takes_shared = any(
            p.kind in (p.POSITIONAL_ONLY, p.POSITIONAL_OR_KEYWORD)
            for p in inspect.signature(build).parameters.values())
        env = build(self._shared()) if takes_shared else build()
        self._wire_cell_resolvers(env)
        return env, getattr(mod, "actor")

    def _wire_cell_resolvers(self, env: Any) -> None:
        """Inject the cell's embodiment resolvers into a GenericEnv whose authored
        build_env left the class defaults (audit 2026-07-21: resolvers_from_cell had
        ZERO callers — the effector declaration never reached atoms, and the b12f5aa1
        gripper inversion rode exactly that gap). An authored env that SET its own
        resolvers is respected; only untouched defaults are replaced."""
        from .env import GenericEnv, resolvers_from_cell
        try:
            fields = GenericEnv.__dataclass_fields__
            defaults = {k: fields[k].default
                        for k in ("up_of", "approach_of", "effector_of")}
        except Exception:                              # noqa: BLE001 - not a GenericEnv
            return
        cell = self._cell()
        if not cell:
            return
        up_of, approach_of, effector_of = resolvers_from_cell(cell)
        wired = {"up_of": up_of, "approach_of": approach_of,
                 "effector_of": effector_of}
        for name, default in defaults.items():
            if getattr(env, name, None) is default:
                setattr(env, name, wired[name])

    def _preflight_env(self, env: Any, actor: Optional[Callable], *,
                       slug: str) -> Optional[str]:
        """Validate the authored package BEFORE any hardware motion. Returns a NAMED
        refusal (str) or None. Audit 2026-07-21: engine validation used to be `import
        succeeds + mod.actor exists` — run b12f5aa1 executed 279s of real motion, then
        judge_fn (None, wrong import in the authored __init__) raised post-motion and
        the evidence was lost. Every check here costs zero motion."""
        if actor is not None and not callable(actor):
            return (f"preflight {slug}: actor is not callable "
                    f"({type(actor).__name__}) — fix the package's actor")
        fn = getattr(env, "perceive_fn", None)
        if not callable(fn):
            return (f"preflight {slug}: env.perceive_fn is "
                    f"{'None' if fn is None else 'not callable'} — build_env must "
                    f"wire it; nothing moves until this is fixed")
        # judge_fn is OPTIONAL (robotics_program 2026-07-22): None = no authored
        # judge, success is decided from images by the program loop. If one IS
        # wired, it must be callable and must survive the dry-run below.
        jf = getattr(env, "judge_fn", None)
        if jf is not None and not callable(jf):
            return f"preflight {slug}: env.judge_fn is set but not callable"
        rf = getattr(env, "reset_fn", None)
        if rf is not None and not callable(rf):
            return f"preflight {slug}: env.reset_fn is set but not callable"
        if getattr(env, "bridge", None) is None:
            return (f"preflight {slug}: env.bridge is None — no hardware surface; "
                    f"build_env(shared) must keep shared['bridge']")
        if getattr(env, "stack", None) is None:
            return (f"preflight {slug}: env.stack is None — no motion surface; "
                    f"build_env(shared) must keep shared['stack']")
        # JUDGE DRY-RUN (zero motion): observe once, judge(pre, pre, empty trace).
        # A judge that cannot even run on identical scenes will certainly crash after
        # the robot moved — refuse NOW instead.
        try:
            pre = env.observe()
        except Exception as e:                         # noqa: BLE001
            return (f"preflight {slug}: observe() raised before any motion: "
                    f"{type(e).__name__}: {e} — fix perception_task first")
        try:
            from .trace import Trace
            env.judge(pre, pre, Trace())
        except Exception as e:                         # noqa: BLE001
            return (f"preflight {slug}: judge dry-run judge(pre, pre) raised: "
                    f"{type(e).__name__}: {e} — fix perception_judge before any "
                    f"hardware motion")
        return None


    # ---- dispatch --------------------------------------------------------------------
    def handle(self, verb: str, body: Dict[str, Any]) -> Dict[str, Any]:
        try:
            fn = getattr(self, f"v_{verb}", None)
            if fn is None:
                return {"error": f"unknown task verb {verb!r}"}
            return fn(body or {})
        except Exception as e:                     # noqa: BLE001 - stated, never a dead socket
            return {"error": f"{type(e).__name__}: {e}"}

    # ---- background jobs (live lesson 2026-07-13) -------------------------------------
    def _run_as_job(self, kind: str, fn: Callable[[], dict], *, wait_s: float = 5.0) -> dict:
        """Run a long verb in a worker thread and NEVER let HTTP time out on it. The live
        probe outlasted the 600s window: the client said "edge unreachable" while the robot
        was still moving, and the agent re-issued the verb — DUPLICATE MOTION. Fast results
        return synchronously (within wait_s); slow ones return {"job": id} to poll with
        job_status. Every caller here touches the bridge, so the one-motion-at-a-time lock
        is taken for the job's whole life; a second verb REFUSES with the running job's id
        instead of queueing silently behind an invisible lock."""
        if not self._motion_lock.acquire(blocking=False):
            running = [j for j, r in self._jobs.items() if r["state"] == "running"]
            return {"error": (f"job {', '.join(running) or '?'} is still RUNNING on the "
                              "robot — poll job_status until it finishes; NEVER re-issue "
                              "a motion verb (the robot would move again)")}
        job_id = f"{kind}_{next(self._jobs_seq):04d}"
        rec: Dict[str, Any] = {"kind": kind, "state": "running", "t0": time.time()}
        self._jobs[job_id] = rec
        # DISK MARKER (2026-07-18, the mid-run edge death): a running job must survive the
        # process in STATED form — an edge restart wiped the registry and the agent read
        # "unknown job" as a typo instead of "your robot died MID-MOTION", then proceeded
        # to sibling work with the arm parked over the object.
        try:
            jd = self.data_dir / "jobs"
            jd.mkdir(parents=True, exist_ok=True)
            (jd / f"{job_id}.json").write_text(json.dumps(
                {"kind": kind, "state": "running", "t0": rec["t0"]}), encoding="utf-8")
        except Exception:  # noqa: BLE001 — the marker is best-effort
            pass
        finished = threading.Event()

        def work():
            try:
                rec["result"] = fn()
            except Exception as e:                 # noqa: BLE001 - stated, never a dead thread
                rec["result"] = {"error": f"{type(e).__name__}: {e}"}
            finally:
                rec["t1"] = time.time()
                try:
                    (self.data_dir / "jobs" / f"{job_id}.json").unlink(missing_ok=True)
                except Exception:  # noqa: BLE001
                    pass
                rec["state"] = "done"
                self._motion_lock.release()
                finished.set()

        threading.Thread(target=work, name=f"task-job-{job_id}", daemon=True).start()
        if finished.wait(wait_s):
            out = dict(rec.get("result") or {})
            out.setdefault("job", job_id)
            return out
        return {"job": job_id, "state": "running",
                "note": (f"{kind} is RUNNING on the robot in the background — poll "
                         f"job_status with {{'job': '{job_id}'}} every 10-15s until "
                         "state=done, then read its result. NEVER re-issue the verb "
                         "(it would run the motion AGAIN); progress: .remoroo/edge.log")}

    def v_job_status(self, body: dict) -> dict:
        job = body.get("job")
        if not job:
            recent = list(self._jobs.items())[-5:]
            return {"jobs": [{"job": j, "kind": r["kind"], "state": r["state"]}
                             for j, r in recent]}
        rec = self._jobs.get(job)
        if rec is None:
            grave = self.data_dir / "jobs" / f"{job}.json"
            if grave.exists():
                return {"job": job, "state": "died_with_edge",
                        "error": (f"job {job!r} DIED WITH AN EDGE RESTART while possibly "
                                  "MID-MOTION — the robot may be parked ANYWHERE (occluding "
                                  "cameras, mid-scene). STOP phase work NOW. RECOVER FIRST: "
                                  "run the goto_home verb, then re-observe (scene_snapshot) "
                                  "and re-check the record before continuing; results from "
                                  "before the death are suspect.")}
            return {"error": f"unknown job {job!r} — never issued on this edge process"}
        out = {"job": job, "kind": rec["kind"], "state": rec["state"],
               "elapsed_s": round((rec.get("t1") or time.time()) - rec["t0"], 1)}
        if rec["state"] == "done":
            out["result"] = rec.get("result")
        else:
            out["note"] = ("still running — the robot/models are working; poll again in "
                           "10-15s, do NOT re-issue the original verb")
        return out

    # ---- frames on disk: the agent's own eyes (live lesson 2026-07-13) ----------------
    def _save_frame(self, frame: Dict[str, Any], tag: str) -> Dict[str, str]:
        """Write a captured frame's rgb (+ a bright=near depth visualization) as PNGs the
        agent can view_image. One real look beat eight blind record round-trips live."""
        import numpy as np
        out_dir = self.data_dir / "frames"
        out_dir.mkdir(parents=True, exist_ok=True)
        paths: Dict[str, str] = {}

        def _write(name: str, img) -> Optional[str]:
            fp = out_dir / f"{tag}_{name}.png"
            try:
                import cv2
                ok = cv2.imwrite(str(fp), img if name != "rgb" else img[..., ::-1])
                return str(fp) if ok else None
            except Exception:                      # noqa: BLE001 - PIL fallback
                try:
                    from PIL import Image
                    Image.fromarray(img).save(str(fp))
                    return str(fp)
                except Exception:                  # noqa: BLE001 - stated absence
                    return None

        rgb = frame.get("rgb")
        if rgb is not None:
            path = _write("rgb", np.asarray(rgb).astype(np.uint8))
            if path:
                paths["rgb"] = path
        depth = frame.get("depth")
        if depth is not None:
            d = np.asarray(depth, dtype=float)
            finite = np.isfinite(d) & (d > 0)
            if finite.any():
                lo, hi = np.percentile(d[finite], [2, 98])
                viz = np.zeros_like(d)
                viz[finite] = 255.0 * (1.0 - np.clip((d[finite] - lo) / max(hi - lo, 1e-6), 0, 1))
                path = _write("depth", viz.astype(np.uint8))
                if path:
                    paths["depth"] = path
        return paths

    def v_scene_snapshot(self, body: dict) -> dict:
        """SEE the scene BEFORE authoring perception: grab the named camera(s), save RGB +
        depth PNGs, return the paths — view_image them NOW. Needs only the Bridge (works
        before any task package exists). Bridge-locked like every capture."""
        bridge = self._bridge
        if bridge is None:
            return {"error": "no bridge wired; snapshots need the cell"}

        requested = body.get("cameras")
        if isinstance(requested, str):
            if requested.strip().lower() == "all":
                configured = self._cell().get("cameras") or []
                if isinstance(configured, dict):
                    names = configured.keys()
                else:
                    names = (
                        (camera.get("name") or camera.get("id"))
                        if isinstance(camera, dict) else camera
                        for camera in configured
                    )
                # Resolve the contract once, before the background job, so a live edit
                # to cell.yaml cannot change which cameras belong to this snapshot.
                cameras = tuple(sorted({
                    str(name).strip() for name in names if name is not None and str(name).strip()
                }))
            else:
                cameras = (requested,)
        elif requested:
            cameras = tuple(requested)
        elif body.get("camera"):
            cameras = (body["camera"],)
        else:
            cameras = ()
        if not cameras:
            return {"error": "name the camera(s): {'camera': 'overhead'} or "
                             "{'cameras': [...]} / {'cameras': 'all'} "
                             "(cell.yaml lists them)"}

        def run() -> dict:
            from .perceive.operators import PerceptionOps
            ops = PerceptionOps(None, None, bridge)
            frames = []
            for cam in cameras:
                stamp = time.strftime("%H%M%S")
                paths = self._save_frame(ops.capture(cam), f"{cam}_{stamp}")
                frames.append({"camera": cam, **paths})
            return {"frames": frames,
                    "note": ("view_image these NOW and author perception from what you "
                             "actually SAW — one real look beats blind debugging")}

        return self._run_as_job("snapshot", run, wait_s=30.0)

    # ---- verbs -----------------------------------------------------------------------
    @staticmethod
    def _build_stamp() -> str:
        """The bundle's build stamp (written by bundle_studio.py) — lets the CLI catch
        a stale edge PROCESS executing old bytecode (cost two live cycles)."""
        try:
            return (Path(__file__).resolve().parents[1]
                    / "BUILD_STAMP").read_text().strip()
        except Exception:                          # noqa: BLE001 - dev tree has no stamp
            return ""

    def v_status(self, body: dict) -> dict:
        return {
            "service": "task", "data_dir": str(self.data_dir),
            "build_stamp": self._build_stamp(),
        }








    def v_trial_ids(self, body: dict) -> dict:
        return {"trial_ids": self._store(body["task_slug"]).all_ids()}

    def v_trial_get(self, body: dict) -> dict:
        rec = self._store(body["task_slug"]).load(body["trial_id"])
        return {"record": rec.to_dict()}

    def v_trial_run(self, body: dict) -> dict:
        """Run one trial of the AUTHORED task package on this cell (the M0 smoke path)."""
        if self._motion_lock.locked():
            running = [j for j, r in self._jobs.items() if r["state"] == "running"]
            return {"error": (f"job {', '.join(running) or '?'} is still RUNNING on the "
                              "robot — poll job_status; hardware takes one thing at a time")}
        slug = body["task_slug"]
        self._ensure_home(slug)                    # the run STARTS at home (no ceremony)
        refusal = self._verify_sim_floor(slug, body)
        if refusal:
            return {"error": refusal}
        try:
            env, actor = self._env_builder(slug)
        except Exception as e:                     # noqa: BLE001
            return {"error": (f"cell task package not available for {slug!r}: "
                              f"{type(e).__name__}: {e}")}
        refusal = self._preflight_env(env, actor, slug=slug)
        if refusal:
            return {"error": refusal}
        rec = run_trial(env, actor, task_slug=slug,
                        program_ref=body.get("program_ref", f"{slug}@cell"),
                        knobs=Knobs(body.get("knobs") or {}),
                        seed=int(body.get("seed", 0)),
                        budget=TrialBudget(**(body.get("budget") or {})),
                        store=self._store(slug),
                        skip_reset=bool(body.get("skip_reset", False)))
        return {"trial_id": rec.trial_id, "outcome": rec.outcome, "verdict": rec.verdict,
                "knobs": rec.knobs}

    def _ensure_home(self, slug: str) -> None:
        """THE RUN STARTS AT HOME (robotics_program 2026-07-22): the first motion verb
        stamps HOME from wherever the operator parked the robot — no phase_start
        ceremony. goto_home returns to this pose; an already-stamped home is kept."""
        led = self._ledger(slug)
        if (getattr(led, "run_home", None) or {}).get("joints"):
            return
        stack = self._stack_provider() if self._stack_provider else None
        reader = (getattr(stack, "joint_positions", None)
                  or getattr(self._bridge, "joint_state", None)
                  or getattr(stack, "_seed_positions", None))
        joints = dict(reader()) if callable(reader) else {}
        if joints:
            led.set_run_home(joints)

    def _verify_sim_floor(self, slug: str, body: dict) -> Optional[str]:
        """ENGINE-ENFORCED N (robotics_program): real execution requires a
        WORKER-VERIFIED practice report with >= N episodes. The engine fetches the
        report from the sibling worker itself — agent claims are never evidence.
        Enforced only where a worker is configured (a cell without one cannot
        practice at all — the run terminates upstream); floor<=0 disables (dev)."""
        import os as _os
        import urllib.request as _rq
        floor = int(_os.environ.get("REMOROO_SIM_TRIALS_FLOOR", "1000"))
        if floor <= 0:
            return None
        url = (_os.environ.get("REMOROO_SIBLING_URL", "") or "").rstrip("/")
        if not url:
            cfg = self.data_dir.parent / "sibling.json"
            if cfg.exists():
                try:
                    url = (json.loads(cfg.read_text(encoding="utf-8"))
                           .get("url") or "").rstrip("/")
                except Exception:                       # noqa: BLE001
                    url = ""
        if not url:
            return None
        ev_dir = self.data_dir / "sim_evidence"
        ev_path = ev_dir / f"{slug}.json"
        job = str(body.get("sim_job") or "")
        if not job and ev_path.exists():
            try:
                cached = json.loads(ev_path.read_text(encoding="utf-8"))
            except Exception:                           # noqa: BLE001
                cached = {}
            if int(cached.get("episodes", 0)) >= floor:
                return None
        if not job:
            return (f"real execution refused: no verified sim practice for {slug!r}. "
                    f"Run a task_sibling evolve campaign and pass its job id as "
                    f"sim_job — the engine requires >= {floor} episodes "
                    f"(engine-enforced N, robotics_program; set "
                    f"REMOROO_SIM_TRIALS_FLOOR to tune)")
        try:
            with _rq.urlopen(f"{url}/fetch?job_id={job}", timeout=20) as r:
                report = (json.loads(r.read().decode()).get("results")) or {}
        except Exception as e:                          # noqa: BLE001
            return (f"real execution refused: could not verify sim job {job!r} "
                    f"against the worker ({type(e).__name__}: {e}) — a report the "
                    f"engine cannot fetch is not evidence")
        episodes = int(report.get("episodes") or 0)
        if episodes < floor:
            return (f"real execution refused: sim job {job!r} ran {episodes} "
                    f"episodes < the N floor ({floor}). Keep evolving "
                    f"(more generations/seeds/scenes) before touching the robot")
        ev_dir.mkdir(parents=True, exist_ok=True)
        ev_path.write_text(json.dumps(
            {"job_id": job, "episodes": episodes,
             "n_scenes": report.get("n_scenes"),
             "success_rate_best": report.get("success_rate_best"),
             "verified_at": time.time()}, indent=1), encoding="utf-8")
        return None


    # ---- scene record + phase spine + ritual (Task Engine v2, M0) ----------------------

    def _ledger(self, task_slug: str):
        from .spine import PhaseLedger
        return PhaseLedger.load(str(self.data_dir / "state" / f"{task_slug}.json"),
                                task_slug)






    def v_goto_home(self, body: dict) -> dict:
        """RECOVERY: drive every arm back to the stamped RUN HOME (probes/rituals END
        there — cameras clear; a died_with_edge job may have left the robot mid-scene).
        Background job like every motion verb."""
        self._ensure_home(body["task_slug"])       # the run starts at home
        led = self._ledger(body["task_slug"])
        joints = (getattr(led, "run_home", None) or {}).get("joints") or {}
        if not joints:
            return {"error": "no HOME stamped and no joints readable (edge warming "
                             "or bridge down) — cannot home"}
        stack = self._stack_provider() if self._stack_provider else None
        if stack is None or not hasattr(stack, "goto_joints"):
            return {"error": "no motion stack (edge warming or bridge down)"}

        def go():
            r = stack.goto_joints(joints)
            return {"ok": bool(getattr(r, "ok", False)),
                    "message": str(getattr(r, "message", r))[:300]}
        return self._run_as_job("goto_home", go, wait_s=float(body.get("wait_s", 8.0)))

    def v_motion_health(self, body: dict) -> dict:
        """SELF-HEALING TRIAGE (2026-07-18): prove every arm can PLAN from its current pose;
        on failure the built-in ablation ROUTES the fix — route=human (world model / limits /
        bridge: physical reality, do NOT retry, report and stop), route=agent (phantom
        self-collision: the reply's `heal` carries the goal + metric — repair the cell's
        collision model, then re-run THIS verb until ok), route=engine (planner fault: state
        it, stop). Run it BEFORE motion work and whenever 2+ consecutive plans fail — one
        verdict replaces 50 blind retries."""
        stack = self._stack_provider() if self._stack_provider else None
        if stack is None or not hasattr(stack, "motion_health"):
            return {"error": "no motion stack (edge still warming or bridge down)"}
        quick = bool(body.get("quick"))
        return self._run_as_job("motion_health",
                                lambda: stack.motion_health(quick=quick),
                                wait_s=float(body.get("wait_s", 8.0)))



    def v_phase_start(self, body: dict) -> dict:
        """Startup: capture RUN HOME from the live stack (COMP-09) + instrument notes."""
        led = self._ledger(body["task_slug"])
        joints = body.get("joints")                    # tests/sim may inject
        if joints is None:
            stack = self._stack_provider() if self._stack_provider else None
            reader = (getattr(stack, "joint_positions", None)
                      or getattr(self._bridge, "joint_state", None)
                      or getattr(stack, "_seed_positions", None))  # the real MotionStack's live config
            joints = dict(reader()) if callable(reader) else {}
        if not joints:
            return {"error": "no joints readable for RUN HOME — startup can't complete"}
        led.set_run_home(joints)
        led.save()
        return {"ok": True, "run_home_joints": len(joints)}



    def _cell_groups(self) -> Dict[str, Any]:
        """cell.yaml groups (joint_names per chain) — the packer selects each chain's
        joints from get_observation() with these (live 2026-07-15: without them the
        state vector was all zeros and the stats tour averaged blindness)."""
        try:
            groups = self._cell().get("groups") or {}
            if isinstance(groups, list):
                # canonical cell.yaml shape: groups is a LIST of {name, ...} (audit
                # 2026-07-21: dict() on that list THREW and this silently returned {})
                return {str(g.get("name")): g for g in groups if isinstance(g, dict)}
            return dict(groups)
        except Exception:                          # noqa: BLE001
            return {}

    def _groups_for_packer(self, stack: Any) -> Dict[str, Any]:
        """Chain -> joint_names for the packer. Raw cell.yaml groups often carry only
        tip_links (rig 2026-07-15: bootstrap refused with arm.position degraded while
        goto_joints worked fine — the joint names live in the STACK's enriched
        groups, not the yaml). Yaml first (either key spelling), stack overlay wins."""
        groups = self._cell_groups()
        for spec in groups.values():
            if isinstance(spec, dict) and "joint_names" not in spec and "joints" in spec:
                spec["joint_names"] = list(spec["joints"])
        if stack is not None:
            for g in list(getattr(stack, "group_names", []) or []):
                try:
                    jn = stack._group(g).get("joint_names")
                    if jn:
                        groups.setdefault(g, {})["joint_names"] = list(jn)
                except Exception:                  # noqa: BLE001 - yaml names remain
                    continue
        return groups

    def _camera_mount(self, camera: str) -> str:
        """cell.yaml cameras: name/id -> mount ('static' or the carrying arm).
        Unknown cameras return '' (treated as possibly-moving)."""
        if not hasattr(self, "_camera_mounts"):
            mounts: Dict[str, str] = {}
            try:
                import yaml
                cell = yaml.safe_load(Path("remoroo_cell/cell.yaml").read_text()) or {}
                for c in cell.get("cameras") or []:
                    name = str(c.get("name") or c.get("id") or "")
                    if name:
                        mounts[name] = str(c.get("mount") or c.get("role") or "")
            except Exception:                      # noqa: BLE001 - no cell, no mounts
                pass
            self._camera_mounts = mounts
        return self._camera_mounts.get(camera, "")






    def v_toolbox(self, body: dict) -> dict:
        from .perceive.loaders import toolbox_catalog
        return toolbox_catalog()

    # ---- perception models (install ONCE per cell; never inside a task run) -----------
    def v_models_status(self, body: dict) -> dict:
        from .perceive.loaders import models_status
        return {"weights_dir": str(self.data_dir / "weights"),
                "models": models_status(str(self.data_dir / "weights"))}

    def v_models_install(self, body: dict) -> dict:
        """Fetch + hash-verify the pinned checkpoints (GBs; minutes on first run).
        Idempotent — `remoroo models install` calls this at setup time so the agent
        never spends tokens discovering or fetching weights."""
        from .perceive.loaders import models_install
        return {"weights_dir": str(self.data_dir / "weights"),
                "models": models_install(str(self.data_dir / "weights"))}

    # ---- VLA verbs (ENG 8.3): the cold-start policy as smart probe / whole stage -------
    def _cell_is_multiarm(self) -> bool:
        """cell.yaml groups count (the edge runs in the working copy root). Unreadable
        cell -> False (never block a CI/fake path on a missing file)."""
        try:
            import yaml
            cell = yaml.safe_load(Path("remoroo_cell/cell.yaml").read_text()) or {}
            return len(cell.get("groups") or {}) > 1
        except Exception:                             # noqa: BLE001
            return False

    def v_vla_load(self, body: dict) -> dict:
        """Load a policy runtime on this cell. 'lingbot' (PRIMARY: LingBot-VLA 2.0 via
        its local policy server) | 'openpi' (pi0.5, evaluation) | 'fake' (CI/dry-run)."""
        kind = body.get("runtime", "fake")
        if kind == "fake":
            from .vla.runtime import FakeVLA
            self._vla = FakeVLA(approach_steps=int(body.get("approach_steps", 3)))
        elif kind == "lingbot":
            from .vla.runtime import LingBotRuntime
            profile = self._vla_profile()
            if profile is None and self._cell_is_multiarm():
                return {"error": ("this is a MULTI-ARM cell and there is no "
                                  "remoroo_cell/vla_profile.yaml — the canonical "
                                  "fallback layout is single-arm-only guesswork. "
                                  "AUTHOR the profile NOW from cell.yaml groups "
                                  "(cameras + state/actions slices; see the seed's "
                                  "vla profile section), then vla_load again")}
            runtime = LingBotRuntime(
                server_url=body.get("server_url") or self._vla_server_url(),
                profile=profile, tcps=body.get("tcps"))
            self._vla_observe = None
            if profile is not None:                   # profile-driven observation packing
                from .vla.packer import make_observation_packer
                stack = self._stack_provider() if self._stack_provider else None
                self._vla_observe = make_observation_packer(
                    profile, bridge=self._bridge, stack=stack,
                    groups=self._groups_for_packer(stack))
            obs = None
            packer_err = None
            if self._vla_observe is not None and self._bridge is not None:
                try:
                    obs = self._vla_observe()
                    bad = [d for d in (obs.get("degraded") or [])
                           if d.startswith("image:")]
                    if bad:                           # the server asserts these frames
                        packer_err = f"camera frames missing: {bad}"
                        obs = None
                except Exception as e:                # noqa: BLE001 - stated, not masked
                    packer_err = f"{type(e).__name__}: {e}"
                    obs = None
            proof = runtime.prove(obs)
            if packer_err and proof.get("proven") == "handshake":
                # a handshake-only proof because the PACKER failed is not a pass —
                # the silent downgrade masked a dead camera as "wire proven: 42ms"
                return {"error": f"cannot prove inference — the observation packer "
                                 f"failed ({packer_err}); scene_snapshot each profile "
                                 "camera to find the dead one",
                        "proof": proof}
            if proof.get("proven") is False:
                return {"error": f"vla wire proof failed: {proof.get('error')}",
                        "proof": proof}               # NOT loaded — stated, never silent
            self._vla = runtime
        elif kind == "openpi":
            from .vla.runtime import OpenPiRuntime
            self._vla = OpenPiRuntime(checkpoint=body.get("checkpoint", "pi05_base"),
                                      tcp=body.get("tcp", "arm_a"))
        else:
            return {"error": f"unknown runtime {kind!r} (fake | lingbot | openpi)"}
        self._vla_kind = kind
        out = {"ok": True, "runtime": kind}
        if kind == "lingbot":
            out["proof"] = proof                      # bytes exchanged, stated
            out["note"] = ("wire PROVEN" if proof.get("proven") == "inference"
                           else "wire connects (handshake); author vla_profile.yaml "
                                "to prove full inference")
        return out

    def v_vla_status(self, body: dict) -> dict:
        kind = getattr(self, "_vla_kind", None)
        return {"loaded": kind is not None, "runtime": kind}

    def _vla_profile(self):
        """The cell's authored embodiment profile (remoroo_cell/vla_profile.yaml), or
        None — the agent authors it; both wire sides derive from it."""
        from .vla.profile import load_profile
        return load_profile(str(self.data_dir.parent.parent
                                / "remoroo_cell" / "vla_profile.yaml"))

    def v_vla_bootstrap_stats(self, body: dict) -> dict:
        """REAL norm stats from a short guarded tour (motion job): N small joint-space
        offsets around home, observation packed at each stop, per-dim stats written to
        the file the server's config points at. Replaces identity stats (which
        de-normalize the policy's outputs into aimless motion); replaced in turn by
        episode statistics once the finetune pipeline runs."""
        profile = self._vla_profile()
        if profile is None:
            return {"error": "no vla_profile.yaml — author the embodiment first"}
        if self._bridge is None:
            return {"error": "no bridge wired; the bootstrap tour moves the robot"}

        def run() -> dict:
            from .vla.packer import make_observation_packer
            from .vla.stats import bootstrap_tour, stats_from_states
            stack = self._stack_provider() if self._stack_provider else None
            packer = make_observation_packer(profile, bridge=self._bridge, stack=stack,
                                             groups=self._groups_for_packer(stack))
            states = bootstrap_tour(profile, stack=stack, bridge=self._bridge,
                                    packer=packer,
                                    n_poses=int(body.get("n_poses", 10)),
                                    joint_span=float(body.get("joint_span", 0.12)),
                                    seed=int(body.get("seed", 0)))
            if len(states) < 4:
                return {"error": f"only {len(states)} poses reached — too few for "
                                 "stats; widen joint_span or check the planner"}
            import json as _json
            out_path = self.data_dir / "vla" / "norm_stats.json"
            out_path.parent.mkdir(parents=True, exist_ok=True)
            stats = stats_from_states(profile, states)
            out_path.write_text(_json.dumps(stats, indent=1))
            return {"ok": True, "poses": len(states), "path": str(out_path),
                    "features": sorted(stats["norm_stats"]),
                    "note": "restart the vla server (norm stats load at reset; the "
                            "cli yaml's norm_stats_file must point at this path)"}

        return self._run_as_job("vla_stats", run, wait_s=5.0)

    def v_vla_apply_profile(self, body: dict) -> dict:
        """Generate LingBot's OWN config files (robot config + the cli yaml the policy
        server reads) from the authored profile — the agent never hand-writes LingBot's
        format. Paths come from .remoroo/vla.yaml; restart the server after (`remoroo
        vla restart`) so it loads them."""
        profile = self._vla_profile()
        if profile is None:
            return {"error": "no remoroo_cell/vla_profile.yaml on this cell — author it "
                             "first (the embodiment wiring: cameras/state/actions)"}
        try:
            import yaml
            cfg = yaml.safe_load((self.data_dir.parent / "vla.yaml").read_text()) or {}
        except Exception as e:                        # noqa: BLE001
            return {"error": f"no readable .remoroo/vla.yaml ({type(e).__name__}: {e}) — "
                             "the rig must declare the policy server first"}
        workdir, checkpoint = cfg.get("workdir"), cfg.get("checkpoint")
        if not workdir or not checkpoint:
            return {"error": "vla.yaml needs workdir + checkpoint to place the configs"}
        from .vla.profile import write_lingbot_configs
        norm_stats = str(self.data_dir / "vla" / "norm_stats.json")   # finetune job output
        # the gate PROBED where this rig's server actually reads its config
        # (rig-specific, often NOT beside the checkpoint) and recorded it
        config_path = None
        commission = (self.data_dir.parent.parent / "remoroo_cell" / "vla"
                      / "commission.json")
        if commission.exists():
            try:
                import json as _json
                config_path = _json.loads(commission.read_text()).get("config_path")
            except Exception:                  # noqa: BLE001
                pass
        out = write_lingbot_configs(
            profile, workdir=workdir, checkpoint=checkpoint,
            config_path=config_path,
            qwen_path=cfg.get("qwen_path") or str(Path(checkpoint).parent
                                                  / "Qwen3-VL-4B-Instruct"),
            norm_stats_path=norm_stats)
        # the config DICTS ride along: task_vla_finetune ships them to the trainer box
        return {"ok": True, "robo_name": profile.robo_name,
                "state_dim": profile.state_dim, "action_dim": profile.action_dim,
                "norm_stats_expected_at": norm_stats, **out}

    def _vla_liveness(self) -> dict:
        """Declared? serving? — probed like a CITIZEN (full ws handshake + clean
        masked close; mirrors the CLI's probe: a bare connect makes the server log
        handshake tracebacks on every startup)."""
        import base64
        import socket as _socket

        cfg_path = self.data_dir.parent / "vla.yaml"
        out: dict = {"declared": cfg_path.exists(), "serving": False}
        if not out["declared"]:
            return out
        try:
            import yaml
            port = int((yaml.safe_load(cfg_path.read_text()) or {}).get("port", 8791))
        except Exception:                          # noqa: BLE001
            port = 8791
        out["port"] = port
        try:
            with _socket.create_connection(("127.0.0.1", port), timeout=2.0) as s:
                s.settimeout(2.0)
                key = base64.b64encode(__import__("os").urandom(16)).decode()
                s.sendall((f"GET / HTTP/1.1\r\nHost: 127.0.0.1:{port}\r\n"
                           "Upgrade: websocket\r\nConnection: Upgrade\r\n"
                           f"Sec-WebSocket-Key: {key}\r\n"
                           "Sec-WebSocket-Version: 13\r\n\r\n").encode())
                resp = s.recv(1024)
                out["serving"] = bool(resp)
                if resp.startswith(b"HTTP/1.1 101"):
                    mask = __import__("os").urandom(4)
                    payload = bytes(b ^ mask[i % 4]
                                    for i, b in enumerate(b"\x03\xe8"))
                    s.sendall(b"\x88\x82" + mask + payload)
                    try:
                        s.recv(64)
                    except OSError:
                        pass
        except OSError:
            pass
        return out

    def _vla_server_url(self) -> str:
        """The rig's declared policy server: .remoroo/vla.yaml (the SAME file
        `remoroo vla` starts the process from), so the service manager and this
        client can never disagree about the port."""
        default = "http://127.0.0.1:8791"
        cfg = self.data_dir.parent / "vla.yaml"           # data_dir = .remoroo/task
        try:
            import yaml
            port = (yaml.safe_load(cfg.read_text()) or {}).get("port")
            return f"http://127.0.0.1:{int(port)}" if port else default
        except Exception:                                 # noqa: BLE001 - default is fine
            return default

    def v_vla_rollout(self, body: dict) -> dict:
        """One guarded VLA episode as a FULL trial: reset -> observe -> policy (every
        action through GuardedExecutor clamps + envelope) -> judge -> record. THE FIRST
        ACT of looking when a VLA serves: vla_rollout(the task text) IS the jump start —
        day-zero demonstrations, keyframes, and possibly the task itself. Runs WITHOUT
        the authored package (day zero precedes authoring)."""
        runtime = getattr(self, "_vla", None)
        if runtime is None:
            return {"error": "no VLA runtime loaded; call vla_load first"}
        slug = body["task_slug"]
        instruction = body.get("instruction") or slug.replace("_", " ")
        try:
            env, _actor = self._env_builder(slug)
            refusal = self._preflight_env(env, None, slug=slug)
            if refusal:                            # authored but BROKEN: refuse, never
                return {"error": refusal}          # silently sidestep the authored env
        except Exception:                          # noqa: BLE001 - DAY ZERO has no package
            # THE JUMP START (live lesson 2026-07-14: requiring the authored package
            # here made the day-zero attempt structurally impossible — every agent
            # "skipped the VLA" because the machine gated it behind the authoring it
            # was meant to precede). A minimal env suffices: the guarded executor
            # clamps every action; judging day zero is the AGENT's job from the
            # before/after snapshots (view_image), not an authored verifier's.
            from .env import GenericEnv, resolvers_from_cell
            from .envelope import Envelope
            shared = self._shared()
            cell_cfg = shared.get("cell") or {}
            up_of, approach_of, effector_of = resolvers_from_cell(cell_cfg)
            from .judge.v0 import Verdict, milestones
            from .scene.state import SceneState
            env = GenericEnv(
                backend="real", stack=shared.get("stack"), bridge=shared.get("bridge"),
                envelope=Envelope.from_cell(cell_cfg),
                perceive_fn=lambda: SceneState(perception_program_id="day0-empty"),
                # day-zero honesty: score = proprioceptive milestones only (grasped,
                # carried); ok stays False — the AGENT judges outcome from the
                # before/after snapshots, an authored verifier judges later runs.
                judge_fn=lambda pre, post, trace: Verdict(
                    score=milestones(trace), ok=False, confidence=0.0,
                    judge_version="day0-milestones",
                    flags=["day0: judge outcome from snapshots via view_image"]),
                reset_fn=None, perception_version="day0",
                up_of=up_of, approach_of=approach_of, effector_of=effector_of)
        from .vla.skill import make_vla_skill
        packer = getattr(self, "_vla_observe", None)  # profile packer beats env.observe
        vla = make_vla_skill(runtime,
                             observe_of=(lambda env: packer) if packer else None)
        max_steps = int(body.get("max_steps", 30))

        def actor(env_, ctx):
            vla(env_, ctx, instruction, max_steps=max_steps)

        def run() -> dict:
            rec = run_trial(env, actor, task_slug=slug,
                            program_ref=f"vla:{getattr(self, '_vla_kind', '?')}",
                            knobs=Knobs(body.get("knobs") or {}),
                            seed=int(body.get("seed", 0)),
                            budget=TrialBudget(**(body.get("budget") or {})),
                            store=self._store(slug),
                            skip_reset=bool(body.get("skip_reset", False)))
            led = self._ledger(slug)
            led.instruments["vla_day0"] = {"trial_id": rec.trial_id,
                                           "outcome": rec.outcome,
                                           "instruction": instruction,
                                           "t": time.time()}
            led.save()
            out = {"trial_id": rec.trial_id, "outcome": rec.outcome,
                   "verdict": rec.verdict, "instruction": instruction,
                   "runtime": getattr(self, "_vla_kind", "?"),
                   "note": "day-zero attempt RECORDED (the looking exit needs it); "
                           "snapshot + view_image now to judge what it did"}
            flags = list(getattr(rec, "anomaly_flags", None) or [])
            if flags:                          # a failed actor names its own cause HERE
                out["anomalies"] = flags
            # MOTION ACCOUNTING (live 2026-07-15: "completed, zero movement" — turn
            # that into numbers): every commanded target is in the trace.
            import math
            moves = []
            for s in rec.trace:                 # targets nest under args (TraceStep)
                if s.get("name") not in ("vla_direct", "reach"):
                    continue
                a = s.get("args") or {}
                if a.get("tgt") or a.get("pose"):
                    moves.append(s)
            deltas = []
            prev = None
            for s in moves:
                a = s.get("args") or {}
                xyz = list(a.get("tgt") or a.get("pose"))[:3]
                if prev is not None:
                    deltas.append(math.dist(prev, xyz))
                prev = xyz
            out["motion"] = {"commands": len(moves),
                             "total_path_m": round(sum(deltas), 4),
                             "max_step_m": round(max(deltas), 4) if deltas else 0.0,
                             "decode": dict(getattr(runtime, "decode_stats", {}) or {})}
            dec = out["motion"]["decode"]
            if dec.get("rows") and not dec.get("emitted"):
                out["motion"]["note"] = (
                    f"the model produced {dec['rows']} action rows and the decoder "
                    "emitted ZERO commands — (near-)CONSTANT outputs, the collapsed-"
                    "policy signature of out-of-distribution inputs: NARROW norm "
                    "stats blow up state z-scores ((x-mean)/std) AND shrink the "
                    "output envelope. Re-run vla_bootstrap_stats with a WIDE tour, "
                    "restart, retry")
            elif moves and sum(deltas) < 0.01:
                out["motion"]["note"] = (
                    "the policy commanded near-ZERO displacement — its reachable "
                    "envelope is the norm-stats distribution (z*std+mean): a narrow "
                    "bootstrap tour = a policy trapped in a centimeter bubble. "
                    "Re-run vla_bootstrap_stats with a WIDER tour (joint_span 0.35+, "
                    "n_poses 16+) so the stats span the workspace, then restart+prove")
            return out

        return self._run_as_job("vla_day0", run, wait_s=5.0)
