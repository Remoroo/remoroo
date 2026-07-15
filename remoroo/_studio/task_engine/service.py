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
import itertools
import sys
import threading
import time
from pathlib import Path
from typing import Any, Callable, Dict, Optional

from .params import Knobs
from .perceive.bank import RegressionBank
from .record import TrialStore, replay_score
from .reset.reversibility import ReversibilityTable
from .run_trial import TrialBudget, run_trial
from .supervisor import Supervisor, SupervisorConfig

BRIDGE_VERBS = {"trial_run", "vla_rollout", "ritual_trial", "perceive_run",
                "probe_run", "scout_run", "scene_snapshot"}
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
        self.bank = RegressionBank.load(str(self.data_dir / "bank.jsonl"))
        self.reversibility = ReversibilityTable.load(str(self.data_dir / "reversibility.json"))
        self.supervisor: Optional[Supervisor] = None
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

    def _shared(self) -> Dict[str, Any]:
        """THE handoff to the authored cell package: build_env(shared) receives the LIVE
        surfaces here — the Bridge and the MotionStack — plus the task data dir. This is
        the contract; the package never reaches into the edge for accessors."""
        stack = None
        if self._stack_provider is not None:
            stack = self._stack_provider()             # may build/warm: that's the point
        return {"bridge": self._bridge, "stack": stack,
                "data_dir": str(self.data_dir),
                "calib": self._calib_surface()}

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
        return env, getattr(mod, "actor")

    def _perception_env(self, task_slug: str) -> Any:
        """Looking needs ONLY perception (audit 2026-07-13: requiring mod.actor made
        the agent author 7 files before its first look). Try the full package; when it
        is absent or partial, import perception_task alone and wrap it: make_perceive
        (preferred) or perceive/observe. Hot-reloaded like the full path."""
        try:
            env, _actor = self._env_builder(task_slug)
            return env
        except Exception:                              # noqa: BLE001 - partial package
            pass
        mod_name = f"remoroo_cell.task_{task_slug}.perception_task"
        for name in [m for m in list(sys.modules)
                     if m.startswith(f"remoroo_cell.task_{task_slug}")]:
            del sys.modules[name]
        importlib.invalidate_caches()
        mod = importlib.import_module(mod_name)
        maker = getattr(mod, "make_perceive", None) or getattr(mod, "perceive", None)
        if maker is None:
            raise AttributeError(f"{mod_name} must define make_perceive(shared)")
        perceive = maker(self._shared())
        stack = self._stack_provider() if self._stack_provider else None
        return type("E", (), {"stack": stack, "observe": staticmethod(perceive)})()

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
        finished = threading.Event()

        def work():
            try:
                rec["result"] = fn()
            except Exception as e:                 # noqa: BLE001 - stated, never a dead thread
                rec["result"] = {"error": f"{type(e).__name__}: {e}"}
            finally:
                rec["t1"] = time.time()
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
            return {"error": f"unknown job {job!r}"}
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
        cameras = body.get("cameras") or ([body["camera"]] if body.get("camera") else [])
        if not cameras:
            return {"error": "name the camera(s): {'camera': 'overhead'} or "
                             "{'cameras': [...]} (cell.yaml lists them)"}

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
            "supervisor": self.supervisor.status() if self.supervisor else {"state": "off"},
            "bank_cases": len(self.bank.cases),
            "reversibility": self.reversibility.to_dict(),
        }

    def v_supervisor_start(self, body: dict) -> dict:
        if self.supervisor is None:
            cfg = SupervisorConfig(**(body.get("config") or {}))
            bridge = self._bridge
            if bridge is None:
                return {"error": "no bridge wired; supervisor needs the cell"}
            self.supervisor = Supervisor(bridge, cfg)
            self.supervisor.start()
        return {"ok": True, "status": self.supervisor.status()}

    def v_supervisor_status(self, body: dict) -> dict:
        return self.supervisor.status() if self.supervisor else {"state": "off"}

    def v_supervisor_stop(self, body: dict) -> dict:
        if self.supervisor:
            self.supervisor.shutdown()
            self.supervisor = None
        return {"ok": True}

    def v_reversibility_get(self, body: dict) -> dict:
        return {"table": self.reversibility.to_dict()}

    def v_reversibility_update(self, body: dict) -> dict:
        self.reversibility.update(body["action_class"], bool(body["undo_ok"]))
        self.reversibility.save(str(self.data_dir / "reversibility.json"))
        return {"ok": True, "score": self.reversibility.score(body["action_class"])}

    def v_bank_stats(self, body: dict) -> dict:
        by = {}
        for c in self.bank.cases:
            by[c.source] = by.get(c.source, 0) + 1
        return {"cases": len(self.bank.cases), "by_source": by}

    def v_bank_label_grasp(self, body: dict) -> dict:
        case = self.bank.label_from_grasp(predicted_xyz=body["xyz"], held=bool(body["held"]),
                                          closed_width=float(body.get("closed_width", 0.0)),
                                          frame_ref=body.get("frame_ref"))
        self.bank.save(str(self.data_dir / "bank.jsonl"))
        return {"ok": True, "case_id": case.case_id}

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
        try:
            env, actor = self._env_builder(slug)
        except Exception as e:                     # noqa: BLE001
            return {"error": (f"cell task package not available for {slug!r}: "
                              f"{type(e).__name__}: {e}")}
        rec = run_trial(env, actor, task_slug=slug,
                        program_ref=body.get("program_ref", f"{slug}@cell"),
                        knobs=Knobs(body.get("knobs") or {}),
                        seed=int(body.get("seed", 0)),
                        budget=TrialBudget(**(body.get("budget") or {})),
                        store=self._store(slug),
                        skip_reset=bool(body.get("skip_reset", False)))
        return {"trial_id": rec.trial_id, "outcome": rec.outcome, "verdict": rec.verdict,
                "knobs": rec.knobs}

    def v_trial_replay(self, body: dict) -> dict:
        """Re-judge a stored trial from its evidence (the DEC-05 re-basing path)."""
        rec = self._store(body["task_slug"]).load(body["trial_id"])
        env, _actor = self._env_builder(body["task_slug"])
        return {"trial_id": rec.trial_id, "old": rec.verdict,
                "new": replay_score(rec, env.judge_fn)}

    # ---- scene record + phase spine + ritual (Task Engine v2, M0) ----------------------
    def _record(self, task_slug: str):
        from .scene_record.store import SceneRecord
        return SceneRecord.load(str(self.data_dir / "record" / f"{task_slug}.json"),
                                task_slug)

    def _ledger(self, task_slug: str):
        from .spine import PhaseLedger
        return PhaseLedger.load(str(self.data_dir / "state" / f"{task_slug}.json"),
                                task_slug)

    def v_record_submit(self, body: dict) -> dict:
        """The agent's perception submits a scene WITH capture provenance; the engine
        merges and GRADES (two looks or a touch; arm-correlation bounce; calibration
        attribution). Returns the grade — the agent iterates until it passes."""
        rec = self._record(body["task_slug"])
        return rec.submit(body.get("entities") or [], body.get("provenance") or {})

    def v_record_get(self, body: dict) -> dict:
        return self._record(body["task_slug"]).to_dict()

    def v_record_confirm(self, body: dict) -> dict:
        """Physical proof from a probe/trial: touch|lift|trial. Also narrows ranges."""
        rec = self._record(body["task_slug"])
        rec.confirm_touch(body["object_id"], kind=body.get("kind", "touch"),
                          source=body.get("source", "probe"))
        for n in body.get("narrow") or []:
            rec.narrow(body["object_id"], n["param"], float(n["lo"]), float(n["hi"]))
        return {"ok": True,
                "proof_level": rec.objects[body["object_id"]].proof_level()}

    def _facts(self, slug: str, evidence: Optional[dict] = None) -> Dict[str, Any]:
        """The engine's own answer to "did this phase earn its exit" — computed from
        the record, the ground-truth library, and the trial store (measured), plus
        shape-checked tool-result documents for the off-cell phases (reported). The
        agent's claims are never facts (audit 2026-07-13)."""
        led = self._ledger(slug)
        rec = self._record(slug)
        facts: Dict[str, Any] = {
            "run_home_set": bool(led.run_home),
            "actionable": len(rec.entities(min_proof="corroborated")),
            "gt_cases": len(self._library().cases),
        }
        store = self._store(slug)
        from .expect import summarize
        total = ok = declared = passed = 0
        for tid in store.all_ids():
            r = store.load(tid)
            total += 1
            if r.verdict.get("ok") or r.outcome == "success":
                ok += 1
            s = summarize(r.trace)
            declared += int(s.get("declared", 0))
            passed += int(s.get("passed", 0))
        facts.update(trials_total=total, trials_ok=ok,
                     expect_declared=declared, expect_passed=passed)
        ev = evidence if isinstance(evidence, dict) else {}
        ranked = ev.get("ranked")
        facts["sim_ranked"] = (sum(1 for r in ranked
                                   if isinstance(r, dict) and r.get("ref")
                                   and isinstance(r.get("mean"), (int, float)))
                               if isinstance(ranked, list) else 0)
        promoted = ev.get("promoted")
        facts["promoted_fitness"] = (promoted.get("fitness")
                                     if isinstance(promoted, dict) else None)
        facts["vla_day0"] = bool(led.instruments.get("vla_day0"))
        facts["vla_waived"] = "vla" in led.waivers
        return facts

    def v_phase_status(self, body: dict) -> dict:
        slug = body["task_slug"]
        return self._ledger(slug).status(facts=self._facts(slug))

    def v_phase_waive(self, body: dict) -> dict:
        """Waive sim or search with the stated reason (e.g. task_sibling reported the
        worker cannot run cousins). LOUD: stamped in the ledger, printed on the cert."""
        return self._ledger(body["task_slug"]).waive(body.get("phase", ""),
                                                     body.get("reason", ""))

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
        led.instruments = body.get("instruments") or led.instruments
        # the VLA truth, declared or not, commissioned or not — the agent must
        # never have to DISCOVER that a serving policy exists (a live run skipped
        # the scout because nothing said the server was up)
        led.instruments["vla"] = self._vla_liveness()
        commission = (self.data_dir.parent.parent / "remoroo_cell" / "vla"
                      / "commission.json")
        if commission.exists():
            try:
                import json as _json
                c = _json.loads(commission.read_text(encoding="utf-8"))
                led.instruments["vla"].update({
                    "runtime": c.get("runtime"),
                    "latency_ms": c.get("latency_ms"),
                    "wire": c.get("wire"),
                    "commissioned_at": c.get("commissioned_at")})
            except Exception as e:             # noqa: BLE001 - stated, not fatal
                led.instruments["vla"]["commission_error"] = (
                    f"unreadable commission.json: {type(e).__name__}")
        led.save()
        v = led.instruments.get("vla") or {}
        return {"ok": True, "run_home_joints": len(joints),
                "vla_declared": bool(v.get("declared")),
                "vla_serving": bool(v.get("serving")),
                "vla_commissioned": bool(v.get("commissioned_at")),
                "note": ("VLA is SERVING — vla_load {runtime} NOW (a client handle, "
                         "PROVES the wire with one round-trip, ~10s warm); "
                         "scout_run gives your second viewpoints"
                         if v.get("serving") else
                         "VLA declared but not serving yet (loading takes minutes); "
                         "retry vla_load before scouting" if v.get("declared") else
                         "no VLA on this rig")}

    def v_phase_advance(self, body: dict) -> dict:
        slug = body["task_slug"]
        ev = body.get("evidence") or {}
        return self._ledger(slug).advance(body["to"], ev,
                                          facts=self._facts(slug, ev))

    def v_phase_regress(self, body: dict) -> dict:
        return self._ledger(body["task_slug"]).regress(body["to"],
                                                       body.get("reason", ""))

    def v_ritual_trial(self, body: dict) -> dict:
        """A real trial under the ritual (home, drift check, act, home, look, compare).
        This is how the search's promoted candidates touch hardware; the ledger must be
        in the 'real' phase (the flow, not a guard: hardware is this phase's tool)."""
        slug = body["task_slug"]
        led = self._ledger(slug)
        if led.phase != "real":
            return {"error": f"ritual trials belong to the 'real' phase; ledger says "
                             f"{led.phase!r} — the spine advances on evidence"}
        try:
            env, actor = self._env_builder(slug)
        except Exception as e:                     # noqa: BLE001
            return {"error": f"cell task package not available: "
                             f"{type(e).__name__}: {e}"}
        def run() -> dict:
            from .ritual import run_ritual_trial
            out = run_ritual_trial(
                env, actor, task_slug=slug,
                program_ref=body.get("program_ref", f"{slug}@cell"),
                record=self._record(slug), run_home=led.run_home,
                sim_prediction=body.get("sim_prediction"),
                knobs=Knobs(body.get("knobs") or {}), seed=int(body.get("seed", 0)),
                budget=TrialBudget(**(body.get("budget") or {})),
                store=self._store(slug))
            rec = out["record"]
            res = {"ritual": out["ritual"]}
            if rec is not None:
                from .expect import summarize
                res.update(trial_id=rec.trial_id, outcome=rec.outcome,
                           verdict=rec.verdict,
                           expectations=summarize(rec.trace))
            return res

        return self._run_as_job("ritual", run, wait_s=5.0)

    # ---- ground truth + replay + probes + scout (Task Engine v2, M1) --------------------
    def _library(self):
        from .perceive.groundtruth import GroundTruthLibrary
        return GroundTruthLibrary.load(str(self.data_dir / "groundtruth"))

    def v_record_retire(self, body: dict) -> dict:
        """Retire record objects that never should have been there (debug entities, a
        wall grabbed as a 'container'). Retired objects keep absorbing sightings (a
        bounced phantom must not respawn) but leave look_plan and the actionable set.
        Match by object_ids and/or a label_prefix."""
        slug = body["task_slug"]
        rec = self._record(slug)
        ids = set(body.get("object_ids") or [])
        prefix = body.get("label_prefix") or ""
        reason = body.get("reason", "agent retire")
        hit = [o for o in rec.objects.values()
               if (o.object_id in ids) or (prefix and o.label.startswith(prefix))]
        if not hit:
            return {"error": "nothing matched — pass object_ids and/or label_prefix "
                             "(record_get lists them)"}
        for o in hit:
            if not o.retired:
                o.retired = reason
        rec.save()
        alive = [o for o in rec.objects.values() if not o.retired]
        return {"retired": sorted(o.object_id for o in hit),
                "remaining": len(alive),
                "note": "retired objects no longer appear in look_plan or count "
                        "toward pending looks"}

    def v_gt_stats(self, body: dict) -> dict:
        lib = self._library()
        return {"cases": len(lib.cases),
                "by_provenance": {p: sum(1 for c in lib.cases
                                         if c.provenance.startswith(p))
                                  for p in {"probe", "trial"}}}

    def v_replay_score(self, body: dict) -> dict:
        """Score a perception program against every physically confirmed scene this
        cell has ever recorded (IFACE-04) — the free fitness; no robot time."""
        from .perceive.groundtruth import score_perception
        lib = self._library()
        if not lib.cases:
            return {"error": "ground-truth library is empty — probe first (every "
                             "touch/lift writes a case), then replay means something"}
        return score_perception(body["program_src"], lib)

    def v_probe_run(self, body: dict) -> dict:
        """One probe on a RECORDED object; success writes proof + ground-truth case +
        narrowed ranges in one call (rule 5). Bridge-locked (real motion)."""
        slug = body["task_slug"]
        try:
            env, _actor = self._env_builder(slug)
        except Exception as e:                     # noqa: BLE001
            return {"error": f"cell task package not available: "
                             f"{type(e).__name__}: {e}"}
        def run() -> dict:
            frame = None
            if body.get("camera"):
                from .perceive.operators import PerceptionOps
                frame = PerceptionOps(None, None, env.bridge).capture(body["camera"])
            from .probe_record import probe_and_record
            from .ritual import _goto_home
            led = self._ledger(slug)
            _goto_home(env, led.run_home)          # probe starts from RUN HOME ...
            out = probe_and_record(
                body.get("kind", "touch"), env.new_ctx(), body.get("tcp", ""),
                record=self._record(slug), object_id=body["object_id"],
                library=self._library(), frame=frame,
                narrow=body.get("narrow"), params=body.get("params"))
            _goto_home(env, led.run_home)          # ... and ends there: cameras clear
            return out

        return self._run_as_job("probe", run, wait_s=5.0)

    @staticmethod
    def _fk_ready(stack) -> bool:
        """True when FK through the stack costs an FK, not a PLANNER BUILD. Live
        2026-07-14: provenance stamping inside perceive_run triggered the full 124s
        robot planner build (stack.link_pose -> _planner_for -> build). A real
        MotionStack exposes _planners; empty means unbuilt — skip FK. Fakes without
        the attribute stay allowed."""
        planners = getattr(stack, "_planners", None)
        return planners is None or bool(planners)

    def _cell_groups(self) -> Dict[str, Any]:
        """cell.yaml groups (joint_names per chain) — the packer selects each chain's
        joints from get_observation() with these (live 2026-07-15: without them the
        state vector was all zeros and the stats tour averaged blindness)."""
        try:
            import yaml
            cell = yaml.safe_load(Path("remoroo_cell/cell.yaml").read_text()) or {}
            return dict(cell.get("groups") or {})
        except Exception:                          # noqa: BLE001
            return {}

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

    def _stamped_provenance(self, env, camera: str) -> Dict[str, Any]:
        """The ENGINE stamps provenance — never from agent claims (grading
        integrity). A static camera (cell.yaml mount) is ONE viewpoint forever. A
        moving mount's viewpoint is the JOINT CONFIGURATION bucketed to 0.05 rad —
        joints move exactly when the camera moves, cost one bridge read, and need
        no FK (live 2026-07-14: FK'ing the camera NAME hit cuRobo's parent map
        with 'overhead' AND triggered the 124s planner build inside perceive_run).
        Same-place re-looks can still never count as a second look."""
        arm_poses: Dict[str, list] = {}
        viewpoint = camera
        stack = getattr(env, "stack", None)
        if self._camera_mount(camera) != "static":
            bucket = ""
            bridge = self._bridge or getattr(env, "bridge", None)
            try:                                   # joints ARE the mount's pose
                obs = bridge.get_observation()
                jp = dict(getattr(obs, "joint_positions", None) or {})
                if jp:
                    import hashlib
                    key = ",".join(f"{n}:{round(float(v) / 0.05)}"
                                   for n, v in sorted(jp.items()))
                    bucket = hashlib.md5(key.encode()).hexdigest()[:8]
            except Exception:                      # noqa: BLE001 - fall through to FK
                pass
            if not bucket and stack is not None and self._fk_ready(stack):
                try:
                    cam_pose = stack.link_pose(camera)
                    xyz = (list(cam_pose[0]) if isinstance(cam_pose, tuple)
                           else list(cam_pose)[:3])
                    bucket = "_".join(str(round(v / 0.05)) for v in xyz[:3])
                except Exception:                  # noqa: BLE001 - static after all
                    pass
            if bucket:
                viewpoint = f"{camera}@{bucket}"
        if stack is not None and self._fk_ready(stack):
            for g in list(getattr(stack, "group_names", []) or [])[:4]:
                try:
                    pose = stack.link_pose(g)
                    xyz = (list(pose[0]) if isinstance(pose, tuple) else
                           list(pose)[:3])
                    arm_poses[g] = [float(v) for v in xyz[:3]]
                except Exception:                  # noqa: BLE001 - stamp what we can
                    continue
        return {"source": camera, "viewpoint": viewpoint, "modality": "rgbd",
                "arm_poses": arm_poses, "kind": "view"}

    def v_perceive_run(self, body: dict) -> dict:
        """THE looking-phase loop (closes the live-run gap of 2026-07-11): run the
        AUTHORED perception against a live capture and submit the result to the
        record with ENGINE-stamped provenance. Two paths:
          - default: the task package's env.observe() (perception_task via build_env,
            hot-reloaded per call — your edit cycle is edit -> perceive_run);
          - program_src: run an inline make_perceive(shared) candidate BEFORE the
            package exists (early iteration; same stamping).
        Returns the grade + what was seen, so the agent iterates until it passes."""
        slug = body["task_slug"]
        camera = body.get("camera", "")
        if body.get("program_src"):
            ns: Dict[str, Any] = {}
            exec(compile(body["program_src"], "<perception_candidate>", "exec"), ns)  # noqa: S102
            if "make_perceive" not in ns:
                return {"error": "program_src must define make_perceive(shared)"}
            perceive = ns["make_perceive"](self._shared())
            stack = self._stack_provider() if self._stack_provider else None
            env = type("E", (), {"stack": stack, "observe": staticmethod(perceive)})()
        else:
            try:
                env = self._perception_env(slug)
            except Exception as e:                 # noqa: BLE001
                return {"error": (f"no perception for {slug!r} and no program_src "
                                  f"({type(e).__name__}: {e}) — author "
                                  "perception_task.py with make_perceive(shared) "
                                  "(the FULL package with actor is a SIM-phase "
                                  "deliverable, not a looking one) or pass "
                                  "program_src")}
        dry = bool(body.get("dry_run"))

        def run() -> dict:
            print(f"[perceive] running {slug} perception (FIRST call loads pinned "
                  "models into this process — minutes on embedded GPUs; this is not a "
                  "hang)", flush=True)
            scene = env.observe()
            print(f"[perceive] done: {len(scene.entities())} entities", flush=True)
            entities = [{"label": e.label, "pose": list(e.pose[:3]),
                         "dims": list(e.dims) if e.dims else None}
                        for e in scene.entities()]
            shown = entities[:40]                  # the agent reads results HERE, never
            frame_paths: Dict[str, str] = {}       # by spelunking the record file
            if camera and self._bridge is not None:
                try:
                    from .perceive.operators import PerceptionOps
                    frame = PerceptionOps(None, None, self._bridge).capture(camera)
                    frame_paths = self._save_frame(frame, f"{camera}_{time.strftime('%H%M%S')}")
                except Exception:                  # noqa: BLE001 - frames are a courtesy
                    frame_paths = {}
            if dry:
                return {"seen": len(entities), "entities": shown, "dry_run": True,
                        "frame": frame_paths or None,
                        "note": ("DRY RUN — nothing recorded. The scene record is a "
                                 "world model, never a debug channel. Iterate here "
                                 "until the entities look sane (view_image the saved "
                                 "frame to compare), then re-run without dry_run.")}
            prov = self._stamped_provenance(env, camera or "unknown")
            grade = self._record(slug).submit(entities, prov)
            pending = grade.get("pending_second_look") or []
            if pending:
                live = self._vla_liveness()
                how = ("scout_run NOW — the VLA is SERVING and its sweep is your second "
                       "viewpoint" if live.get("serving") else
                       "the VLA is still loading — run an atoms.look tour over "
                       "look_plan's proposals NOW and retry scout_run afterwards"
                       if live.get("declared") else
                       "run an atoms.look tour over look_plan's proposals NOW (no VLA "
                       "on this rig)")
                note = (f"{len(pending)} object(s) await a SECOND LOOK — do NOT keep "
                        f"iterating this camera's code (single-view polishing is "
                        f"unbounded; a second viewpoint corroborates for free): {how}. "
                        "Iterate perception only for defects visible ACROSS views.")
            else:
                note = ("all detections corroborated or bounced — probe the accepted "
                        "objects next (probe_run), then advance")
            return {"seen": len(entities), "entities": shown,
                    "viewpoint": prov["viewpoint"], "frame": frame_paths or None,
                    "grade": grade, "note": note}

        return self._run_as_job("perceive", run, wait_s=45.0)

    def v_scout_run(self, body: dict) -> dict:
        """The VLA scout (COMP-12): a guarded rollout that SWEEPS while saving
        keyframes for the agent's perception to process. Each keyframe carries a
        distinct viewpoint tag, so submissions from these frames satisfy the two-look
        rule. No VLA loaded -> stated fallback (PROB-12): use a look tour instead."""
        runtime = getattr(self, "_vla", None)
        if runtime is None:
            return {"error": "no VLA runtime loaded (vla_load first); fallback: plan a "
                             "look tour with atoms.look over 3-4 viewpoints and submit "
                             "each view — same two-look effect, slower"}
        live = self._vla_liveness()
        if live.get("declared") and not live.get("serving"):
            return {"error": "the VLA server is still LOADING (a 6B checkpoint takes "
                             "minutes) — do the atoms.look tour now and retry "
                             "scout_run when `vla_status`/phase instruments say "
                             "serving; never restart a loading server"}
        slug = body["task_slug"]
        try:
            env, _actor = self._env_builder(slug)
        except Exception as e:                     # noqa: BLE001
            return {"error": f"cell task package not available: "
                             f"{type(e).__name__}: {e}"}
        camera = body.get("camera", "wrist")
        every = int(body.get("keyframe_every", 3))
        out_dir = self.data_dir / "scout" / slug
        out_dir.mkdir(parents=True, exist_ok=True)
        from .perceive.operators import PerceptionOps
        ops = PerceptionOps(None, None, env.bridge)
        frames: list = []

        base_observe = (getattr(self, "_vla_observe", None) or env.observe)
        steps = [0]

        def observe_and_tap():
            obs = base_observe()
            if steps[0] % every == 0:
                f = ops.capture(camera)
                k = len(frames)
                ref = str(out_dir / f"kf_{k:03d}.json")
                import json as _json
                Path(ref).write_text(_json.dumps(
                    {"camera": camera, "t": f.get("t_capture"),
                     "viewpoint": f"scout_{k:03d}"}, default=str))
                frames.append({"ref": ref, "viewpoint": f"scout_{k:03d}"})
            steps[0] += 1
            return obs

        def run() -> dict:
            from .vla.skill import make_vla_skill
            vla = make_vla_skill(runtime, observe_of=lambda _env: observe_and_tap)
            ctx = env.new_ctx()
            r = vla(env, ctx, body.get("instruction",
                                       "find the objects and record their poses"),
                    max_steps=int(body.get("max_steps", 20)))
            return {"ok": r.ok, "stopped_by": r.evidence.get("stopped_by"),
                    "keyframes": frames,
                    "note": "run your perception on each keyframe and record_submit "
                            "with its viewpoint tag — that satisfies the two-look rule"}

        return self._run_as_job("scout", run, wait_s=5.0)

    def v_audit_bundle(self, body: dict) -> dict:
        """The cert's audit bundle (ART-06): record + ledger + per-trial expectation
        stats into .remoroo/task/audit/<slug>/bundle.json — what makes the
        certificate a legible artifact."""
        import json as _json
        slug = body["task_slug"]
        from .expect import summarize
        trials = []
        store = self._store(slug)
        for tid in store.all_ids()[-50:]:
            rec = store.load(tid)
            trials.append({"trial_id": tid, "outcome": rec.outcome,
                           "verdict": rec.verdict,
                           "expectations": summarize(rec.trace)})
        led = self._ledger(slug)
        bundle = {"task_slug": slug,
                  "record": self._record(slug).to_dict(),
                  "ledger": led.status(),
                  "waivers": led.waivers,      # a waived phase is LOUD on the cert
                  "evidence_provenance": {
                      "measured": ["looking", "real"],   # edge-owned stores
                      "reported": ["sim", "search"],     # off-cell tool results
                      "waived": sorted(led.waivers)},
                  "trials": trials}
        out = self.data_dir / "audit" / slug / "bundle.json"
        out.parent.mkdir(parents=True, exist_ok=True)
        out.write_text(_json.dumps(bundle, indent=1, default=str), encoding="utf-8")
        return {"ok": True, "path": str(out), "trials": len(trials)}

    def v_look_plan(self, body: dict) -> dict:
        """Ranked next-viewpoint proposals from the record's open questions (COMP-14):
        look requests first, then unknown volumes by size, then single-look objects."""
        from .scene_record.lookplan import propose
        return {"proposals": propose(self._record(body["task_slug"]))}

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
                    groups=self._cell_groups())
            obs = None
            if self._vla_observe is not None and self._bridge is not None:
                try:
                    obs = self._vla_observe()
                except Exception:                     # noqa: BLE001 - handshake proof then
                    obs = None
            proof = runtime.prove(obs)
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
                                             groups=self._cell_groups())
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
        except Exception:                          # noqa: BLE001 - DAY ZERO has no package
            # THE JUMP START (live lesson 2026-07-14: requiring the authored package
            # here made the day-zero attempt structurally impossible — every agent
            # "skipped the VLA" because the machine gated it behind the authoring it
            # was meant to precede). A minimal env suffices: the guarded executor
            # clamps every action; judging day zero is the AGENT's job from the
            # before/after snapshots (view_image), not an authored verifier's.
            from .env import GenericEnv
            from .envelope import Envelope
            shared = self._shared()
            try:
                import yaml
                cell_cfg = yaml.safe_load(Path("remoroo_cell/cell.yaml").read_text()) or {}
            except Exception:                      # noqa: BLE001 - defaults still clamp
                cell_cfg = {}
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
                reset_fn=None, perception_version="day0")
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
            return out

        return self._run_as_job("vla_day0", run, wait_s=5.0)
