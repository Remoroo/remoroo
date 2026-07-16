#!/usr/bin/env python3
"""Rig micro-benchmark: WHY is a warm cspace plan 10.5 s, and is it the CUDA-graph poison?

Runs the SAME plan cuRobo runs for `goto_joints` (`MotionPlanner.plan_cspace`) in a tight,
instrumented loop and prints `solve_time` (cuRobo's own metric — the one the edge log shows at
`solve_time=10.5`), wall time, success, and whether the CUDA-graph state got POISONED. It isolates
the mechanism with four independent knobs, so ONE table answers "do we need to touch cuRobo?":

  --graph {on,off}        cuRobo CUDA graphs on (default) or force eager (curobo.runtime.cuda_graphs)
  --warm-cspace {on,off}  capture the cspace graph in a QUIET window BEFORE timing (default on).
                          cuRobo's warmup() only warms plan_POSE, so normally the cspace graph is
                          captured lazily on the first goto_joints — under concurrent load. This
                          flag captures it clean first.
  --load {none,perception}  run cuRobo's perception graphs (segmenter + ESDF integrator) CONCURRENTLY
                          in a background thread — the SAME concurrent cuRobo-graph work the boot-time
                          "deep-warm perception" does while the first goto_joints plans. This is the
                          suspected poison trigger, reproduced with SYNTHETIC depth (no camera needed).
  --plans N / --max-attempts M / --delta R

The decisive matrix (run each; stop the edge first so the GPU is yours):
  A  --graph on  --warm-cspace on  --load none          clean warm replay  -> expect a few hundred ms
  B  --graph on  --warm-cspace off --load perception     reproduce the rig  -> expect seconds + POISON
  C  --graph on  --warm-cspace on  --load perception     does load poison a CLEAN-warmed graph? (key)
  D  --graph off --load none                              the pure-eager floor

Reading it:  A fast + D slow  => graphs are the whole story (not ESDF, not "cuRobo is slow").
             C fast           => a clean serial warm is enough; the fix is our warmup ordering, NO fork.
             C slow/poisoned  => steady-state replay is poisoned too; need serialize or camera-process.

Self-contained: builds its OWN MotionStack.from_cell — no edge, no cameras, no robot required (falls
back to the planner's default config as the start). Run WITHOUT the edge running (it owns the GPU).

    python3 bench_cspace_latency.py --cell /path/to/remoroo_cell --graph on --warm-cspace on --load none
"""
from __future__ import annotations

import argparse
import gc
import os
import random
import statistics
import sys
import threading
import time
import traceback
from pathlib import Path
from typing import Dict, List, Optional


# --- import motion_engine whether run from the repo or the bundled _studio tree ------------------
try:
    from motion_engine import MotionStack, DepthFrame  # type: ignore
except Exception:  # noqa: BLE001
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))   # the server/ dir
    from motion_engine import MotionStack, DepthFrame  # type: ignore


POISON_MARKERS = ("offset increment", "stream is capturing", "capture", "cuda graph", "cudagraph")


def _is_poison(msg: str) -> bool:
    m = (msg or "").lower()
    return any(k in m for k in POISON_MARKERS)


def _set_cuda_graphs(enabled: bool) -> str:
    """Flip cuRobo's global CUDA-graph switch BEFORE the planner is built (GraphExecutor reads it at
    construction). Returns a human string of what was set."""
    out = []
    for modname in ("curobo.runtime", "curobo._src.runtime"):
        try:
            mod = __import__(modname, fromlist=["cuda_graphs"])
            setattr(mod, "cuda_graphs", bool(enabled))
            out.append(f"{modname}.cuda_graphs={enabled}")
        except Exception as e:  # noqa: BLE001
            out.append(f"{modname}: {type(e).__name__}")
    return "; ".join(out)


class _BgLoad:
    """Base: a background thread doing GPU work concurrent with the solver, with error counting."""

    def __init__(self, hz: float) -> None:
        self.period = 1.0 / max(0.5, hz)
        self._stop = threading.Event()
        self._t: Optional[threading.Thread] = None
        self.iters = 0
        self.errors = 0
        self.last_error = ""

    def _once(self) -> None:  # override
        raise NotImplementedError

    def _run(self) -> None:
        while not self._stop.is_set():
            t0 = time.perf_counter()
            try:
                self._once()
                self.iters += 1
            except Exception as e:  # noqa: BLE001 — count, keep hammering (errors are informative)
                self.errors += 1
                self.last_error = f"{type(e).__name__}: {e}"
            dt = time.perf_counter() - t0
            if dt < self.period:
                self._stop.wait(self.period - dt)

    def start(self) -> None:
        self._t = threading.Thread(target=self._run, name="bg-load", daemon=True)
        self._t.start()
        time.sleep(0.5)

    def stop(self) -> None:
        self._stop.set()
        if self._t is not None:
            self._t.join(timeout=5.0)


class GraphLoad(_BgLoad):
    """Concurrent CUDA-GRAPH contention from another thread — the poison mechanism, distilled: it
    captures+replays a small torch CUDA graph and runs `torch.randn` (which touches the DEVICE-GLOBAL
    PyTorch RNG generator whose `capturing_` flag the poison corrupts) on its own stream, continuously.
    Reproduces "another thread does graph/RNG work while the solver captures/replays" WITHOUT touching
    the planner's collision world — so solves stay collision-free and the timing stays clean."""

    def __init__(self, hz: float = 30.0) -> None:
        super().__init__(hz)
        import torch  # noqa: F401 — fail fast if no CUDA torch
        self._dev = "cuda"

    def _once(self) -> None:
        import torch
        a = torch.randn(384, 384, device=self._dev)     # eager RNG (the poisoned resource) + matmul
        _ = a @ a
        g = torch.cuda.CUDAGraph()
        s = torch.cuda.Stream(device=self._dev)
        s.wait_stream(torch.cuda.current_stream(device=self._dev))
        with torch.cuda.stream(s):
            x = torch.randn(384, 384, device=self._dev)
        # torch.cuda.graph is patched to capture_error_mode=thread_local by capture_mode.install()
        # (same as the rig). A concurrent capture here still races the device-global generator flag.
        with torch.cuda.graph(g):
            _ = torch.randn(384, 384, device=self._dev) @ x
        g.replay()
        torch.cuda.synchronize(self._dev)


class PerceptionLoad(_BgLoad):
    """MOST FAITHFUL load: run cuRobo's real perception pipeline (segmenter + ESDF integrator, both
    CUDA-graphed) on SYNTHETIC frames — the exact concurrent cuRobo-graph work the boot deep-warm does.
    CAVEAT: update_world_live APPLIES an ESDF to the planner, so solves may then fast-fail on collision
    — under this load, read the POISON column (a RuntimeError), not solve_ms. Pair with --world modeled."""

    def __init__(self, stack: MotionStack, hz: float = 8.0) -> None:
        super().__init__(hz)
        self.stack = stack

    def _frames(self) -> List[DepthFrame]:
        import numpy as np
        H, W = 180, 320
        # Obstacles FAR from the robot (depth 2.5 m, camera 3 m out) so the ESDF this load applies
        # doesn't make the home config born-in-collision — we want the GPU/graph contention, not a
        # collision problem. The segmenter + ESDF graphs still run (the point), so plans keep timing.
        depth = np.full((H, W), 2.5, dtype=np.float32)
        K = np.array([[200.0, 0, W / 2.0], [0, 200.0, H / 2.0], [0, 0, 1]], dtype=np.float32)
        pose = ([3.0, 0.0, 1.0], [0.0, 0.0, 1.0, 0.0])     # far out, looking away from base (wxyz)
        q = self.stack._seed_positions() or {}
        return [DepthFrame(depth=depth, intrinsics=K, cam_pose_in_base=pose, q=q, name="synth")]

    def _once(self) -> None:
        try:
            self.stack.update_world_live(self._frames(), diagnostics=False)
        except TypeError:
            self.stack.update_world_live(self._frames())


class Planner:
    """Issue real cuRobo plans and read `solve_time` + trajectory quality. mode='cspace' = `plan_cspace`
    (joint goal; what `goto_joints` does). mode='pose' = `plan_pose` (Cartesian goal + IK; what
    `move_to_pose`/reach/descend do). n_goals>1 builds DIVERSE random goals so success is a real RATE
    over varied targets, not one trivial move repeated; motion_ms = the interpolated trajectory's
    execution time (finetune's quality payoff — fewer passes -> slower/rougher execution)."""

    def __init__(self, stack: MotionStack, delta: float, mode: str = "cspace",
                 finetune: int = -1, iters: int = 0, n_goals: int = 1, seed: int = 0,
                 preset_goals=None) -> None:
        self.stack = stack
        self.mode = mode
        groups = list(getattr(stack, "_groups", {}).keys())
        self.cvp = stack._planner_for(groups)          # CuroboV2Planner
        self.mp = self.cvp.planner                       # raw cuRobo MotionPlanner
        self.names = list(self.mp.joint_names)
        self.delta = float(delta)
        self.finetune = int(finetune)
        self.iters = int(iters)
        self.n_goals = max(1, int(n_goals))
        self._rng = random.Random(seed)
        self._preset_goals = preset_goals              # a pre-validated (feasible) goal list to reuse
        try:
            self._interp_dt = float(self.mp.trajopt_solver.config.interpolation_dt)
        except Exception:  # noqa: BLE001
            self._interp_dt = 0.025
        self._build_goals()

    def _cspace_goal(self, randomize: bool):
        from curobo.types import JointState
        q0 = self._start.position.detach().clone()
        lims = self.cvp.joint_limits() if hasattr(self.cvp, "joint_limits") else {}
        q1 = q0.clone()
        # Randomized goals: perturb a RANDOM SUBSET of joints by a random magnitude (a per-goal scale)
        # — fewer joints moving = far fewer dual-arm self-collisions, so most goals are FEASIBLE, and
        # the random scale gives an easy->hard spread. (Infeasible ones are still filtered by the
        # feasibility validator upstream.)
        scale = self._rng.uniform(0.3, 1.0) if randomize else 1.0
        for i, n in enumerate(self.names):
            if randomize:
                step = self._rng.uniform(-self.delta, self.delta) * scale if self._rng.random() < 0.5 else 0.0
            else:
                step = self.delta if i % 3 == 1 else (-self.delta if i % 3 == 2 else 0.0)
            lo, hi = lims.get(n, (-3.1, 3.1))
            q1[0, i] = min(max(float(q0[0, i]) + step, lo + 1e-3), hi - 1e-3)
        return JointState.from_position(q1, joint_names=self.names)

    def _pose_goal(self, randomize: bool, seed: dict):
        # FK every tool frame at home, then offset. randomize = a small random xyz box (diverse
        # reachable targets); else a fixed z-lift (deterministic single goal).
        goals = {}
        for tf in self.cvp._tool_frames:
            fk = self.cvp.link_pose(tf, seed)
            if fk is None:
                continue
            xyz, wxyz = [float(v) for v in fk[0]], [float(v) for v in fk[1]]
            if randomize:
                for k in range(3):
                    xyz[k] += self._rng.uniform(-self.delta, self.delta)
            else:
                xyz[2] += self.delta
            goals[tf] = (xyz, wxyz)
        if not goals:
            raise RuntimeError("no tool-frame FK available to build a pose goal")
        return self.cvp._goal(goals)

    def _build_goals(self) -> None:
        seed = self.stack._seed_positions() or {}
        self._start = self.cvp._start_js(seed)           # (1, n) JointState
        if self._preset_goals is not None:               # reuse a validated feasible set (all configs share it)
            self._goals = list(self._preset_goals)
            self.n_goals = max(1, len(self._goals))
            return
        rnd = self.n_goals > 1
        if self.mode == "cspace":
            self._goals = [self._cspace_goal(rnd) for _ in range(self.n_goals)]
        else:
            self._goals = [self._pose_goal(rnd, seed) for _ in range(self.n_goals)]

    def _iter_kw(self) -> dict:
        kw = {"finetune_attempts": max(0, self.finetune)}
        if self.iters > 0:
            kw.update(initial_iters=self.iters, time_optimal_iters=self.iters, finetune_iters=self.iters)
        return kw

    def _solve_direct_cspace(self, goal):
        return self.mp.trajopt_solver.solve_cspace(goal, self._start, **self._iter_kw())

    def _solve_direct_pose(self, goal, rec: dict):
        """IK (num_seeds) -> trajopt.solve_pose, timing IK and trajopt SEPARATELY."""
        import torch
        num_seeds = self.mp.trajopt_solver.config.num_seeds
        t_ik = time.perf_counter()
        ik = self.mp.ik_solver.solve_pose(goal, return_seeds=num_seeds, current_state=self._start)
        torch.cuda.synchronize()
        rec["ik_ms"] = (time.perf_counter() - t_ik) * 1e3
        seed_config = ik.solution
        good = int(torch.count_nonzero(ik.success))
        if good == 0:
            rec["msg"] = "IK found no solution"
            return None
        if good < num_seeds:                              # repair bad seeds (as plan_pose does)
            seed_config[~ik.success][:, :] = seed_config[ik.success][0:1, :].clone()
        return self.mp.trajopt_solver.solve_pose(
            goal, self._start, seed_config=seed_config, use_implicit_goal=True, **self._iter_kw())

    def _motion_ms(self, res):
        """Execution duration of the interpolated trajectory (n_points x interpolation_dt) — the
        quality signal for finetune (time-optimal retiming makes this SHORTER)."""
        try:
            ip = res.get_interpolated_plan()
            npts = int(ip.position.shape[-2])
            return npts * self._interp_dt * 1e3
        except Exception:  # noqa: BLE001
            return None

    def plan(self, max_attempts: int, gi: int = 0) -> dict:
        goal = self._goals[gi % len(self._goals)]
        t0 = time.perf_counter()
        rec = {"wall_ms": None, "solve_ms": None, "total_ms": None, "ik_ms": None,
               "motion_ms": None, "success": False, "poison": False, "msg": ""}
        try:
            if self.mode == "cspace":
                res = (self._solve_direct_cspace(goal) if self.finetune >= 0
                       else self.mp.plan_cspace(goal, self._start, max_attempts=max_attempts))
            else:
                res = (self._solve_direct_pose(goal, rec) if self.finetune >= 0
                       else self.mp.plan_pose(goal, self._start,
                                              use_implicit_goal=True, max_attempts=max_attempts))
            rec["wall_ms"] = (time.perf_counter() - t0) * 1e3
            if res is not None:
                st = getattr(res, "solve_time", None)
                tt = getattr(res, "total_time", None)
                rec["solve_ms"] = float(st) * 1e3 if st is not None else None
                rec["total_ms"] = float(tt) * 1e3 if tt is not None else None
                try:
                    rec["success"] = bool(res.success.any())
                except Exception:  # noqa: BLE001
                    rec["success"] = bool(getattr(res, "success", False))
                if rec["success"]:
                    rec["motion_ms"] = self._motion_ms(res)
        except Exception as e:  # noqa: BLE001
            rec["wall_ms"] = (time.perf_counter() - t0) * 1e3
            rec["msg"] = f"{type(e).__name__}: {e}"
            rec["poison"] = _is_poison(rec["msg"])
        return rec


def _agg(recs: List[dict], key: str) -> dict:
    xs = [r[key] for r in recs if r.get(key) is not None]
    if not xs:
        return {"n": 0}
    xs_sorted = sorted(xs)
    p95 = xs_sorted[min(len(xs_sorted) - 1, int(round(0.95 * (len(xs_sorted) - 1))))]
    return {"n": len(xs), "median": statistics.median(xs), "p95": p95,
            "min": xs_sorted[0], "max": xs_sorted[-1], "mean": statistics.fmean(xs)}


def _print_phase(title: str, recs: List[dict]) -> None:
    show_ik = any(r.get("ik_ms") is not None for r in recs)
    print(f"\n  [{title}]  ({len(recs)} plans)")
    hdr = f"    {'#':>3} {'wall_ms':>9} {'solve_ms':>9} {'total_ms':>9}"
    if show_ik:
        hdr += f" {'ik_ms':>8}"
    print(hdr + f" {'ok':>3} {'poison':>7}  msg")
    for i, r in enumerate(recs):
        w = f"{r['wall_ms']:.0f}" if r['wall_ms'] is not None else "-"
        s = f"{r['solve_ms']:.0f}" if r['solve_ms'] is not None else "-"
        t = f"{r['total_ms']:.0f}" if r['total_ms'] is not None else "-"
        line = f"    {i:>3} {w:>9} {s:>9} {t:>9}"
        if show_ik:
            ik = f"{r['ik_ms']:.0f}" if r.get("ik_ms") is not None else "-"
            line += f" {ik:>8}"
        print(line + f" {str(r['success']):>3} {str(r['poison']):>7}  {r['msg'][:70]}")
    a = _agg(recs, "solve_ms")
    aw = _agg(recs, "wall_ms")
    if show_ik:
        aik = _agg(recs, "ik_ms")
        if aik.get("n"):
            print(f"    ik_ms     median={aik['median']:.0f}  p95={aik['p95']:.0f}")
    amo = _agg(recs, "motion_ms")
    if amo.get("n"):
        print(f"    motion_ms median={amo['median']:.0f}  (trajectory execution time)")
    npois = sum(1 for r in recs if r["poison"])
    nok = sum(1 for r in recs if r["success"])
    if a.get("n"):
        print(f"    solve_ms  median={a['median']:.0f}  p95={a['p95']:.0f}  "
              f"min={a['min']:.0f}  max={a['max']:.0f}")
    if aw.get("n"):
        print(f"    wall_ms   median={aw['median']:.0f}  p95={aw['p95']:.0f}")
    print(f"    success={nok}/{len(recs)}   poisoned={npois}/{len(recs)}")


# ============================== ABLATION STUDY ==================================================

def _force_debug_off() -> None:
    for modname in ("curobo.runtime", "curobo._src.runtime"):
        try:
            setattr(__import__(modname, fromlist=["debug"]), "debug", False)
        except Exception:  # noqa: BLE001
            pass


def _patch_seeds(n: int):
    """Best-effort: force num_trajopt_seeds=n in MotionPlannerCfg.create (a BUILD-time param, so it
    must be patched around the planner build). Returns a restore() callable, or None if unpatchable."""
    try:
        from curobo.motion_planner import MotionPlannerCfg
    except Exception:  # noqa: BLE001
        return None
    desc = MotionPlannerCfg.__dict__.get("create")
    func = getattr(desc, "__func__", None)
    if func is None:
        return None
    is_static = isinstance(desc, staticmethod)

    def wrapped(*a, **kw):
        kw.setdefault("num_trajopt_seeds", int(n))
        return func(*a, **kw)

    MotionPlannerCfg.create = staticmethod(wrapped) if is_static else classmethod(wrapped)
    return lambda: setattr(MotionPlannerCfg, "create", desc)


def _ensure_safety_shim(cell: str) -> bool:
    """The cell's primitives.py Bridge imports a `safety_shim` spine that the EDGE pre-registers in
    sys.modules at startup — a standalone script must do the same or the Bridge import fails
    ('No module named safety_shim') and the harness silently falls back to the planner default
    (home) config, which then reads as in-collision against the real cell.yaml obstacles. Mirror the
    edge's registration (remoroo.edge is the shipped spine)."""
    try:
        from remoroo import edge as spine  # the shipped safety/transport spine
    except Exception:  # noqa: BLE001
        try:                               # fall back to the edge's own resolver (also registers it)
            import sys as _sys
            sys_path0 = str(Path(__file__).resolve().parents[1])
            if sys_path0 not in _sys.path:
                _sys.path.insert(0, sys_path0)
            from edge_real import _ensure_safety_shim_resolves  # type: ignore
            _ensure_safety_shim_resolves()
            return True
        except Exception as e:  # noqa: BLE001
            print(f"  could not register safety_shim spine ({type(e).__name__}: {e})")
            return False
    name = Path(cell).name
    sys.modules.setdefault("safety_shim", spine)
    sys.modules.setdefault(f"{name}.safety_shim", spine)
    return True


def _build_stack(cell: str, graph_on: bool, seeds: int, mode: str, delta: float,
                 warm: bool, warmup_plans: int, max_attempts: int,
                 use_bridge: bool = False, world: str = "empty") -> MotionStack:
    """Fresh stack: set graph/debug flags, (optionally) patch trajopt seeds + connect the bridge,
    build+prewarm, set the world (empty=clear for a latency floor / modeled=keep the cell obstacles
    for a REAL success test), and warm. One build serves a whole finetune x iters grid."""
    _set_cuda_graphs(graph_on)
    _force_debug_off()
    restore = _patch_seeds(seeds) if seeds and seeds > 0 else None
    bridge = None
    if use_bridge:
        _ensure_safety_shim(cell)          # register the spine the cell Bridge imports (like the edge)
        try:
            from remoroo_cell.primitives import Bridge  # type: ignore
            bridge = Bridge.from_cell_yaml(str(Path(cell) / "cell.yaml"))
            bridge.connect()
            print("  bridge CONNECTED — real robot config seeds the start")
        except Exception as e:  # noqa: BLE001
            print(f"  bridge connect FAILED ({type(e).__name__}: {e})")
            if world != "empty":
                print("  !!! --world modeled WITHOUT a real start = the DEFAULT (home) config, which "
                      "collides with the cell.yaml obstacles. Fix the bridge or use --world empty.")
            bridge = None
    print(f"  building stack (seeds={'default' if not seeds else seeds}, "
          f"graph={'on' if graph_on else 'off'}, world={world}, bridge={'yes' if bridge else 'no'})...")
    stack = MotionStack.from_cell(cell, bridge=bridge) if bridge is not None else MotionStack.from_cell(cell)
    stack.prewarm()
    if restore is not None:
        restore()                              # only needed during the build
    p0 = Planner(stack, delta, mode)
    if world == "empty":
        try:
            p0.cvp.clear_world()               # empty world -> latency solves always run
            print("  world CLEARED (empty; self-collision only)")
        except Exception as e:  # noqa: BLE001
            print(f"  clear_world failed: {type(e).__name__}: {e}")
    else:
        print("  world = MODELED (cell obstacles kept — this is a real success/quality test)")
    if warm:
        for i in range(max(2, warmup_plans)):
            p0.plan(max_attempts, i)
    try:
        stack._bench_bridge = bridge           # stashed so a multi-seed loop can release cameras
    except Exception:  # noqa: BLE001
        pass
    return stack


def _release_stack(stack) -> None:
    """Release the bridge/cameras (the 3 ZEDs are single-open, so the NEXT seed build can't reopen
    them unless we close first) and free GPU memory between rebuilds."""
    b = getattr(stack, "_bench_bridge", None)
    for m in ("disconnect", "close", "shutdown", "stop", "release"):
        fn = getattr(b, m, None)
        if callable(fn):
            try:
                fn()
                break
            except Exception:  # noqa: BLE001
                pass
    try:
        import torch
        gc.collect()
        torch.cuda.empty_cache()
    except Exception:  # noqa: BLE001
        pass


def _run_config(planner: Planner, plans: int, warmup: int, max_attempts: int) -> dict:
    ng = planner.n_goals
    for i in range(warmup):
        planner.plan(max_attempts, i % ng)
    recs = [planner.plan(max_attempts, i % ng) for i in range(plans)]
    return {"solve": _agg(recs, "solve_ms").get("median"), "wall": _agg(recs, "wall_ms").get("median"),
            "ik": _agg(recs, "ik_ms").get("median"), "motion": _agg(recs, "motion_ms").get("median"),
            "ok": sum(1 for r in recs if r["success"]), "n": len(recs),
            "poison": sum(1 for r in recs if r["poison"]),
            "msg": next((r["msg"] for r in recs if r["msg"]), "")}


def _start_ok(stack: MotionStack, mode: str, max_attempts: int):
    """Is the START collision-free in the CURRENT world? Plan a tiny (0.05) move — if that fast-fails
    on 'collision', the start config itself sits in the world (modeled obstacles wrong / robot near
    them), so nothing downstream is measurable. Returns (ok, msg)."""
    r = Planner(stack, 0.05, mode, finetune=-1, n_goals=1).plan(max_attempts, 0)
    return bool(r["success"]), (r.get("msg") or "")


def _make_validated_goals(stack: MotionStack, mode: str, delta: float, n: int, seed: int,
                          max_attempts: int):
    """Generate diverse goals and keep only those the FULL (real-path) planner SOLVES — so the goal
    set is genuinely feasible and success RATE measures the reduced config, not infeasible targets.
    Returns (goals, n_collision_rejected, n_tried)."""
    gen = Planner(stack, delta, mode, finetune=-1, n_goals=max(4 * n, 12), seed=seed)
    keep, coll, tried = [], 0, 0
    for gi in range(len(gen._goals)):
        tried += 1
        r = gen.plan(max_attempts, gi)
        if r["success"]:
            keep.append(gen._goals[gi])
        elif "collision" in (r.get("msg") or "").lower():
            coll += 1
        if len(keep) >= n:
            break
    return keep, coll, tried


def _fmt(v) -> str:
    return f"{v:.0f}" if isinstance(v, (int, float)) else "-"


def run_ablation(args) -> int:
    cell = str(Path(args.cell).resolve())
    fts = [int(x) for x in str(args.ablate_finetune).split(",") if x.strip()]
    its = [int(x) for x in str(args.ablate_iters).split(",") if x.strip()]
    seeds_list = [int(x) for x in str(args.ablate_seeds).split(",") if x.strip()] or [0]
    show_ik = args.mode == "pose"
    ng = max(1, int(args.goals))
    pcfg = ng if ng > 1 else args.ablate_plans          # 1 plan per distinct goal, else repeat one goal
    print("=" * 78)
    print(f"ABLATION STUDY  mode={args.mode}  graph={args.graph}  world={args.world}  "
          f"goals={ng}  plans/config={pcfg}  bridge={'yes' if args.bridge else 'no'}")
    print(f"  finetune={fts}  iters={its}  seeds={seeds_list if seeds_list != [0] else 'default(4)'}")

    rows: List[dict] = []
    prev_stack = None
    for seeds in seeds_list:
        if prev_stack is not None:                 # free the ZEDs/GPU before the next build reopens them
            _release_stack(prev_stack)
            prev_stack = None
        stack = _build_stack(cell, args.graph == "on", seeds, args.mode, args.delta,
                             warm=(args.warm_cspace == "on"), warmup_plans=2,
                             max_attempts=args.max_attempts, use_bridge=args.bridge, world=args.world)
        prev_stack = stack
        sk = "def" if not seeds else str(seeds)

        # Guard: is the START valid in this world? (modeled world / a bad robot config makes EVERY
        # plan fast-fail on 'start in collision' — measuring nothing.)
        ok, smsg = _start_ok(stack, args.mode, args.max_attempts)
        if not ok:
            print(f"  ⚠ [{sk}] START is NOT collision-free in world={args.world}: {smsg[:90]}")
            print("     -> the start config sits in the world. Use --world empty (default), or fix the "
                  "modeled obstacles / robot config. Skipping this build.")
            continue

        # Build ONE validated (feasible) goal set that every config is scored against.
        goals = None
        this_pcfg = pcfg
        if ng > 1:
            goals, coll, tried = _make_validated_goals(stack, args.mode, args.delta, ng, 1234,
                                                       args.max_attempts)
            print(f"  [{sk}] validated {len(goals)} feasible goals (tried {tried}, "
                  f"rejected {coll} in-collision)")
            if not goals:
                print("     -> no feasible goals at this --delta; lower it. Skipping this build.")
                continue
            this_pcfg = len(goals)

        def _mk(ft, it, _stack=stack, _goals=goals):
            return Planner(_stack, args.delta, args.mode, finetune=ft, iters=it,
                           n_goals=ng, seed=1234, preset_goals=_goals)

        # baseline = the REAL plan path (finetune=3 cspace / 1 pose + retry) at default iters
        base = _run_config(_mk(-1, 0), this_pcfg, 2, args.max_attempts)
        base["seeds"], base["ft"], base["it"], base["speedup"] = sk, "real", "def", 1.0
        rows.append(base)
        print(f"  [{sk}] real path (baseline): solve={_fmt(base['solve'])}ms "
              f"motion={_fmt(base['motion'])}ms ok={base['ok']}/{base['n']}")
        for ft in fts:
            for it in its:
                r = _run_config(_mk(ft, it), this_pcfg, 2, args.max_attempts)
                r["seeds"], r["ft"], r["it"] = sk, ft, it
                r["speedup"] = (base["solve"] / r["solve"]) if (base.get("solve") and r.get("solve")) else None
                rows.append(r)
                print(f"  [{sk}] ft={ft} it={it}: solve={_fmt(r['solve'])}ms "
                      f"motion={_fmt(r['motion'])}ms ok={r['ok']}/{r['n']} poison={r['poison']}")

    # --- table ---
    print("\n" + "=" * 78)
    print(f"ABLATION RESULTS  (mode={args.mode}, world={args.world}, goals={ng})")
    hdr = f"  {'seeds':>5} {'ft':>4} {'iters':>5} {'solve_ms':>9} {'motion_ms':>9} {'wall_ms':>8}"
    if show_ik:
        hdr += f" {'ik_ms':>6}"
    print(hdr + f" {'ok':>6} {'pois':>4} {'speedup':>8}")
    for r in rows:
        line = (f"  {r['seeds']:>5} {str(r['ft']):>4} {str(r['it']):>5} "
                f"{_fmt(r['solve']):>9} {_fmt(r.get('motion')):>9} {_fmt(r['wall']):>8}")
        if show_ik:
            line += f" {_fmt(r['ik']):>6}"
        sp = f"{r['speedup']:.1f}x" if r.get("speedup") else "-"
        print(line + f" {r['ok']}/{r['n']:<3} {r['poison']:>4} {sp:>8}")

    # --- recommendation: fastest config that keeps 100% success, no poison, and ~baseline motion time
    #     (motion within 1.5x of the real path's — so we don't trade a 15x planning win for a robot
    #     that then crawls through the move because finetune's time-optimal retiming was dropped). ---
    print("\n" + "-" * 78)
    for seeds in seeds_list:
        sk = "def" if not seeds else str(seeds)
        base = next((r for r in rows if r["ft"] == "real" and r["seeds"] == sk), None)
        cand = [r for r in rows if r["seeds"] == sk and r["ft"] != "real"
                and r["ok"] == r["n"] and r["poison"] == 0 and r.get("solve")]
        if not base or not cand:
            print(f"[seeds={sk}] no fully-succeeding config — widen the grid or check the msg column.")
            continue
        bm = base.get("motion")
        quality = [r for r in cand if not (bm and r.get("motion")) or r["motion"] <= 1.5 * bm]
        best = min(quality or cand, key=lambda r: r["solve"])
        sp = f"{base['solve'] / best['solve']:.1f}x" if base.get("solve") and best.get("solve") else "?"
        mnote = ""
        if bm and best.get("motion"):
            mnote = f", motion {_fmt(best['motion'])}ms vs {_fmt(bm)}ms baseline ({best['motion']/bm:.2f}x)"
        print(f"RECOMMENDATION [seeds={sk}, mode={args.mode}]: finetune={best['ft']} iters={best['it']} "
              f"-> {_fmt(best['solve'])} ms ({sp} vs real{mnote}), {best['ok']}/{best['n']} ok")
    if args.world == "empty":
        print("  NOTE: empty world + benchmark goals. Re-run with --world modeled --bridge --goals 30 "
              "for the REAL success rate + trajectory quality that picks the final config.")
    print("-" * 78)

    if args.json:
        import json
        Path(args.json).write_text(json.dumps({"args": vars(args), "rows": rows}, indent=2))
        print(f"  raw rows -> {args.json}")
    return 0


def main() -> int:
    ap = argparse.ArgumentParser(description="cuRobo cspace plan latency / CUDA-graph-poison probe")
    ap.add_argument("--cell", default=os.environ.get("REMOROO_CELL", "remoroo_cell"))
    ap.add_argument("--mode", choices=["cspace", "pose"], default="cspace",
                    help="cspace = plan_cspace (joint goal, what goto_joints does); "
                         "pose = plan_pose (Cartesian goal + IK, what move_to_pose/reach/descend do)")
    ap.add_argument("--graph", choices=["on", "off"], default="on")
    ap.add_argument("--warm-cspace", choices=["on", "off"], default="on")
    ap.add_argument("--load", choices=["none", "graph", "perception"], default="none")
    ap.add_argument("--world", choices=["empty", "modeled"], default="empty",
                    help="empty (default) clears obstacles so latency solves always run "
                         "(self-collision only); modeled keeps the cell obstacles")
    ap.add_argument("--plans", type=int, default=20)
    ap.add_argument("--warmup-plans", type=int, default=3)
    ap.add_argument("--max-attempts", type=int, default=5)
    ap.add_argument("--delta", type=float, default=0.15)
    ap.add_argument("--finetune", type=int, default=-1,
                    help="-1 (default) = real plan_cspace path (finetune=3 + retry). >=0 calls "
                         "trajopt_solver.solve_cspace directly with this many finetune passes "
                         "(0 = a single optimize pass) — attributes the 6.5 s to passes.")
    ap.add_argument("--iters", type=int, default=0,
                    help="0 = default LBFGS iters (100). >0 overrides per-pass iteration count "
                         "(needs --finetune >=0). Tests whether solve time scales with iterations.")
    ap.add_argument("--load-hz", type=float, default=8.0)
    ap.add_argument("--json", default="")
    # --- ablation study (one build, sweep finetune x iters [x rebuilt seeds]) ---
    ap.add_argument("--ablate", action="store_true",
                    help="run the finetune x iters (x seeds) ablation and print a ranked table")
    ap.add_argument("--ablate-finetune", default="0,1,2,3", help="comma list of finetune passes")
    ap.add_argument("--ablate-iters", default="25,50,75,100",
                    help="comma list of LBFGS iters/pass (multiples of inner_iters=25)")
    ap.add_argument("--ablate-seeds", default="",
                    help="comma list of num_trajopt_seeds to REBUILD+test (empty = default 4 only)")
    ap.add_argument("--ablate-plans", type=int, default=5, help="timed plans per config")
    ap.add_argument("--goals", type=int, default=1,
                    help=">1 tests that many DIVERSE random goals (real success RATE + motion time), "
                         "1 = one fixed goal repeated (pure latency)")
    ap.add_argument("--bridge", action="store_true",
                    help="connect the cell Bridge so the REAL robot config seeds the start "
                         "(needed with --world modeled so the start isn't born-in-collision)")
    args = ap.parse_args()

    if args.ablate:
        return run_ablation(args)

    cell = str(Path(args.cell).resolve())
    print("=" * 78)
    print("cuRobo cspace latency / graph-poison probe")
    print(f"  cell={cell}")
    print(f"  graph={args.graph}  warm_cspace={args.warm_cspace}  load={args.load}  "
          f"plans={args.plans}  max_attempts={args.max_attempts}  delta={args.delta}")

    # 1) flip the CUDA-graph switch BEFORE the planner is built; force debug OFF (debug adds
    #    per-op TORCH_CHECK + cuda syncs that could dominate — rule it out).
    print("  " + _set_cuda_graphs(args.graph == "on"))
    for modname in ("curobo.runtime", "curobo._src.runtime"):
        try:
            m = __import__(modname, fromlist=["debug"])
            if getattr(m, "debug", False):
                print(f"  WARNING: {modname}.debug was True — forcing False")
            setattr(m, "debug", False)
        except Exception:  # noqa: BLE001
            pass
    if args.finetune >= 0:
        print(f"  DIRECT solve_cspace: finetune_attempts={args.finetune} "
              f"iters={'default' if args.iters <= 0 else args.iters}")

    # 2) build + warm the stack (planner build + pose warmup). No bridge: default config start.
    t0 = time.perf_counter()
    stack = MotionStack.from_cell(cell)
    print(f"  MotionStack.from_cell: {time.perf_counter() - t0:.1f}s")
    t0 = time.perf_counter()
    try:
        stack.prewarm()                 # planner build + IK/trajopt (pose) warmup
    except Exception as e:  # noqa: BLE001
        print(f"  prewarm FAILED: {type(e).__name__}: {e}")
        traceback.print_exc()
        return 2
    print(f"  prewarm (build + pose warmup): {time.perf_counter() - t0:.1f}s")

    planner = Planner(stack, delta=args.delta, mode=args.mode,
                      finetune=args.finetune, iters=args.iters)
    print(f"  planner ready: mode={args.mode} dof={len(planner.names)} "
          f"groups={list(stack._groups.keys())}")

    # Clear the collision world so a latency solve isn't fast-failed on "start/end in collision"
    # (self-collision stays ON). The graph-vs-eager mechanism is independent of the world, so this is
    # a clean lower bound on solver latency; use --world modeled to keep the cell's obstacles.
    if args.world == "empty":
        try:
            planner.cvp.clear_world()
            print("  world CLEARED (empty; self-collision only) — latency solves will run")
        except Exception as e:  # noqa: BLE001
            print(f"  clear_world failed ({type(e).__name__}: {e}); solves may fast-fail on collision")

    # 3) optional CLEAN cspace warmup — capture the cspace graph while the GPU is QUIET (no load yet)
    warm_recs: List[dict] = []
    if args.warm_cspace == "on":
        print("\n  warming the cspace graph in a quiet window (this is the proposed fix — cuRobo only "
              "warms plan_pose)...")
        for _ in range(max(2, args.warmup_plans)):
            warm_recs.append(planner.plan(args.max_attempts))
        _print_phase("cspace WARM (quiet)", warm_recs)

    # 4) start the concurrent GPU load (if requested)
    load: Optional[_BgLoad] = None
    if args.load == "graph":
        print("\n  starting CUDA-graph contention load (torch capture+randn on another thread)...")
        load = GraphLoad(hz=max(args.load_hz, 20.0))
        load.start()
    elif args.load == "perception":
        print("\n  starting perception load (segmenter+ESDF graphs, synthetic frames — sets the "
              "world; read the POISON column, not solve_ms)...")
        load = PerceptionLoad(stack, hz=args.load_hz)
        load.start()

    # 5) untimed warmups (under the current load), then the timed run
    for _ in range(args.warmup_plans):
        planner.plan(args.max_attempts)

    timed: List[dict] = []
    poisoned_latched = False
    for _ in range(args.plans):
        r = planner.plan(args.max_attempts)
        if r["poison"]:
            poisoned_latched = True
        # once the process RNG is poisoned, later eager ops throw too — mark the run
        r["poison"] = r["poison"] or poisoned_latched
        timed.append(r)

    if load is not None:
        load.stop()
        print(f"\n  perception load: {load.iters} integrates, {load.errors} errors"
              + (f" (last: {load.last_error[:80]})" if load.last_error else ""))

    _print_phase(f"TIMED  (graph={args.graph} warm_cspace={args.warm_cspace} load={args.load})", timed)

    # 6) verdict
    a = _agg(timed, "solve_ms")
    med = a.get("median")
    npois = sum(1 for r in timed if r["poison"])
    print("\n" + "-" * 78)
    print("VERDICT")
    if med is None:
        print("  no solve_time captured — every plan raised. Check the msg column (likely poison).")
    else:
        print(f"  median warm solve_time = {med:.0f} ms   (edge log showed ~10500 ms; "
              f"cuRobo Orin baseline ~100-480 ms)")
        if args.graph == "off":
            print("  -> this is the PURE-EAGER floor. Compare against graph=on runs.")
        elif npois > 0:
            print(f"  -> {npois}/{len(timed)} plans POISONED. Concurrent load bricked the graph -> eager. "
                  "This reproduces the rig.")
        elif med < 1000:
            print("  -> graph replay WORKS here. The 10.5 s is NOT intrinsic to cuRobo/ESDF.")
            if args.load != "none" and args.warm_cspace == "on":
                print("     AND a clean-warmed graph SURVIVED the concurrent load => a serial warm is "
                      "sufficient; the fix is our warmup ordering, NO cuRobo fork needed.")
        else:
            print("  -> slow but no explicit poison caught — inspect solve_ms spread + the msg column.")
    print("  Matrix:  A --graph on  --warm-cspace on  --load none   (clean warm -> expect hundreds of ms)")
    print("           B --graph on  --warm-cspace off --load graph  (reproduce the poison)")
    print("           C --graph on  --warm-cspace on  --load graph  (does load poison a clean graph?)")
    print("           D --graph off --warm-cspace off --load none   (pure-eager floor)")
    print("-" * 78)

    if args.json:
        import json
        Path(args.json).write_text(json.dumps(
            {"args": vars(args), "warm": warm_recs, "timed": timed,
             "load_iters": (load.iters if load else 0)}, indent=2))
        print(f"  raw records -> {args.json}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
