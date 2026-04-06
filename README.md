---
title: "Your First Experiment"
description: "A complete walkthrough: from installation to running your first autonomous experiment and interpreting the results."
icon: "Rocket"
order: 2
---

## What Remoroo Actually Does

Before we dive in, here's what Remoroo handles out of the box:

| Use Case | Example Goal | Metrics |
|----------|--------------|---------|
| **ML Training** | "Train my classifier to 92% accuracy with inference < 50ms" | `accuracy >= 0.92, inference_ms < 50` |
| **Pipeline Optimization** | "Make our ETL pipeline run in under 2 seconds" | `runtime_s <= 2.0, correctness == true` |
| **Multi-Service Planners** | "Optimize all three planning services without breaking outputs" | `planner_a_runtime_s < baseline, planner_b_runtime_s < baseline, ...` |
| **Large Codebase Refactoring** | "Add type hints to all functions in the auth module" | `mypy --strict` passes |

These aren't toy problems. Remoroo navigates multi-file repos, handles tradeoffs between competing metrics, and validates results automatically. **Your project can use any languages and tools your repo already relies on** (Python, Node, Rust, shell pipelines, etc.); Remoroo runs the commands and checks the metrics you specify.

---

## Remoroo v2

**Only the v2 agent loop is available.** Older “v1” / legacy pipeline modes are not supported in current releases.

v2 is built for longer, tool-driven sessions: structured traces, run checkpoints, context budgeting and compression, stagnation detection, and a supervised execution environment (Docker or venv) with a clear split between model decisions and executed commands.

**Where to look on disk:**

- **`<repo>/.remoroo/runs/<run-id>/`** — checkpoint, trace, context views, reports, patches (best place to inspect a run).

---

## How the pieces fit together

| Piece | Role |
|-------|------|
| **`remoroo run --local`** | Your entry point: creates a run, streams progress, drives the local worker. |
| **Brain (hosted)** | Planning, model calls, and run bookkeeping. The CLI uses sensible defaults; you do **not** need to configure a Brain URL for normal use. |
| **Local worker** | Clones or copies the repo (unless `--in-place`), runs commands in Docker or venv, writes artifacts and `.remoroo` state. |
| **`remoroo worker`** | Optional long-lived worker that polls for jobs (useful for dedicated machines). |

---

## Prerequisites

Before you begin:

- **Python 3.10+** ([python.org](https://python.org)) — for the `remoroo` CLI itself
- **Git** — required; the CLI can prompt to help install it on some platforms
- **Docker** — if you use the default execution engine (`docker`); not required if you use `--engine venv`
- **Internet** — for the default hosted Brain and authentication (same as a normal `remoroo login` flow)

```bash
python --version   # 3.10+
git --version
docker --version   # if using --engine docker (default)
```

---

## Step 1: Installation

```bash
pip install remoroo
```

Verify:

```bash
remoroo --help
```

---

## Step 2: Authentication

```bash
remoroo login
```

Opens the browser to sign in. Credentials are saved under `~/.config/remoroo/`. You can also set **`REMOROO_API_KEY`** for non-interactive use.

Verify:

```bash
remoroo whoami
```

---

## Step 3: Your First Experiment

### Example: Optimize an ML Training Pipeline

Suppose you have a training script that's too slow and accuracy is borderline:

```python
# train.py (your existing code)
import torch
import torch.nn as nn
from sklearn.model_selection import train_test_split

class SimpleClassifier(nn.Module):
    def __init__(self, input_dim, hidden_dim, output_dim):
        super().__init__()
        self.fc1 = nn.Linear(input_dim, hidden_dim)
        self.fc2 = nn.Linear(hidden_dim, output_dim)
    
    def forward(self, x):
        x = torch.relu(self.fc1(x))
        return self.fc2(x)

def train_model():
    # ... your training loop
    pass

if __name__ == "__main__":
    train_model()
```

### Run Remoroo

```bash
remoroo run --local \
  --goal "Optimize the neural network to achieve accuracy >= 0.85, loss <= 0.5, training_time < 30s. Save metrics to artifacts/metrics.json." \
  --metrics "accuracy >= 0.85, loss <= 0.5, training_time < 30"
```

### What Happens

1. **Baseline**: Remoroo runs your code as-is and captures current metrics
2. **Analysis**: The v2 agent explores the repo and plans changes
3. **Iteration**: It edits the working tree (sandbox or `--in-place`), re-runs, checks metrics
4. **Validation**: Constraints must pass as you specified
5. **Result**: SUCCESS when goals are met, with a report and patch when applicable

### Expected Output

The summary panel shows **outcome**, **run id**, and **artifact path** (under `.remoroo/runs/` in your repo). A metrics table compares baseline vs final when `metrics.json` / `baseline_metrics.json` are present.

---

## Example: Autoresearch (autonomous LLM training loop)

[Autoresearch](https://github.com/karpathy/autoresearch) is Karpathy’s small project where an agent repeatedly edits `train.py`, runs a **fixed time-budget** training job, and chases a better **val_bpb** (lower is better), guided by `program.md`.

**Repos**

- **Upstream:** [github.com/karpathy/autoresearch](https://github.com/karpathy/autoresearch)
- **Remoroo fork** (Apple Silicon / MPS, Remoroo-tuned defaults and integration): [github.com/Remoroo/autoresearch](https://github.com/Remoroo/autoresearch)

**In this monorepo** we ship an **`autoresearch/`** tree (same shape: `prepare.py`, `train.py`, `program.md`) wired for Remoroo v2 — use it as a known-good integration example:

```bash
cd autoresearch   # from the Remoroo offline engine repo root
# One-time: data + tokenizer (see autoresearch/README.md — e.g. uv run prepare.py)

remoroo run --local \
  --repo . \
  --goal "Follow program.md: improve val_bpb within the configured TIME_BUDGET; only edit train.py; log experiments to results.tsv per program.md." \
  --metrics "val_bpb < 2.0"
```

Tune `--goal` / `--metrics` to match your `program.md` constraints and baseline. Long runs belong in **tmux** (see below).

---

## Example: Large Codebase Pipeline Optimization

```bash
remoroo run --local \
  --repo ./my-etl-pipeline \
  --goal "Optimize the ETL pipeline to run in under 2 seconds while maintaining correctness" \
  --metrics "runtime_s <= 2.0, correctness == true"
```

---

## Example: Optimize Multiple Planning Services

```bash
remoroo run --local \
  --repo ./planner-suite \
  --goal "Optimize all three planners without changing their outputs" \
  --metrics "planner_a_runtime_s < baseline planner_a_runtime_s, planner_b_runtime_s < baseline planner_b_runtime_s, planner_c_runtime_s < baseline planner_c_runtime_s"
```

---

## Understanding Artifacts

Each run gets a stable id. **Authoritative run outputs for that repo** live here:

```
<repo>/.remoroo/runs/<run-id>/
├── metrics.json              # Final numeric metrics (when available)
├── baseline_metrics.json     # Baseline snapshot (when available)
├── final_report.md           # What the agent did and why
├── final_patch.diff          # Apply with git apply / patch
├── system_diagram.md         # Architecture notes (when generated)
├── checkpoint.json           # v2 run checkpoint (large; for inspection / tooling)
├── trace.jsonl               # Step-by-step trace
├── run_state.json            # Run metadata
├── context_view.json         # Context debugging (optional)
└── ...                       # Other engine outputs as versions evolve
```

The CLI adds **`.remoroo/`** to `.gitignore` when it can so metadata does not clutter commits.

**Additional cache copy:** By default, the CLI also uses a per-repo cache under **`~/.cache/remoroo/runs/<repo-name>/<run-id>/`** for synchronized artifacts from the worker. Prefer **`.remoroo/runs/<run-id>/`** in the repo for day-to-day inspection unless you rely on `--out`.

### Applying the Patch

```bash
cd your-repo
git apply .remoroo/runs/<run-id>/final_patch.diff
git diff  # Review changes
```

---

## Environment variables (optional)

| Variable | Purpose |
|----------|---------|
| `REMOROO_API_KEY` | Bearer token; use instead of interactive login when needed. |
| `REMOROO_DEFAULT_ENGINE` | `docker` or `venv` if you want a non-default engine without passing `--engine`. |

**Advanced:** `REMOROO_API_URL` (or `remoroo run --brain-url`) only if you point the CLI at a **non-default** Brain deployment. Normal installs do not require this.

---

## CLI quick reference

| Command / flag | Description |
|----------------|-------------|
| `remoroo run --local` | Run on this machine. |
| `remoroo run --local --repo PATH` | Repository root (default: current directory). |
| `remoroo run --local --goal "..."` | Goal string. |
| `remoroo run --local --metrics "a, b"` | Comma-separated metrics / constraints. |
| `remoroo run --local --out PATH` | Base directory for cached artifacts (default: `~/.cache/remoroo/runs/<repo-name>`). |
| `remoroo run --local --yes` | Skip confirmations (also auto-applies patch on success when applicable). |
| `remoroo run --local --verbose` | Verbose logging. |
| `remoroo run --local --no-patch` | Do not prompt to apply `final_patch.diff`. |
| `remoroo run --local --engine docker` / `--engine venv` | Execution sandbox (default: docker, or `REMOROO_DEFAULT_ENGINE`). |
| `remoroo run --local --in-place` | Edit the repo directly instead of an isolated working copy. |
| `remoroo run --local --no-cache-env` | Disable Docker layer caching for the environment (opposite of default `--cache-env`). |
| `remoroo worker --repo PATH` | Standalone worker polling for jobs; optional `--server URL` for a non-default control plane / Brain. |
| `remoroo login` / `logout` / `whoami` | Authentication. |

**Exit codes (`remoroo run`):** `0` success; `1` failure or error; `2` **either** partial success from the session **or** you used `--remote` while hosted execution is unavailable (check the message printed to the terminal).

---

## Troubleshooting

### 1. "Could not connect to Brain Server" / health check failed

Usually a network or VPN issue blocking the default endpoint. Check connectivity; only if you **self-host** the Brain should you set `REMOROO_API_URL` or `--brain-url`.

### 2. "Docker is not running" (engine docker)

```
Cannot connect to Docker daemon
```

**Fix:** Start Docker Desktop or use `--engine venv` if appropriate for your project.

### 3. "Authentication required"

**Fix:** Run `remoroo login` or set `REMOROO_API_KEY`.

### 4. "Metric not met after max turns"

**Fixes:** Loosen or split goals, check achievability, read `final_report.md` under `.remoroo/runs/<run-id>/`.

### 5. "Patch failed to apply"

**Fix:**

```bash
git stash
git apply .remoroo/runs/<run-id>/final_patch.diff
git stash pop
```

### 6. "Timeout exceeded"

**Fixes:** Speed up verification, avoid infinite loops, shrink inputs for iteration.

---

## Running Long Experiments (ML Training, RL, etc.)

ML training, reinforcement learning, and other compute-heavy experiments can run for **hours**. Your terminal and machine need to stay alive the entire time. Here's how to set that up.

### macOS

**Problem:** Closing the terminal kills the process. Closing the laptop lid suspends it. Logging out kills it.

**Step 1: Use `tmux` (terminal survives disconnect)**

```bash
# Install tmux (one-time)
brew install tmux

# Start a named session
tmux new -s remoroo
```

Now run your experiment inside the tmux session:

```bash
remoroo run --local \
  --goal "Train a PPO agent to achieve avg_reward >= 200 on BipedalWalker-Hardcore-v3" \
  --metrics "avg_reward >= 200"
```

Detach anytime with `Ctrl+B` then `D` — the process keeps running in the background.

Reattach later:

```bash
tmux attach -t remoroo
```

**Step 2: Prevent Mac from sleeping**

Even with tmux, macOS will freeze everything when your Mac sleeps. Use `caffeinate` to prevent this:

```bash
# Inside your tmux session:
caffeinate -s remoroo run --local \
  --goal "Train a PPO agent..." \
  --metrics "avg_reward >= 200"
```

`caffeinate -s` prevents system sleep as long as the command is running (requires power adapter).

**Step 3 (optional): System Settings**

For extra safety, go to **System Settings > Energy** and:
- Set "Turn display off after" to your preference (display sleep is fine)
- Disable "Put hard disks to sleep when possible"
- Enable "Prevent automatic sleeping when the display is off" (if available)

**Quick reference (macOS):**

```bash
# Full recipe: tmux + caffeinate
tmux new -s remoroo
caffeinate -s remoroo run --local --goal "..." --metrics "..."
# Ctrl+B, D to detach. tmux attach -t remoroo to reattach.
```

### Linux (local or SSH)

```bash
# tmux (same as macOS)
tmux new -s remoroo
remoroo run --local --goal "..." --metrics "..."
# Ctrl+B, D to detach
```

No `caffeinate` needed on servers — they don't sleep. If running over SSH, tmux ensures the process survives SSH disconnections.

### Linux (remote server via SSH)

If you're running over SSH to a remote machine:

```bash
# SSH into your server
ssh user@your-server

# Start tmux (critical — without this, SSH disconnect kills everything)
tmux new -s remoroo
remoroo run --local --goal "..." --metrics "..."
# Ctrl+B, D to detach
# You can now safely close your laptop
```

Reattach after reconnecting:

```bash
ssh user@your-server
tmux attach -t remoroo
```

### What Happens If...

| Event | Without tmux | With tmux | With tmux + caffeinate |
|-------|-------------|-----------|------------------------|
| Close terminal | Process dies | Keeps running | Keeps running |
| Mac sleeps (lid close) | Process freezes | Process freezes | Keeps running |
| SSH disconnects | Process dies | Keeps running | Keeps running |
| Mac logs out | Process dies | Keeps running | Keeps running |
| Mac reboots | Process dies | Process dies | Process dies |

### Checking on a Running Experiment

While detached from tmux, you can still monitor progress:

```bash
# Reattach to see live output
tmux attach -t remoroo

# Or inspect the latest run under the repo's .remoroo directory
ls -lt ./.remoroo/runs/
tail -f ./.remoroo/runs/<run-id>/trace.jsonl
```

---

## Tips for Success

1. **Baseline-relative metrics** — `runtime_s < baseline runtime_s` is often more robust than fixed thresholds.

2. **Multi-metric goals** — Remoroo is designed for coupled constraints (accuracy *and* latency).

3. **Version control** — Work in a git repo; `.remoroo/` should stay ignored.

4. **Read the report** — `final_report.md` explains trade-offs and attempts.

5. **v2 debugging** — Use `trace.jsonl` and `context_view*.json` under `.remoroo/runs/<run-id>/` when something behaved unexpectedly.

---

## Next Steps

- [Why Remoroo?](https://www.remoroo.com/docs/1-getting-started/why-remoroo) — Use cases and philosophy
- [CLI Reference](https://www.remoroo.com/docs/4-cli-reference/reference) — Full command documentation
- [Architecture](https://www.remoroo.com/docs/2-architecture/overview) — How the engine works

Ready? Run your first experiment:

```bash
remoroo run --local
```
