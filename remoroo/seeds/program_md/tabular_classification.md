# Higgs Boost — Constrained Tabular Classification

> **Status:** iterating · harness locked, baseline pending overnight run.
>
> *Beat the published Baldi-Sadowski-Whiteson (Nature Comms, 2014) deep-net
> result of **AUC 0.880** on the canonical UCI HIGGS test split.
> The original 5-min CPU-only budget has been **lifted** for this phase:
> we now allow MPS GPU and longer training (still single test split,
> still locked test rows). Memory cap remains.*

The Higgs dataset is **11 M rows × 28 features** of LHC-style high-energy
physics events: separating Higgs boson signal from background. The
Baldi 2014 paper used hours on a multi-GPU cluster to get the deep-net
result. We're going to try and beat the shallow line — and at least
graze the deep one — with one Mac core in five minutes.

## Single entry point

```
python run.py
```

`run.py` invokes `harness.py`, which:

- pins the process to **a single CPU core** (`taskset` on Linux,
  `OMP_NUM_THREADS=1 / VECLIB / MKL` env on macOS),
- imposes a **4 GB resident-memory cap** (`resource.RLIMIT_AS` on
  POSIX where supported, otherwise tracked + soft-killed),
- imposes a **3600 s training wall-clock** (SIGALRM, relaxed),
- loads the **canonical Baldi 2014 test split** — the *last 500 000
  rows* of `HIGGS.csv.gz`, never seen by training,
- calls the agent's `train.train_one_run()`,
- scores AUC on the locked test split,
- appends one row to `results.tsv`.

## Headline metric — `test_auc` on the LOCKED Baldi 2014 test split

ROC AUC against the held-out final 500 K rows. Higher = better.

| Target | Source |
|---|---|
| **0.733** | Baldi 2014 — shallow boosted decision tree baseline |
| **0.816** | Baldi 2014 — vanilla deep net (5×300, 1000 epochs, ~hours on GPU) |
| **0.880** | Baldi 2014 — deep net + low-level + high-level features |

Cross-checks (do not inform the optimiser):

- `train_auc` — overfit signal.
- `wall_clock_s` — must be ≤ 300.
- `peak_rss_mb` — must be ≤ 4096.
- `rows_seen_train` — how many rows the agent's recipe actually used.

## Locked constraints (anti-gaming)

| Constraint | Value | Enforced by |
|---|---|---|
| CPU | **1 core** | `OMP_NUM_THREADS=1`, `MKL_NUM_THREADS=1`, `OPENBLAS_NUM_THREADS=1`, `VECLIB_MAXIMUM_THREADS=1` set by `harness.py` before model imports |
| Memory | **≤ 4 GB resident** | `resource.RLIMIT_AS = 4 GiB` on POSIX; `harness._mem_watchdog()` thread polls `psutil.Process().memory_info().rss` and SIGKILLs over 4 GiB |
| Wall-clock training | **≤ 3600 s** (relaxed) | SIGALRM in `harness.py` |
| Wall-clock inference | **≤ 60 s** for 500 K rows | timer in `harness.eval_loop()` |
| Test set | last 500 000 rows of HIGGS.csv | `data.get_test_split()` (locked) |
| Train set | first 10 500 000 rows | `data.get_train_split()` (locked) — the agent may *subsample* but cannot peek into the test rows |
| Random seed | `42` | locked in `harness.py` |
| Model artefact size | ≤ 200 MB on disk | `harness.py` checks `artifact_dir` after train |

The harness asserts each violation explicitly and writes
`status ∈ {keep, regress, neutral, crash, time_exceeded, mem_exceeded,
artifact_too_large}` so every commit has a verdict.

## Repo layout

| File | Locked? | Purpose |
|---|---|---|
| `run.py` | yes | single entry point |
| `harness.py` | yes | CPU pin, memory cap, time budget, locked eval, results.tsv |
| `data.py` | yes (split, download, schema) | canonical 10.5 M / 0.5 M split |
| `metrics.py` | yes | ROC AUC, peak RSS reader |
| `setup_data.py` | yes | one-time HIGGS.csv.gz download from UCI |
| `features.py` | no | the agent's playground (engineering, scaling) |
| `model.py` | no | the agent's playground (LightGBM, XGBoost, FFN, ensembles) |
| `train.py` | no | the agent's playground (CV, early stopping, blending) |

Outputs: `artifacts/<commit>/` (model.bin, log), append to `results.tsv`.

## What the agent is allowed to touch

Editable:

- `features.py` — feature engineering on the 28-D row (interactions,
  polynomial expansion, mass-feature recombinations, target encoding).
- `model.py` — boosted-tree hyperparameters (LightGBM / XGBoost),
  small MLP, blends. Must respect 4 GB cap and 300 s budget.
- `train.py` — training loop, early stopping, k-fold CV (the test split
  is still locked and never visible).

Off-limits:

- `harness.py`, `metrics.py`, `data.py`, `setup_data.py`, the LOCKED
  constants. Touching these invalidates the run.

## Loop

1. **Commit first.** `git add -A && git commit -m "<hypothesis>"`.
2. **Run.** `python run.py`. Harness enforces budgets.
3. **Log.** Harness appends one line to `results.tsv`:
   `commit  test_auc  train_auc  wall_clock_s  peak_rss_mb  rows_seen_train  status  description`.

## Public lineage worth noting

- **Baldi, Sadowski, Whiteson — *Nature Communications* 2014** — original
  HIGGS dataset paper. Shallow BDT 0.733, deep net 0.816, deep+features
  0.880. Hours on a GPU cluster.
- **UCI ML Repository** — HIGGS dataset, 11 M × 28 features.
- **LightGBM / XGBoost benchmarks** — tabular SOTA at low cost; the
  question is how close you can push under the 4 GB / 1 CPU / 5 min line.

This benchmark trades raw scale for **constraint pressure**: SOTA-quality
tabular classification on a single Mac core in the time it takes to brew
coffee.
