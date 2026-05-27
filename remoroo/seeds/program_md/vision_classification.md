# CIFAR-10 Speedrun — Constrained Vision

> **Status:** iterating · harness locked, baseline pending overnight run.
>
> *Beat the line of public CIFAR-10 fast-recipes (DAWNBench / "How to train
> your ResNet" / hlb-CIFAR10) under hard time + size constraints on a
> single Apple Silicon Mac.*

Autonomous loop for a small CIFAR-10 classifier trained from scratch under
strict wall-clock and parameter-count budgets. The agent's job is to climb
from a stock baseline (~92% test accuracy) to **≥ 95%** without breaking the
budgets.

## Single entry point

```
python run.py
```

`run.py` invokes `harness.py`, which loads data, calls the agent's
`train.py`, kills the trainer at 900 s wall-clock, asserts the model has
≤ 1 M parameters, and runs the locked evaluation routine.

## Headline metric — `test_acc` on the LOCKED CIFAR-10 test set

Top-1 accuracy on the standard 10 000-image CIFAR-10 test set. No
test-time augmentation. Higher = better. Target: **≥ 95.0 %**.

Cross-checks (do not inform the optimiser):

- `train_acc` — overfitting sanity.
- `wall_clock_s` — actual training time used (must be ≤ 900).
- `n_params` — actual parameter count (must be ≤ 1 000 000).

## Locked constraints (anti-gaming)

| Constraint | Value | Enforced by |
|---|---|---|
| Hardware | Apple Silicon Mac · MPS backend (or CPU fallback) | runtime check in `harness.py` |
| Wall-clock training | ≤ **900 s** (15 min) | `harness.py` SIGKILLs trainer at 900 s |
| Model parameters | ≤ **1 000 000** (1 M) | `harness.py` calls `count_params()`, asserts |
| Test-time augmentation | not allowed | locked `harness.eval_loop()` |
| Test set | 10 000 standard CIFAR-10 test images | locked `data.py:get_test_loader()` |
| Random seed | `42` (data + init + augmentation) | locked in `harness.py` |
| Mixed precision | allowed (`torch.autocast` on MPS) | n/a |

## Repo layout

| File | Locked? | Purpose |
|---|---|---|
| `run.py` | yes | calls `harness.run()` — single entry point |
| `harness.py` | yes | budget enforcement, param counting, eval loop, results.tsv writer |
| `data.py` | yes (loader, splits, normalisation) — augmentation `train_transforms()` is editable | CIFAR-10 dataloader |
| `metrics.py` | yes | accuracy, param count, calibration |
| `model.py` | no | the agent's playground (architecture) |
| `train.py` | no | the agent's playground (optimiser, schedule, augmentations) |

Outputs: `artifacts/<commit>/` (best.pt, train_curve.json, log), append to
`results.tsv`.

## What the agent is allowed to touch

Editable:

- `model.py` — architecture (depth, width, GeLU vs ReLU, EMA, ghost convs,
  small ViT / CCT, etc.). Must keep param count ≤ 1 M.
- `train.py` — optimiser, LR schedule, batch size, mixup / cutmix, label
  smoothing, gradient clipping, EMA half-life. Must respect 900 s budget.
- `data.py:train_transforms()` — augmentation pipeline (RandAugment,
  AutoAugment, TrivialAugment, custom). Test transforms locked.

Off-limits:

- `harness.py`, `metrics.py`, the loader / split / normalisation half of
  `data.py`, the LOCKED constants (`SEED`, `WALL_CLOCK_BUDGET_S`,
  `MAX_PARAMS`, `BATCH_SIZE_LIMIT_HINT` etc.).

Editing locked files invalidates cross-commit comparisons.

## Loop

1. **Commit first.** `git add -A && git commit -m "<hypothesis>"`. Every
   experiment is identified by its commit.
2. **Run.** `python run.py`. Harness enforces budgets.
3. **Log.** Harness appends one line to `results.tsv` regardless of outcome:
   `commit  test_acc  train_acc  wall_clock_s  n_params  status  description`.
   `status ∈ {keep, regress, neutral, crash, budget_exceeded, params_exceeded}`.
   Missing rows are bugs.

## Public lineage worth noting

- DAWNBench (Stanford) — "fastest to 94 % on CIFAR-10".
- David Page, *How to train your ResNet* (myrtle.ai blog).
- tysam-code, *hlb-CIFAR10* (~3.4 s on 1×A100; 6 M params, no size cap).

This benchmark is harder than DAWNBench (param cap) and easier than
hlb-CIFAR10 (15 min not 3 s). The interesting Pareto point is
**≤ 1 M params, ≤ 15 min, ≥ 95 % accuracy on Apple Silicon**.
