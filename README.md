---
title: "Your First Experiment"
description: "A complete walkthrough: from installation to running your first autonomous experiment and interpreting the results."
icon: "Rocket"
order: 2
---

> [!IMPORTANT]
> **Remoroo is Python-only for now.** All code and experiments must be written in Python. JavaScript, TypeScript, and other language support is coming soon.

## What Remoroo Actually Does

Before we dive in, here's what Remoroo handles out of the box:

| Use Case | Example Goal | Metrics |
|----------|--------------|---------|
| **ML Training** | "Train my classifier to 92% accuracy with inference < 50ms" | `accuracy >= 0.92, inference_ms < 50` |
| **Pipeline Optimization** | "Make our ETL pipeline run in under 2 seconds" | `runtime_s <= 2.0, correctness == true` |
| **Multi-Service Planners** | "Optimize all three planning services without breaking outputs" | `planner_a_runtime_s < baseline, planner_b_runtime_s < baseline, ...` |
| **Large Codebase Refactoring** | "Add type hints to all functions in the auth module" | `mypy --strict` passes |

These aren't toy problems. Remoroo navigates multi-file repos, handles tradeoffs between competing metrics, and validates results automatically.

---

## Prerequisites

Before you begin:

- **Python 3.10+** ([python.org](https://python.org))
- **Docker** running ([docker.com](https://docker.com)) — for sandboxed execution
- **Git** — Remoroo works best in version-controlled repos

```bash
python --version   # 3.10+
docker --version   # Any recent version
git --version
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

Opens browser to sign in. Credentials saved to `~/.config/remoroo/credentials`.

Verify:

```bash
remoroo whoami
```

---

## Step 3: Your First Experiment

Let's run a real optimization — not a toy example.

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
2. **Analysis**: The agent identifies bottlenecks (learning rate, architecture, batch size)
3. **Iteration**: It patches `train.py`, runs again, checks metrics
4. **Validation**: All three constraints must pass — not just one
5. **Result**: SUCCESS if all metrics met, with a clean patch to apply

### Expected Output

```
╭──────────────── Run Summary ────────────────╮
│ SUCCESS                                     │
│ Run ID: 20260203-143022-ml-training         │
│ Artifacts: runs/20260203-143022-ml-training │
╰─────────────────────────────────────────────╯

📈 Detailed Performance
┏━━━━━━━━━━━━━━━━┳━━━━━━━━━━━┳━━━━━━━━━┳━━━━━━━━━━┓
┃ Metric         ┃ Baseline  ┃ Final   ┃ Progress ┃
┡━━━━━━━━━━━━━━━━╇━━━━━━━━━━━╇━━━━━━━━━╇━━━━━━━━━━┩
│ accuracy       │ 0.72      │ 0.87    │ +0.15    │
│ loss           │ 0.81      │ 0.42    │ -0.39    │
│ training_time  │ 45.2      │ 22.1    │ -23.1    │
└────────────────┴───────────┴─────────┴──────────┘

📄 Report: final_report.md
🩹 Clean Patch: final_patch.diff
```

---

## Example: Large Codebase Pipeline Optimization

For multi-file codebases:

```bash
remoroo run --local \
  --repo ./my-etl-pipeline \
  --goal "Optimize the ETL pipeline to run in under 2 seconds while maintaining correctness" \
  --metrics "runtime_s <= 2.0, correctness == true"
```

The agent will:
- Navigate your entire codebase
- Identify slow modules (tokenization, feature building, I/O)
- Patch multiple files in a single run
- Verify both runtime AND correctness

---

## Example: Optimize Multiple Planning Services

When you have interdependent services:

```bash
remoroo run --local \
  --repo ./planner-suite \
  --goal "Optimize all three planners without changing their outputs" \
  --metrics "planner_a_runtime_s < baseline planner_a_runtime_s, planner_b_runtime_s < baseline planner_b_runtime_s, planner_c_runtime_s < baseline planner_c_runtime_s"
```

Remoroo automatically:
- Runs baseline to capture current performance
- Compares final metrics against baseline
- Ensures no metric regresses

---

## Understanding Artifacts

Every run creates:

```
runs/<run-id>/
├── metrics.json           # Final metric values
├── baseline_metrics.json  # Before changes
├── final_report.md        # What the agent did and why
├── final_patch.diff       # Apply with: git apply final_patch.diff
├── system_diagram.md      # Codebase architecture (for complex repos)
└── artifacts/             # Files your code generates
    └── metrics.json       # (if your code writes here)
```

### Applying the Patch

After a successful run:

```bash
cd your-repo
git apply runs/<run-id>/final_patch.diff
git diff  # Review changes
```

---

## CLI Quick Reference

| Command | Description |
|---------|-------------|
| `remoroo run --local` | Run locally (Docker sandbox) |
| `remoroo run --local --repo PATH` | Specify repository |
| `remoroo run --local --goal "..."` | Set goal directly |
| `remoroo run --local --metrics "..."` | Set metrics (comma-separated) |
| `remoroo run --local --yes` | Skip confirmations |
| `remoroo run --local --verbose` | Debug logging |
| `remoroo run --local --no-patch` | Don't apply patch automatically |
| `remoroo login` | Authenticate |
| `remoroo whoami` | Check auth status |

---

## Troubleshooting

### 1. "Docker is not running"

```
Cannot connect to Docker daemon
```

**Fix:** Start Docker Desktop or:
```bash
sudo systemctl start docker  # Linux
```

### 2. "Authentication required"

**Fix:** Run `remoroo login` and complete browser flow.

### 3. "Metric not met after max turns"

The agent couldn't satisfy your constraints.

**Fixes:**
- Check if the metric is actually achievable
- Simplify goals (optimize one thing at a time first)
- Review `final_report.md` to understand what was tried

### 4. "Patch failed to apply"

Your working directory has conflicts.

**Fix:**
```bash
git stash
git apply final_patch.diff
git stash pop
```

### 5. "Timeout exceeded"

**Fixes:**
- Ensure verification command runs quickly
- Check for infinite loops
- Reduce dataset/input sizes for faster iteration

---

## Tips for Success

1. **Use Baseline-Relative Metrics**: `runtime_s < baseline runtime_s` is more robust than hardcoded thresholds.

2. **Multi-Metric = Real Problems**: Don't simplify to single metrics. Real constraints (accuracy AND speed) are what Remoroo handles best.

3. **Version Control**: Always run in a git repo. `git diff` and `git checkout .` are your safety net.

4. **Check the Report**: `final_report.md` explains the agent's reasoning — essential for understanding trade-offs.

5. **Start with Your Actual Code**: Remoroo shines on real codebases, not synthetic examples.

---

## Next Steps

- [Why Remoroo?](/docs/1-getting-started/why-remoroo) — Use cases and philosophy
- [CLI Reference](/docs/4-cli-reference/reference) — Full command documentation
- [Architecture](/docs/2-architecture) — How the engine works

Ready? Run your first experiment:

```bash
remoroo run --local
```
