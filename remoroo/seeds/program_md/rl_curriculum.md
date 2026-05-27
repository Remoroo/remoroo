# PPO Research Loop

Autonomous research loop for improving a PPO agent on **BipedalWalkerHardcore-v3** via a **two-stage curriculum**: first learn basic locomotion on the easier **BipedalWalker-v3**, then fine-tune on the hardcore variant. **Training targets Apple Silicon using PyTorch MPS.**

## Configuration

Single source of truth for runtime parameters. Edit here; all code and docs read from this file.

| Parameter          | Value   | Description                                                    |
|--------------------|---------|----------------------------------------------------------------|
| STAGE1_ENV         | BipedalWalker-v3 | Easier env for basic locomotion                     |
| STAGE2_ENV         | BipedalWalkerHardcore-v3 | Final evaluation env                      |
| STAGE1_STEPS       | 200000  | Environment steps for Stage 1                                  |
| STAGE2_STEPS       | 6000000  | Environment steps for Stage 2                                  |
| STAGE1_BUDGET      | 360     | Wall-clock seconds for Stage 1 (6 minutes)                     |
| STAGE2_BUDGET      | 5400     | Wall-clock seconds for Stage 2 (9 minutes)                     |
| STAGE1_GOAL        | 250     | Avg reward (last 100 eps) to early-exit Stage 1                |
| STAGE2_GOAL        | 300     | Avg reward (last 100 eps) to consider Hardcore solved          |
| HIDDEN_DIM         | 256     | Network hidden layer width                                     |
| LR                 | 2.5e-4  | Initial learning rate (linear decay per stage)                 |
| N_ENVS             | 8       | Parallel vectorized environments per stage (Box2D on CPU)      |

```
STAGE1_ENV=BipedalWalker-v3
STAGE2_ENV=BipedalWalkerHardcore-v3
STAGE1_STEPS=200000
STAGE2_STEPS=300000
STAGE1_BUDGET=360
STAGE2_BUDGET=10800
STAGE1_GOAL=250
STAGE2_GOAL=300
HIDDEN_DIM=256
LR=2.5e-4
N_ENVS=8
```
<!-- Edit the block above to change runtime parameters. -->

## Two-stage curriculum rationale

**BipedalWalkerHardcore-v3** adds stumps, pitfalls, and rough terrain on top of the same walker physics as **BipedalWalker-v3**. Both share identical observation (24-D) and action (4-D, continuous [-1, 1]) spaces.

Training directly on Hardcore from scratch wastes most of the step budget just learning to stand and walk — skills the easier env teaches much faster. The two-stage approach:

1. **Stage 1 — BipedalWalker-v3** (~200k steps, 6 min): Learn stable walking gait. The agent should reach avg reward ≥ 250 (near-solved on non-hardcore). This gives the policy a working motor prior and populates the observation normalizer with meaningful statistics.

2. **Stage 2 — BipedalWalkerHardcore-v3** (~300k steps, 9 min): Fine-tune on the hard variant. The agent already walks, so it can focus its budget on learning to handle obstacles. Target: avg reward ≥ 300 (benchmark solved).

**What transfers**: model weights, optimizer state, and the running observation normalizer. The LR schedule resets per stage so Stage 2 starts with full learning rate.

**What to watch**: Stage 2 may show an initial dip as the policy encounters obstacles it hasn't seen. This is expected — if it recovers within ~50k steps, the curriculum is working.

## Apple Silicon (MPS) — primary training target

Treat MPS as a **different throughput/memory profile** from CUDA.

- **Gymnasium / Box2D** runs on **CPU** across `N_ENVS` parallel instances via `SyncVectorEnv`. The GPU handles the batched policy/value forward & backward.
- The loop is **heterogeneous**: CPU-bound (env stepping) or GPU-bound (large batches). The script logs **steps/sec** each update — use it to find the `n_envs` sweet spot (typically 8–12 on Mac, CPU-limited beyond that).
- **Unified memory** (~16 GB): large nets or rollouts can cause OOM. Shrink batch footprint before abandoning an idea.
- Set `PYTORCH_ENABLE_MPS_FALLBACK=1` (done in script) for graceful CPU fallback on unsupported ops.
- Stay **FP32** until the learning curve is stable; mixed precision on MPS is immature.

## Setup

1. **Agree on a run tag** (e.g. `apr11`). Branch `autoresearch/<tag>` must not exist.
2. **Create the branch**: `git checkout -b autoresearch/<tag>`.
3. **Read in-scope files**: `program.md` (this file) and `ppo_agent.py` (the file you modify).
4. **Check deps**: `python -c "import torch, gymnasium, numpy"`.
5. **Confirm MPS**: `python -c "import torch; print('mps', torch.backends.mps.is_available())"`.
6. **Initialize results.tsv** with header + baseline.
7. **Confirm and go**.

## Experimentation

Launch with:

```bash
uv run ppo_agent.py > run.log 2>&1
```

The script runs **both stages sequentially** and prints labeled output:

```
[Stage 1] Step  16384 | Avg Reward:   185.32 | Best:   185.32 | Loss: 0.4321 | LR: 2.30e-04 | Steps/s: 4200
[Stage 1] Done in 48s. Best avg: 248.10, Final avg: 245.00, Steps: 200000, Steps/s: 4150

[Stage 2] Step  32768 | Avg Reward:    42.50 | Best:    42.50 | Loss: 0.3210 | LR: 2.10e-04 | Steps/s: 3800
[Stage 2] Done in 79s. Best avg: 55.20, Final avg: 48.30, Steps: 300000, Steps/s: 3700

FINAL: Stage1 best=248.10  Stage2 best=55.20
```

A checkpoint (`checkpoint.pt`) is saved between stages so you can resume Stage 2 independently if needed.

Extract results:

```bash
# Final summary
tail -5 run.log | grep -E "FINAL|Stage|Goal reached"

# Best avg in each stage
grep "Avg Reward" run.log | grep "Stage 1" | awk -F': ' '{print $2}' | awk -F',' '{print $1}' | sort -n | tail -1
grep "Avg Reward" run.log | grep "Stage 2" | awk -F': ' '{print $2}' | awk -F',' '{print $1}' | sort -n | tail -1
```

**What you CAN modify** in `ppo_agent.py`:
- Network architecture, hyperparameters, optimizer, rollout config, exploration, reward shaping, observation handling, stage split, stage budgets — everything.

**What you CANNOT do**:
- Add dependencies beyond `torch`, `gymnasium`, `numpy`, `pygame`.
- Change the env reward function (no custom wrappers that alter scoring).
- Cheat on the metric — avg reward over last 100 episodes from the raw env is ground truth.

**Key metrics**: **Stage 2 best avg reward** is the headline number. Stage 1 is means to an end.

**Simplicity criterion**: simpler is better at equal performance. But the two-stage split itself is justified by evidence — don't collapse it back to single-stage unless a single-stage approach genuinely outperforms.

## Logging results

Log to `results.tsv` (tab-separated). The TSV now includes both stage results:

```
commit	s1_avg	s2_avg	steps_k	status	description
```

1. `commit` — git hash (short, 7 chars)
2. `s1_avg` — best avg reward in Stage 1 (BipedalWalker-v3)
3. `s2_avg` — best avg reward in Stage 2 (BipedalWalkerHardcore-v3) — **this is the primary metric**
4. `steps_k` — total steps (both stages) in thousands
5. `status` — `keep`, `discard`, or `crash`
6. `description` — what was tried

Example:

```
commit	s1_avg	s2_avg	steps_k	status	description
a1b2c3d	248.10	55.20	500	keep	baseline two-stage curriculum
b2c3d4e	260.30	82.50	500	keep	lower entropy to 0.0005
c3d4e5f	180.00	-20.00	500	discard	double hidden_dim to 512
d4e5f6g	0.00	0.00	0	crash	MPS error on LayerNorm
```

## The experiment loop

LOOP FOREVER:

1. Check git state and branch.
2. Read `results.tsv` — what's been tried, current best `s2_avg`.
3. **Formulate hypothesis**: what change do you expect to improve Stage 2 avg reward, and why? Consider whether the change should target Stage 1 learning, Stage 2 adaptation, or both.
4. Edit `ppo_agent.py`.
5. `git commit`.
6. Run: `uv run ppo_agent.py > run.log 2>&1`.
7. Extract: `tail -10 run.log; grep "Avg Reward" run.log | tail -5`.
8. If crash, check `tail -n 50 run.log`. Fix MPS issues with narrow fallbacks. Try up to 2 fixes before moving on.
9. Record in `results.tsv`.
10. If `s2_avg` improved → keep. Otherwise → `git reset --hard` to previous best.

**Stage-specific tuning**: You can change the step split (e.g. 150k/350k), budgets, goals, or hyperparameters independently per stage. If Stage 1 already solves walking quickly, shift more steps to Stage 2.

**Timeout**: Total wall time should stay under `STAGE1_BUDGET + STAGE2_BUDGET` (15 min). Kill if > 1.5× that.

**NEVER STOP**: Run indefinitely until manually stopped.

## Convergence recipe

### 1) Diagnose before changing

After each run, check `run.log` for:
- **Stage 1 not converging**: If Stage 1 avg is still negative, the base walking skill isn't learned — fix Stage 1 first (LR, network, rollout).
- **Stage 2 initial collapse**: Big dip at Stage 2 start is normal; if it never recovers, the policy may be too specialized to easy terrain. Try shorter Stage 1 or higher entropy in Stage 2.
- **Entropy collapse**: Policy becomes deterministic too early in either stage.
- **Value instability**: Loss spikes, reward cliffs.

### 2) Staged targets

1. **Stage 1 avg ≥ 200**: Basic walking works.
2. **Stage 1 avg ≥ 250**: Near-solved, good motor prior.
3. **Stage 2 avg > 0**: Agent survives hardcore obstacles sometimes.
4. **Stage 2 avg ≥ 100**: Solid adaptation.
5. **Stage 2 avg ≥ 300**: Benchmark solved.

### 3) Hypothesis discipline

- One primary lever per experiment when unstable.
- After stability, combine proven changes.
- **Stage-aware**: if Stage 1 is already good, focus changes on Stage 2 adaptation (e.g. Stage 2 entropy, LR, rollout).

### 4) Ordered idea buckets

1. **Stage split**: Adjust step allocation between stages based on where the bottleneck is.
2. **Trust region**: clip_ratio, epochs per rollout, minibatch size.
3. **Exploration**: entropy coef (per stage?), log_std init/bounds, action clamping.
4. **Value function**: value_coef, proper value clipping with stored old values.
5. **Returns/scaling**: gamma (higher for long-horizon hardcore?), GAE lambda.
6. **Optimization**: LR, annealing, batch size — profile MPS throughput.
7. **Capacity**: hidden_dim, separate vs shared trunk — after stability.

## Research ideas — prioritized by evidence

### Tier 0 — Quick wins (apply now)

1. **Verify no reward clipping**: Current code uses raw env rewards (no `np.clip` on rewards). Keep it this way. SB3's reference config explicitly sets `norm_reward: False`.

2. **Tune the stage split**: If Stage 1 converges quickly (e.g. in 100k steps), give Stage 2 more budget. The split is soft — the script early-exits Stage 1 if `STAGE1_GOAL` is reached.

3. **Boost Stage 2 entropy**: After Stage 1 narrows the policy for easy terrain, Stage 2 may need more exploration. Consider resetting `log_std` slightly higher at the start of Stage 2, or using a higher `entropy_coef` for Stage 2.

### Tier 1 — Proven hyperparameters

4. **Match SB3 reference structure**: `batch_size=64`, `rollout_steps=2048`, `n_epochs=10`, `ent_coef=0.001` constant, `lr=2.5e-4` linear decay, `clip_range=0.2` linear decay.

5. **Separate actor/critic** (already done in current code): No shared trunk. Policy and value heads have independent [256, 256] networks.

6. **Linear clip_range decay** alongside LR per stage.

### Tier 2 — Structural

7. **Store old values for proper value clipping** during PPO update.
8. **State-Dependent Exploration (gSDE)**: std depends on state, not a global parameter. High impact on locomotion.
9. **Higher gamma for Stage 2**: Stage 2 episodes can be longer due to obstacles. `gamma=0.995` or `0.999` for better long-horizon credit.

### Tier 3 — Exploration boost

10. **ICM (Intrinsic Curiosity Module)**: Intrinsic reward from forward-model prediction error. Published results show 300+ on Hardcore, but typically needs 5M+ steps. Could work if Stage 1 provides a strong prior.
11. **Reset log_std at Stage 2 start**: Force re-exploration. E.g. set `agent.actor_critic.log_std.data.fill_(-0.3)` before Stage 2.

### Tier 4 — MPS throughput

12. **Tune `n_envs`**: Default is 8. Try 4, 8, 12, 16 and compare steps/sec in the log. Beyond ~12 on most Macs, Box2D CPU becomes the bottleneck and steps/sec flattens or drops.
13. **torch.compile** (experimental, test last).
14. **Domain_knowledge**: Use your own domain_knowlwdge tool to find new ideas. 

### What NOT to prioritize

- Frame stacking / LSTM for 24-D flat obs.
- FP16 on MPS (immature).
- Grid searches (each run is precious at 15 min).
