# `program.md` examples — pick the closest match for your repo

This directory is a small catalog of real, working `program.md` files from
existing Remoroo autoresearch workflows. When you (the bootstrap agent)
need to author a `program.md` for a new repo, your job is:

1. **Diagnose the repo.** Read `README.md`, the top-level directory
   listing, dependency manifests (`pyproject.toml`, `requirements.txt`,
   `package.json`), and any obvious training/eval entry points
   (`train.py`, `run.py`, `finetune.py`, `scripts/*.py`, `Makefile`).
2. **Pick the closest example below.** Read it cover-to-cover. Read a
   second one if the first is only a partial match.
3. **Adapt, don't copy.** Rewrite the example into a `program.md` at the
   repo root that reflects the operator's real entry point, real metric,
   real frozen surface, and real time budget. Use `ask_human` to confirm
   anything you can't infer from the code.
4. **Commit and stop.** Single commit, message like
   `bootstrap: program.md from <example> seed`. Then call `done`. Do NOT
   start an experiment loop — that's a separate run.

## Catalog

| File | Domain | Headline metric | Closest fit when… |
|---|---|---|---|
| [`lm_pretraining.md`](./lm_pretraining.md) | Language-model pretraining | `val_bpb` (lower better) | repo has `train.py` + a tokenizer/dataloader; goal is reducing perplexity / bpb under a fixed time budget |
| [`vision_classification.md`](./vision_classification.md) | Image classification under hard time + parameter caps | `test_acc` (higher better) | CIFAR / ImageNet-style classifier, agent edits architecture + augmentation + optimiser, harness enforces wall-clock and param-count caps |
| [`tabular_classification.md`](./tabular_classification.md) | Tabular ML on a public dataset | `test_auc` (higher better) | LightGBM / XGBoost / FFN over a fixed train/test split, CPU-bound, memory cap matters |
| [`rl_curriculum.md`](./rl_curriculum.md) | Reinforcement learning with a multi-stage curriculum | per-stage avg reward (higher better) | PPO/SAC on Gym/Gymnasium envs, two or more training stages, agent tunes net + hyperparams + stage split |
| [`robotics_calibration.md`](./robotics_calibration.md) | Robotics / sim calibration | self-consistency on a locked validation pose set (lower better) | MuJoCo / pybullet workflow with a sensor pipeline, locked validation set, `X_gt` ground truth held aside |
| [`speech_asr.md`](./speech_asr.md) | Speech-to-text / ASR | `wer` (lower better) | Conformer / Wav2Vec2 / Whisper-finetune, log-mel pipeline locked, agent tunes model + decoder + LM fusion |
| [`speech_tts.md`](./speech_tts.md) | Text-to-speech / neural voice | `mel_recon_loss` (lower better) | FastSpeech / Tacotron / VITS, mel-extraction pipeline locked, agent tunes model + vocoder + schedule |

## Choosing rules of thumb

- **Look at the loss / metric the operator cares about**, not the raw
  domain. Two RL workflows with totally different envs both fit
  `rl_curriculum.md` if the agent's lever is policy + hyperparams.
- **Look at what the agent edits.** The example whose
  "What you CAN modify" section matches the real editable surface is the
  right one. If `train.py` is the only editable file, use the example
  with the same shape.
- **Look at the constraints.** Hard time / memory / param caps move you
  toward `vision_classification.md` or `tabular_classification.md` even
  if the underlying task is different.
- **No clean match?** Start from `lm_pretraining.md` — its Configuration
  block + Setup + Loop is the simplest skeleton. Drop the LM-specific
  metric and rewrite for the operator's task.

## What the staged seeds give you (and what they don't)

Every seed includes:

* `Configuration` table — single source of truth for runtime parameters.
* `Setup` steps — how a fresh experiment starts (branch, deps, baseline).
* `Experimentation` — what the agent CAN and CANNOT modify.
* `Output format` — the literal stdout shape the harness emits.
* `Logging results` — TSV columns and example rows.
* `The experiment loop` — numbered steps, including the **NEVER STOP**
  rule for autoresearch runs.

What seeds DO NOT cover (you have to fill in via `ask_human`):

* The repo's actual headline metric name and direction.
* The exact entry-point command (`python run.py`? `uv run train.py`?
  `make experiment`?).
* The locked / editable file surface (which file is the "playground"?).
* The per-experiment time budget the operator wants.
* Domain-specific notes — dialect for ASR, voice for TTS, dataset
  citations for tabular, etc.
