# Neural Voice Synthesis (TTS) — RooDojo

> **Status:** open frontier · harness locked, baseline pending.
> The agent has not yet trained on this workflow. This file specifies the
> contract so it can.

Autonomous loop for training a neural text-to-speech model and improving it
against a frozen evaluation set. Same universal contract as
`reinforcement-learning/ppo-bipedal-hardcore` and
`robotics/eye-in-hand-calibration`: locked harness, locked metric, append-only
`results.tsv`, every commit is one experiment.

## Single entry point

```
python run.py
```

(Implementation lands when the agent picks up this workflow. The harness shape
is fixed; the model architecture is not.)

## Headline metric — `mel_recon_loss` on the LOCKED eval set

L1 distance between predicted and ground-truth mel-spectrograms on a fixed
held-out set of 200 utterances drawn from a public TTS corpus
(LJSpeech / VCTK candidates). Lower = better. Eval set is locked at the
`VAL_*` constants in `run.py` and may not be edited — touching it invalidates
cross-commit comparisons.

Secondary cross-checks (do not inform the optimiser):

- `phoneme_error_rate` — predicted phoneme sequence vs. ground truth on
  the same eval set.
- `mos_proxy` — UTMOS-style listener-model score, sanity-only.

## Repo layout (planned)

| File | Locked? | Purpose |
|---|---|---|
| `run.py` | `VAL_*`, eval rules | entry point: data → train → eval → log |
| `model.py` | no | the agent's playground (FastSpeech-style by default) |
| `data.py` | top half (eval-set construction, mel extraction) | corpus loading, mel pipeline |
| `metrics.py` | yes | mel L1, PER, MOS proxy |
| `eval_set.json` | yes | the locked 200-utterance manifest |

Outputs: `artifacts/` (audio samples, mel diffs), `results.tsv` (append-only).

## What the agent is allowed to touch

Editable: `model.py` (architecture, attention, vocoder choice), training
schedule, optimiser, augmentations. The data pipeline up to mel extraction
is locked; everything downstream is fair game.

Off-limits: `metrics.py`, `eval_set.json`, the mel-extraction half of
`data.py`, and the `VAL_*` constants in `run.py`.

## Loop

1. **Commit first.** Every experiment is identified by its commit. No commit,
   no experiment.
2. **Run.** `python run.py`.
3. **Log.** Append one line to `results.tsv` regardless of outcome (improvement,
   regression, crash). Format:
   `commit  mel_recon_loss  phoneme_error_rate  mos_proxy  status  description`.
   `status ∈ {keep, regress, neutral, crash}`. Missing rows are bugs.
