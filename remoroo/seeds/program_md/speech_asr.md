# Automatic Speech Recognition (STT) — RooDojo

> **Status:** open frontier · harness locked, baseline pending.
> The agent has not yet trained on this workflow. This file specifies the
> contract so it can.

Autonomous loop for training a speech-recognition model and lowering its
**Word Error Rate** on a frozen evaluation split. Same universal contract as
the rest of RooDojo: locked harness, locked eval set, append-only
`results.tsv`, one commit per experiment.

## Single entry point

```
python run.py
```

(Implementation lands when the agent picks up this workflow. Suggested base:
a small Conformer / Wav2Vec2 head over a public eval slice.)

## Headline metric — `wer` on the LOCKED eval set

Word Error Rate on a fixed held-out split drawn from a public ASR corpus
(LibriSpeech `test-clean` candidate). Lower = better. Initial target:
`wer ≤ 5%`. Eval set is locked at the `VAL_*` constants in `run.py` and
may not be edited — touching it invalidates cross-commit comparisons.

Secondary cross-checks (do not inform the optimiser):

- `cer` — Character Error Rate on the same set.
- `latency_ms_per_s` — real-time-factor sanity check; not optimised.

## Repo layout (planned)

| File | Locked? | Purpose |
|---|---|---|
| `run.py` | `VAL_*`, eval rules | entry point: data → train → eval → log |
| `model.py` | no | the agent's playground (Conformer / Wav2Vec2 / etc.) |
| `data.py` | top half (eval-set construction, feature extraction) | corpus loading, MFCC / log-mel pipeline |
| `metrics.py` | yes | WER, CER, RTF |
| `eval_set.json` | yes | the locked utterance manifest |

Outputs: `artifacts/` (transcripts, alignment plots), `results.tsv` (append-only).

## What the agent is allowed to touch

Editable: `model.py` (architecture, encoder, decoder, language-model fusion),
training schedule, augmentations (SpecAugment etc.), beam-search params.
The feature extractor up to log-mel is locked.

Off-limits: `metrics.py`, `eval_set.json`, the feature-extraction half of
`data.py`, and the `VAL_*` constants in `run.py`.

## Loop

1. **Commit first.** Every experiment is identified by its commit.
2. **Run.** `python run.py`.
3. **Log.** Append one line to `results.tsv` regardless of outcome.
   Format: `commit  wer  cer  latency_ms_per_s  status  description`.
   `status ∈ {keep, regress, neutral, crash}`.
