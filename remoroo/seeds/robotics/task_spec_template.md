# Task & eval SPEC — `remoroo_cell/task_spec.md` (Phase 7 → G7)

At setup, the evaluation is a **spec the operator reads and approves — NOT
code**. You author `remoroo_cell/task_spec.md` describing the eventual task,
the headline metric, what the eval will measure, and the reset/success
approach. Write **no** `eval/` code now. (Code comes after R&D hardening, when
things are deliberately locked.)

This keeps setup honest about its bar: setup proves **autonomous safe motion**,
not task performance. The task spec just records intent so the operator and a
later `@robot_sprint` share one definition.

## Fill this template with the operator (ask_human to confirm, then approve)

```markdown
# Task spec — <cell_id>

> Status: DRAFT → operator approves at G7. Versioned + editable (R&D).

## 1. Task
<One paragraph: what the robot will eventually be asked to do in this cell.
Plain language. e.g. "Pick the bin's parts and place them on the fixture.">

## 2. Headline metric
- Name: <e.g. mean_success / mean_lift_m / cycle_time_s>
- Direction: <maximize | minimize>
- Target (aspirational): <value, if known — ok to leave TBD>
- How it's measured (described, not coded): <e.g. "fraction of attempts where
  the part ends inside the fixture tolerance, judged by depth + perception">

## 3. What the eval will measure (later)
- Episodes per evaluation: <N, TBD ok>
- Per-episode success definition: <observable condition>
- Failure taxonomy to log: <e.g. missed grasp, collision-abort, drop>

## 4. Reset & success detection
- Reset between episodes: <autonomous? operator-assisted? describe>
- Success detection approach: <perception / fixture sensor / brain scope tool>
- Safety during eval: <same supervisor envelope; E-stop; operator presence?>

## 5. Out of scope for setup
- Setup does NOT execute this task. Setup proves autonomous SAFE MOTION only.
- This spec is intent for the night/`@robot_sprint`, not a setup gate to pass
  by doing the task.

## 6. Open questions
<Anything the operator still needs to decide. ask_human sparingly.>
```

## G7 exit criteria

- [ ] `remoroo_cell/task_spec.md` exists and is filled (TBDs allowed where the
      operator hasn't decided).
- [ ] The operator has **explicitly approved** it via `ask_human` (this is the
      gate — a human read-and-approve, not an automated check).
- [ ] No eval code was written.

Record approval (who/when) in `setup_report.md`.
