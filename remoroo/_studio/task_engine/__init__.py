"""task_engine — the shipped, task-agnostic execution substrate (Class B mechanisms).

Design source: ADC/remoroo_task_execution_engine.md; build plan: ADC/remoroo_engine_impl_plan.md.

Rules this package lives by (CI-enforced, see tests/):
  - No task nouns anywhere in this package (DEC-01). The agent is the generalizer; a task
    exists only as agent-authored artifacts in the customer's cell.
  - No imports from remoroo_brain (CON-11 / DEC-21). Orchestration/recipes live brain-side;
    this package ships MECHANISMS only: atoms over MotionStack+Bridge, perception operators,
    the Env/trial runtime, generic judging channels, reset/probe executors, the supervisor.
  - Everything here runs off-robot in the numpy venv against the fakes in tests/ (CON-01).
"""

__all__ = [
    "atoms", "params", "trace", "envelope", "env", "record", "run_trial",
]
