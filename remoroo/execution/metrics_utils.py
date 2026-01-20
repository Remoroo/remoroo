from __future__ import annotations

from typing import Any, Dict, List, Optional


def unwrap_metrics_dict(obj: Any) -> Dict[str, Any]:
    """
    Normalize a metrics payload into a dict of metric_name -> scalar.

    Supports:
    - A-pattern: {"metrics": {...}, ...}
    - legacy/raw: {"runtime_s": 1.2, "accuracy": 0.9, ...}

    Returns {} for non-dicts or unsupported shapes.
    """
    if not isinstance(obj, dict):
        return {}

    metrics = obj.get("metrics")
    if isinstance(metrics, dict):
        return metrics

    # Legacy/raw: accept scalar-ish values at top level.
    out: Dict[str, Any] = {}
    for k, v in obj.items():
        if not isinstance(k, str) or not k:
            continue
        if k in {"source", "extracted_at", "created_at", "phase", "turn_index", "run_id", "baseline_metrics", "metrics_with_units"}:
            continue
        if isinstance(v, (int, float, bool, str)) or v is None:
            out[k] = v
    return out


def build_metrics_with_units(
    metrics: Dict[str, Any],
    metric_specs: Optional[List[Dict[str, Any]]] = None,
    *,
    default_unit: str = "",
    default_source: str = "engine",
) -> Dict[str, Dict[str, Any]]:
    """
    Build a metrics_with_units map using units declared in metric_specs.

    Returns:
      {metric_name: {"value": <raw>, "unit": <unit>, "source": <source>}}
    """
    if not isinstance(metrics, dict):
        return {}

    unit_by_name: Dict[str, str] = {}
    if isinstance(metric_specs, list):
        for spec in metric_specs:
            if isinstance(spec, dict) and spec.get("name"):
                unit_by_name[str(spec["name"])] = str(spec.get("unit") or "")

    out: Dict[str, Dict[str, Any]] = {}
    for name, val in metrics.items():
        if not isinstance(name, str) or not name:
            continue
        unit = unit_by_name.get(name, default_unit)
        out[name] = {
            "value": val,
            "unit": unit,
            "source": default_source,
        }
    return out

