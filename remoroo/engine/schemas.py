from typing import Dict, Any, Optional

# Edit schema - single schema with all fields required (OpenAI structured outputs requirement)
# Note: OpenAI structured outputs require ALL properties to be in required array when additionalProperties: False
# We validate operation-specific requirements in applier.py (some fields may be None/empty for certain operations)
# 
# Field requirements by operation (validated in applier.py):
# - replace_file: requires path, kind, replacement (non-empty), overwrite (NO line fields should be present or None)
# - create_file: requires path, kind, replacement (non-empty), overwrite (NO line fields should be present or None)
# - insert: requires path, kind, after_line (non-None), replacement (non-empty), overwrite (NO start_line/end_line)
# - delete: requires path, kind, start_line (non-None), end_line (non-None), overwrite (NO replacement)
EDIT_SCHEMA: Dict[str, Any] = {
  "type":"object",
  "required":["path","kind","start_line","end_line","after_line","replacement","overwrite"],
  "additionalProperties": False,
  "properties":{
    "path":{"type":"string"},
    "kind":{"type":"string","enum":["insert","delete","create_file","replace_file"]},
    "start_line":{"type":["integer","null"],"minimum":1},
    "end_line":{"type":["integer","null"],"minimum":1},
    "after_line":{"type":["integer","null"],"minimum":0},
    "replacement":{"type":["string","null"]},
    "overwrite":{"type":"boolean"}
  }
}

PATCH_EDIT_SCHEMA = EDIT_SCHEMA

PATCH_PROPOSAL_SCHEMA: Dict[str, Any] = {
  "type":"object",
  "required":["patch_id","kind","edits","rationale","test_plan","risk_level","suggested_timeout"],
  "additionalProperties": False,
  "properties":{
    "patch_id":{"type":"string"},
    "kind":{"type":"string","enum":["code_patch","test_patch"]},
    "edits":{"type":"array","items":EDIT_SCHEMA},
    "rationale":{"type":"array","minItems":1,"items":{"type":"string"}},
    "test_plan":{"type":"array","minItems":1,"items":{"type":"string"}},
    "risk_level":{"type":"string","enum":["low","medium","high"]},
    "suggested_timeout":{
      "type":"number",
      "description":"LLM-suggested timeout in seconds for commands in this patch (optional)"
    }
  }
}

CONVERGENCE_DECISION_SCHEMA: Dict[str, Any] = {
  "type":"object",
  "required":["should_stop","confidence","reason","extracted_metrics","convergence_parameters"],
  "additionalProperties": False,
  "properties":{
    "should_stop":{"type":"boolean","description":"Whether to stop the command early"},
    "confidence":{"type":"number","minimum":0,"maximum":1,"description":"Confidence in the decision (0-1)"},
    "reason":{"type":"string","description":"Explanation for the decision"},
    "extracted_metrics":{
      "type":"object",
      "description":"Metrics extracted from partial output (if any)",
      "additionalProperties":True
    },
    "convergence_parameters":{
      "type":"object",
      "description":"LLM-determined parameters for convergence checking (optional, for future checks)",
      "additionalProperties": False,
      "required": ["min_runtime_s", "check_interval_s", "confidence_threshold", "pattern_keywords", "success_on_convergence"],
      "properties":{
        "min_runtime_s":{"type":"number","description":"Minimum runtime before checking convergence (seconds)"},
        "check_interval_s":{"type":"number","description":"Interval between convergence checks (seconds)"},
        "confidence_threshold":{"type":"number","minimum":0,"maximum":1,"description":"Confidence threshold for stopping"},
        "pattern_keywords":{"type":"array","items":{"type":"string"},"description":"Keywords that trigger convergence checks"},
        "success_on_convergence":{"type":"boolean","description":"Whether convergence should be considered success"}
      }
    }
  }
}

TURN_REPORT_SCHEMA: Dict[str, Any] = {
  "type":"object",
  "required":[
    "turn_index","proposed_patches","applied_patches","skipped_patches","failed_patches",
    "commands","outcomes","success_signals","failure_class","metrics","syntax_errors","token_usage"
  ],
  "additionalProperties": False,
  "properties":{
    "turn_index":{"type":"integer"},
    "is_partial":{
      "type":"boolean",
      "description":"Architecture Migration: True if this is a partial report (mid-turn), False if complete (end-of-turn)"
    },
    "proposed_patches":{"type":"array","items":{"type":"string"}},
    "applied_patches":{"type":"array","items":{"type":"string"}},
    "skipped_patches":{"type":"array","items":{"type":"string"}},
    "failed_patches":{"type":"array","items":{"type":"string"}},
    "patch_application_errors":{
      "type":"array",
      "items":{
        "type":"object",
        "additionalProperties": False,
        "required": ["patch_id", "error", "file"],
        "properties":{
          "patch_id":{"type":"string"},
          "error":{"type":"string"},
          "file":{"type":"string"}
        }
      }
    },
    "commands":{
      "type":"array",
      "items":{"type":"string"},
      "description":"Architecture Migration: Can be partial (subset of all commands) if is_partial=true"
    },
    "outcomes":{
      "type":"array",
      "description":"Architecture Migration: Can be partial (subset of all outcomes) if is_partial=true"
    },
    "success_signals":{
      "type":"object",
      "required":["all_commands_ok","meets_success_criteria"],
      "additionalProperties": False,
      "properties":{
        "all_commands_ok":{"type":"boolean"},
        "meets_success_criteria":{"type":"boolean"}
      }
    },
    "failure_class":{
      "type":"string",
      "enum":["IMPORT_ERROR","DEPENDENCY_ERROR","SYNTAX_ERROR","TEST_FAILURE","METRIC_FAILURE","RUNTIME_ERROR","OOM_ERROR","TIMEOUT","COMMAND_ERROR","NO_PROGRESS","POLICY_VIOLATION","UNKNOWN"]
    },
    "metrics":{
      "type":"object",
      "required":["metrics","source","extracted_at"],
      "additionalProperties": False,
      "properties":{
        "metrics":{
          "type":"object",
          "additionalProperties": True
        },
        "source":{"type":"string"},
        "extracted_at":{"type":"string"},
        "phase":{"type":"string","enum":["baseline","current"]},
        "turn_index":{"type":"integer"},
        "run_id":{"type":["string","null"]}
      }
    },
    "syntax_errors":{
      "type":"array",
      "items":{
        "type":"object",
        "additionalProperties": False,
        "required": ["file", "error"],
        "properties":{
          "file":{"type":"string"},
          "error":{"type":"string"}
        }
      }
    },
    "token_usage":{
      "type":"object",
      "required":["total_tokens","total_cost","token_usage_by_stage","cost_by_stage","cache_hits"],
      "additionalProperties": False,
      "properties":{
        "total_tokens":{"type":"integer"},
        "total_cost":{"type":"number"},
        "token_usage_by_stage":{
          "type":"object",
          "additionalProperties": True
        },
        "cost_by_stage":{
          "type":"object",
          "additionalProperties": True
        },
        "cache_hits":{"type":"integer"}
      }
    },
    "mid_execution_judge_decision":{
      "oneOf":[
        {"type":"null"},
        {
          "type":"object",
          "required":["decision","reason"],
          "additionalProperties": True,
          "properties":{
            "decision":{
              "type":"string",
              "enum":["CONTINUE","REPLAN_NOW","SUCCESS","FAIL","ITERATE"]
            },
            "reason":{"type":"string"}
          }
        }
      ],
      "description":"Optional mid-execution judge decision if Judge was called during command execution (before end-of-turn)"
    }
  }
}

EXPERIMENT_CONTRACT_SCHEMA: Dict[str, Any] = {
  "type":"object",
  "required":["metric_specs","metric_sources","command_plan","non_retryable_conditions","history"],
  "additionalProperties": False,
  "properties":{
    "metric_specs":{
      "type":"array",
      "items":{
        "type":"object",
        "additionalProperties": False,
        "properties":{
          "name":{"type":"string","description":"Name of the metric (generic - works for any domain, e.g., 'accuracy', 'runtime', 'throughput')"},
          "direction":{"type":"string","enum":["maximize","minimize"],"description":"Whether to maximize or minimize the metric"},
          "threshold":{"type":"string","description":"Threshold specification (e.g., '>= 0.90', '<= 5.0') - optional, can be empty string if not specified"},
          "priority":{"type":"string","enum":["primary","secondary"],"description":"Priority level - primary metrics are prioritized in evaluation. Defaults to secondary if not specified"},
          "required":{"type":"boolean","description":"Whether this metric must be met for success. Defaults to false if not specified"},
          "unit":{"type":"string","description":"Unit of measurement (e.g., 'seconds', 'milliseconds', 'ms', 's', 'bytes', 'MB', 'percent', '%', 'ratio', ''). Extract from threshold or metric name if mentioned (e.g., 'runtime <= 5000ms' -> 'milliseconds'). Default to empty string if not specified."}
        },
        "required":["name","direction","threshold","priority","required","unit"]
      },
      "minItems":1,
      "description":"Array of metric specifications. Each metric gets its own spec. Supports multiple metrics from original_metric string."
    },
    "metric_sources":{
      "type":"array",
      "items":{"type":"string"},
      "description":"Where to look for metrics (e.g., 'artifacts/metrics.json:<metric_name>', 'stdout_regex:<metric_name>[:= ]([0-9.]+)', 'llm_fallback'). Replace <metric_name> with actual metric names.",
      "minItems":1
    },
    "command_plan":{
      "type":"object",
      "additionalProperties": {
        "type":"array",
        "items":{"type":"string"}
      },
      "description":"Command plan with stage names as keys and command lists as values (e.g., {'setup': ['python <file_name>.py'], 'run': ['pytest <test_file>.py']})"
    },
    "non_retryable_conditions":{
      "type":"array",
      "items":{"type":"string"},
      "description":"Conditions that should trigger FAIL (e.g., 'dataset_missing_and_download_forbidden')"
    },
    "history":{
      "type":"object",
      "additionalProperties": False,
      "description":"Historical information (failed_commands, known_missing_deps, etc.)",
      "properties":{
        "failed_commands":{"type":"array","items":{"type":"string"}},
        "known_missing_deps":{"type":"array","items":{"type":"string"}}
      },
      "required":["failed_commands","known_missing_deps"]
    },
    "comparisons":{
      "type":"array",
      "description":"Acceptance comparisons (baseline vs current, thresholds, etc.). This encodes success criteria separately from metric extraction.",
      "items":{
        "type":"object",
        "additionalProperties": False,
        "required":["id","left","op","right","required","notes"],
        "properties":{
          "id":{"type":"string","description":"Unique identifier for this comparison rule"},
          "left":{
            "type":"object",
            "additionalProperties": False,
            "required":["kind","metric","source","constant","unit"],
            "properties":{
              "kind":{"type":"string","enum":["metric"],"description":"Left operand kind (always 'metric')"},
              "metric":{"type":["string","null"],"description":"Metric name (must correspond to a metric spec name)"},
              "source":{"type":["string","null"],"enum":["current","baseline",None],"description":"Where to source the metric value from (current turn vs baseline capture)"},
              "constant":{"type":["number","string","boolean","null"],"description":"Unused for kind='metric' (must be null)"},
              "unit":{"type":"string","description":"Optional unit (informational).", "default":""}
            }
          },
          "op":{
            "type":"string",
            "enum":["lt","lte","gt","gte","eq","neq"],
            "description":"Comparison operator"
          },
          "right":{
            "type":"object",
            "additionalProperties": False,
            "required":["kind","metric","source","constant","unit"],
            "properties":{
              "kind":{"type":"string","enum":["metric","constant"],"description":"Right operand kind"},
              "metric":{"type":["string","null"],"description":"Metric name when kind='metric', else null"},
              "source":{"type":["string","null"],"enum":["current","baseline",None],"description":"Source when kind='metric', else null"},
              "constant":{"type":["number","string","boolean","null"],"description":"Constant threshold when kind='constant', else null"},
              "unit":{"type":"string","description":"Optional unit (informational).", "default":""}
            }
          },
          "required":{"type":"boolean","description":"If true, this comparison MUST be satisfied to declare SUCCESS"},
          "notes":{"type":"string","description":"Explanation or context for this comparison (can be empty string)."}
        }
      }
    }
  }
}

PATCH_INTENT_SCHEMA: Dict[str, Any] = {
  "type":"object",
  "required":["intent_id","description","target_files"],
  "additionalProperties": False,
  "properties":{
    "intent_id":{"type":"string","description":"Unique identifier for this intent"},
    "description":{"type":"string","description":"What this intent aims to achieve"},
    "target_files":{"type":"array","items":{"type":"string"},"description":"Files this intent affects","minItems":1},
    "priority":{"type":"string","enum":["high","medium","low"],"description":"Priority level (default: medium)"},
    "dependencies":{"type":"array","items":{"type":"string"},"description":"Other intent IDs this depends on (optional)"}
  }
}

PATCH_INTENT_BUNDLE_SCHEMA: Dict[str, Any] = {
  "type":"object",
  "required":["intents","target_files"],
  "additionalProperties": False,
  "properties":{
    "intents":{
      "type":"array",
      "items":PATCH_INTENT_SCHEMA,
      "minItems":1,
      "description":"Array of PatchIntent objects (micro-goals for Patcher)"
    },
    "target_files":{
      "type":"array",
      "items":{"type":"string"},
      "description":"All target files across all intents (aggregated list for convenience)"
    }
  }
}

# =============================================================================
# Instrumentation (pre-step) schemas
# =============================================================================

# NOTE: For LLM structured outputs we avoid free-form dicts where possible.
# When we do include dicts, we keep them small and schema-safe.

ENV_KV_PAIR_SCHEMA: Dict[str, Any] = {
  "type":"object",
  "required":["name","value"],
  "additionalProperties": False,
  "properties":{
    "name":{"type":"string","description":"Environment variable name."},
    "value":{"type":"string","description":"Environment variable value."}
  }
}

INSTRUMENTATION_MANIFEST_SCHEMA: Dict[str, Any] = {
  "type":"object",
  "required":[
    "version",
    "protected_files",
    "sentinel_markers",
    "metrics_phase_env_var",
    "artifact_paths",
    "notes"
  ],
  "additionalProperties": False,
  "properties":{
    "version":{"type":"integer","description":"Manifest schema version."},
    "protected_files":{"type":"array","items":{"type":"string"},"description":"Repo file paths that must not be removed/disabled by later patches."},
    "sentinel_markers":{"type":"array","items":{"type":"string"},"description":"Sentinel markers inserted into code to identify instrumentation blocks (optional)."},
    "metrics_phase_env_var":{"type":"string","description":"Env var used to distinguish baseline vs current (e.g., REMOROO_METRICS_PHASE)."},
    "artifact_paths":{
      "type":"object",
      "description":"Canonical artifact paths (relative to repo_root).",
      "additionalProperties": False,
      "required":["baseline","current","merged"],
      "properties":{
        "baseline":{"type":"string"},
        "current":{"type":"string"},
        "merged":{"type":"string"}
      }
    },
    "notes":{"type":"string","description":"Free-form notes (can be empty)."}
  }
}

METRIC_VALUE_SCHEMA: Dict[str, Any] = {
  "type":["number","string","boolean","null"],
  "description":"Scalar metric value (schema-safe)."
}

METRICS_DICT_SCHEMA: Dict[str, Any] = {
  "type":"object",
  "description":"Metric name -> scalar value.",
  "additionalProperties": METRIC_VALUE_SCHEMA
}

METRICS_WITH_UNITS_SCHEMA: Dict[str, Any] = {
  "type":"object",
  "description":"Metric name -> {value, unit, source}.",
  "additionalProperties":{
    "type":"object",
    "additionalProperties": False,
    "required":["value","unit","source"],
    "properties":{
      "value": METRIC_VALUE_SCHEMA,
      "unit":{"type":"string"},
      "source":{"type":"string"}
    }
  }
}

BASELINE_METRICS_ARTIFACT_SCHEMA: Dict[str, Any] = {
  "type":"object",
  "required":["version","phase","metrics","metrics_with_units","source","created_at"],
  "additionalProperties": True,
  "properties":{
    "version":{"type":"integer"},
    "phase":{"type":"string","enum":["baseline"]},
    "metrics": METRICS_DICT_SCHEMA,
    "metrics_with_units": METRICS_WITH_UNITS_SCHEMA,
    "source":{"type":"string","description":"How these metrics were produced (e.g., instrumented_repo)."},
    "created_at":{"type":"string","description":"ISO timestamp or human-readable string."}
  }
}

CURRENT_METRICS_ARTIFACT_SCHEMA: Dict[str, Any] = {
  "type":"object",
  "required":["version","phase","metrics","metrics_with_units","source","created_at"],
  "additionalProperties": True,
  "properties":{
    "version":{"type":"integer"},
    "phase":{"type":"string","enum":["current"]},
    "metrics": METRICS_DICT_SCHEMA,
    "metrics_with_units": METRICS_WITH_UNITS_SCHEMA,
    "source":{"type":"string"},
    "created_at":{"type":"string"}
  }
}

# Back-compat merged metrics artifact. Existing readers expect top-level "metrics".
MERGED_METRICS_ARTIFACT_SCHEMA: Dict[str, Any] = {
  "type":"object",
  "required":["metrics","source","extracted_at"],
  "additionalProperties": True,
  "properties":{
    "metrics": METRICS_DICT_SCHEMA,  # current metrics
    "baseline_metrics": METRICS_DICT_SCHEMA,
    "source":{"type":"string"},
    "extracted_at":{"type":"string"},
    "phase":{"type":"string"}
  }
}

INSTRUMENTATION_METRIC_CAPTURE_SCHEMA: Dict[str, Any] = {
  "type":"object",
  "required":["metric_name","required","phase","chosen_source","notes"],
  "additionalProperties": False,
  "properties":{
    "metric_name":{"type":"string"},
    "required":{"type":"boolean"},
    "phase":{"type":"string","enum":["baseline","current","both"]},
    "chosen_source":{"type":"string","description":"Chosen metric source identifier (from metric_sources or suggested sources)."},
    "notes":{"type":"string","description":"Free-form notes (can be empty)."}
  }
}

INSTRUMENTATION_BASELINE_CAPTURE_SCHEMA: Dict[str, Any] = {
  "type":"object",
  "required":["commands","timeout_s","env"],
  "additionalProperties": False,
  "properties":{
    "commands":{"type":"array","items":{"type":"string"}},
    "timeout_s":{"type":"number"},
    "env":{"type":"array","items": ENV_KV_PAIR_SCHEMA, "description":"Env vars to apply to baseline capture commands."}
  }
}

INSTRUMENTATION_PLAN_SCHEMA: Dict[str, Any] = {
  "type":"object",
  "required":[
    "plan_id",
    "patching_mode",
    "patch_proposal",
    "baseline_capture",
    "metric_capture",
    "instrumentation_manifest",
    "retry_guidance",
    "safety"
  ],
  "additionalProperties": False,
  "properties":{
    "plan_id":{"type":"string"},
    "patching_mode":{"type":"string","enum":["replace_file"],"description":"v1: prefer replace-file patches."},
    "patch_proposal": PATCH_PROPOSAL_SCHEMA,
    "baseline_capture": INSTRUMENTATION_BASELINE_CAPTURE_SCHEMA,
    "metric_capture":{"type":"array","items": INSTRUMENTATION_METRIC_CAPTURE_SCHEMA},
    "instrumentation_manifest": INSTRUMENTATION_MANIFEST_SCHEMA,
    "retry_guidance":{"type":"array","items":{"type":"string"}},
    "safety":{
      "type":"object",
      "required":["no_network","preserve_behavior","deterministic_best_effort"],
      "additionalProperties": False,
      "properties":{
        "no_network":{"type":"boolean"},
        "preserve_behavior":{"type":"boolean"},
        "deterministic_best_effort":{"type":"boolean"}
      }
    }
  }
}

ARTIFACT_REQUEST_SCHEMA: Dict[str, Any] = {
  "type":"object",
  "required":["target_path","format","source_artifact","analysis_type","analysis_description"],
  "additionalProperties": False,
  "properties":{
    "target_path":{
      "type":"string",
      "description":"Path where the analysis artifact should be saved (relative to repo_root)."
    },
    "format":{
      "type":"string",
      "enum":["json","csv","txt"],
      "description":"Output format for the analysis artifact (must be small and analyzable)."
    },
    "source_artifact":{
      "type":["string","null"],
      "description":"Optional: Path to a source file/artifact to analyze."
    },
    "analysis_type":{
      "type":["string","null"],
      "description":"Optional: Free-form analysis type hint (generic)."
    },
    "analysis_description":{
      "type":["string","null"],
      "description":"Optional: Free-form instructions for the requested analysis."
    }
  }
}

RECOMMENDATION_SCHEMA: Dict[str, Any] = {
  "type":"object",
  "required":["priority","description","rationale","tags","artifact_request"],
  "additionalProperties": False,
  "properties":{
    "priority":{
      "type":"string",
      "enum":["high","medium","low"],
      "description":"Priority level for addressing this recommendation."
    },
    "description":{
      "type":"string",
      "description":"Actionable recommendation describing what should be done next."
    },
    "rationale":{
      "type":"string",
      "description":"Optional: Why this recommendation is suggested (empty string if not provided)."
    },
    "tags":{
      "type":"array",
      "items":{"type":"string"},
      "description":"Optional free-form tags to help Planner/Patcher route the recommendation."
    },
    "artifact_request":{
      # IMPORTANT: OpenAI structured outputs do not permit `oneOf` in json_schema.
      # Use a nullable union type instead.
      "type":["object","null"],
      "description":"Optional request to produce a small analysis artifact via code execution.",
      "additionalProperties": False,
      "required":["target_path","format","source_artifact","analysis_type","analysis_description"],
      "properties": ARTIFACT_REQUEST_SCHEMA["properties"],
    }
  }
}

REPLAN_BOUNDARY_SCHEMA: Dict[str, Any] = {
  "type":"object",
  "required":["needs_env_doctor","why"],
  "additionalProperties": False,
  "properties":{
    "needs_env_doctor":{
      "type":"boolean",
      "description":"If true, this is a replan boundary that requires re-running EnvDoctor before continuing."
    },
    "why":{
      "type":"string",
      "description":"Free-form explanation for why EnvDoctor should be re-run."
    }
  }
}

KV_PAIR_SCHEMA: Dict[str, Any] = {
  "type":"object",
  "required":["name","value"],
  "additionalProperties": False,
  "properties":{
    "name":{"type":"string","description":"Key/name for the value (e.g., metric name or contract field name)."},
    "value":{"type":["number","string","boolean","null"],"description":"Scalar value (kept small and schema-safe)."},
  }
}

JUDGE_DECISION_SCHEMA: Dict[str, Any] = {
  "type":"object",
  # OpenAI structured outputs require that every key in properties is included in required.
  # Optional fields must be represented via nullable types (e.g., ["object","null"]) or empty arrays.
  "required":[
    "decision",
    "reason",
    "recommendations",
    "extracted_metrics",
    "contract_suggestions",
    "milestone_level", # [NEW] Architecture Migration: Partial Success Indicator
    "replan_boundary"
  ],
  "additionalProperties": False,
  "properties":{
    "decision":{
      "type":"string",
      "enum":["CONTINUE","REPLAN_NOW","SUCCESS","FAIL","ITERATE"],
      "description":"CONTINUE: mid-turn only - proceed to next command; REPLAN_NOW: mid-turn only - abort and replan; SUCCESS/FAIL: terminate; ITERATE: end-of-turn only - proceed to next turn"
    },
    "milestone_level":{
      "type":["integer", "null"],
      "minimum":0,
      "maximum":3,
      "description":"Progress Indicator: 0=None (Broken), 1=Runnable (No Crash), 2=Testable (Some Tests Pass), 3=Partially Correct (High Utility, some constraints failed)."
    },
    "reason":{
      "type":"string",
      "description":"Explanation for the decision. Include actionable recommendations here if helpful. For artifact requests, use format: 'RECOMMENDATION: Analyze <source_path> and save to <target_path> as <format> (json/csv/txt). <details>'. For mid-execution calls, you may suggest next check interval in the reason (e.g., 'Check again in 120s')."
    },
    "recommendations":{
      "type":["array", "null"],
      "items": RECOMMENDATION_SCHEMA,
      "description":"Actionable, priority-ranked recommendations to guide the Planner/Patcher. May be empty."
    },
    "extracted_metrics":{
      # IMPORTANT: OpenAI structured outputs require object schemas to have additionalProperties=false.
      # Use a list of key/value pairs instead of a free-form dict.
      "type":["array","null"],
      "description":"Optional extracted metrics as key/value pairs (generic).",
      "items": KV_PAIR_SCHEMA
    },
    "contract_suggestions":{
      # IMPORTANT: Avoid free-form dicts (OpenAI structured outputs restriction).
      # Keep this as small scalar updates only; engine applies best-effort merges.
      "type":["array","null"],
      "description":"Optional ExperimentContract suggestions as key/value scalar updates (best-effort).",
      "items": KV_PAIR_SCHEMA
    },
    "replan_boundary":{
      # IMPORTANT: OpenAI structured outputs do not permit `oneOf` in json_schema.
      # Use a nullable union type instead.
      "type":["object","null"],
      "description":"Optional boundary signal for major pivots (e.g., rerun EnvDoctor).",
      "additionalProperties": False,
      "required":["needs_env_doctor","why"],
      "properties": REPLAN_BOUNDARY_SCHEMA["properties"],
    }
  }
}

# Environment Doctor schema - minimal, fast diagnosis for environment issues
ENV_DOCTOR_SCHEMA: Dict[str, Any] = {
  "type":"object",
  "required":["diagnosis","fix_commands","confidence","is_unfixable","unfixable_reason"],
  "additionalProperties": False,
  "properties":{
    "diagnosis":{
      "type":"string",
      "description":"One sentence diagnosis of what's wrong with the environment"
    },
    "fix_commands":{
      "type":"array",
      "items":{"type":"string"},
      "description":"Shell commands to fix the issue (max 3). Use pip install, pip install -e ., etc."
    },
    "confidence":{
      "type":"string",
      "enum":["high","medium","low"],
      "description":"Confidence in the diagnosis and fix"
    },
    "is_unfixable":{
      "type":"boolean",
      "description":"True if the environment issue cannot be fixed automatically (e.g., system dependency, unsupported Python version)"
    },
    "unfixable_reason":{
      "type":["string","null"],
      "description":"If is_unfixable is true, explain why it cannot be fixed"
    }
  }
}

# Schema for goal-aware package inference
ENV_DOCTOR_GOAL_SCHEMA: Dict[str, Any] = {
  "type": "object",
  "required": ["packages", "reasoning"],
  "additionalProperties": False,
  "properties": {
    "packages": {
      "type": "array",
      "items": {"type": "string"},
      "description": "List of package names to install (without 'pip install' prefix). Empty list if no additional packages needed."
    },
    "reasoning": {
      "type": "string",
      "description": "Brief explanation of why these packages are needed for the goal"
    }
  }
}

# Schema for import-based package detection
ENV_DOCTOR_IMPORTS_SCHEMA: Dict[str, Any] = {
  "type": "object",
  "required": ["packages", "reasoning"],
  "additionalProperties": False,
  "properties": {
    "packages": {
      "type": "array",
      "items": {"type": "string"},
      "description": "List of pip package names to install. Use correct pip names (e.g., 'opencv-python' not 'cv2', 'Pillow' not 'PIL', 'scikit-learn' not 'sklearn', 'pyyaml' not 'yaml'). Empty list if no packages needed."
    },
    "reasoning": {
      "type": "string",
      "description": "Brief explanation of detected imports and why these packages are needed"
    }
  }
}


# Schema for interactive exploration (search)
EXPLORATION_REQUEST_SCHEMA: Dict[str, Any] = {
  "type": ["object", "null"],
  "description": "Optional request to search the codebase. Use ONLY when relevant files are seemingly missing from the index.",
  "additionalProperties": False,
  "required": ["tool", "query", "path"],
  "properties": {
    "tool": {
      "type": "string",
      "enum": ["grep", "find", "python_script", "toolsmith"],
      "description": "Tool to use: 'grep', 'find', 'python_script' (exec code), or 'toolsmith' (agentic investigation)."
    },
    "query": {
      "type": "string",
      "description": "Search pattern (grep/find), Python code (python_script), or Investigation Goal (toolsmith)."
    },
    "path": {
      "type": ["string", "null"],
      "description": "Optional directory path to restrict search (grep/find). Ignored for python_script."
    }
  }
}

PLANNER_OUTPUT_SCHEMA: Dict[str, Any] = {
  "type":"object",
  # OpenAI structured outputs require that every key in properties is included in required.
  # Optional fields must be represented via nullable types (e.g., ["object","null"]).
  "required":[
    "plan_id",
    "goal_restated",
    "baseline_status",
    "baseline_commands",
    "entrypoint_evidence",
    "focus_files",
    "experiment_contract",
    "patch_intent_bundle",
    "risks",
    "stop_early",
    "suggested_timeouts",
    "focus_modules",
    "focus_symbols",
    "hydration_request",
    "exploration_request"
  ],
  "additionalProperties": False,
  "properties":{
    "plan_id":{"type":"string"},
    "goal_restated":{"type":"string"},
    "baseline_status":{
      "type":["string","null"],
      "enum":["AVAILABLE","NOT_AVAILABLE",None],
      "description":"Whether a baseline can be executed BEFORE patching. AVAILABLE means the run/baseline commands reference existing runnable code. NOT_AVAILABLE means no runnable baseline exists yet and new code/entrypoint must be created first."
    },
    "baseline_commands":{
      "type":["array","null"],
      "items":{"type":"string"},
      "description":"Optional explicit baseline commands. If baseline_status=AVAILABLE, provide commands to run for baseline capture (ideally the same as the main run command). If NOT_AVAILABLE, null or empty."
    },
    "entrypoint_evidence":{
      "type":["object","null"],
      "additionalProperties": False,
      "description":"Evidence for why the chosen run/baseline entrypoint was selected. This prevents ungrounded/hallucinated paths.",
      "required":["chosen","source","why"],
      "properties":{
        "chosen":{"type":"string","description":"Chosen runnable entrypoint (file path or module)."},
        "source":{"type":"string","enum":["repo_index_summary.entrypoints","repository_structure.existing_files","planned_new_file"],"description":"Where the chosen entrypoint came from."},
        "why":{"type":"string","description":"Short justification grounded in repo context (e.g., appears in entrypoints list, contains __main__, matches goal keywords)."}
      }
    },
    "focus_files":{
      "type":"array",
      "items":{"type":"string"},
      "description":"DEPRECATED: Use patch_intent_bundle.target_files instead. Kept for backward compatibility."
    },
    "experiment_contract":{
      "description":"ExperimentContract (required on turn 0, optional on turn N+). Will be validated separately against EXPERIMENT_CONTRACT_SCHEMA.",
      "type":"object",
      "additionalProperties":False,
      "properties":{
        "metric_specs":{
          "type":"array",
          "items":{
            "type":"object",
            "additionalProperties":False,
            "properties":{
              "name":{"type":"string","description":"Name of the metric (generic - works for any domain, e.g., 'accuracy', 'runtime', 'throughput')"},
              "direction":{"type":"string","enum":["maximize","minimize"],"description":"Whether to maximize or minimize the metric"},
              "threshold":{"type":"string","description":"Threshold specification (e.g., '>= 0.90', '<= 5.0') - optional, can be empty string if not specified"},
              "priority":{"type":"string","enum":["primary","secondary"],"description":"Priority level - primary metrics are prioritized in evaluation. Defaults to secondary if not specified"},
              "required":{"type":"boolean","description":"Whether this metric must be met for success. Defaults to false if not specified"},
              "unit":{"type":"string","description":"Unit of measurement (e.g., 'seconds', 'milliseconds', 'ms', 's', 'bytes', 'MB', 'percent', '%', 'ratio', ''). Extract from threshold or metric name if mentioned (e.g., 'runtime <= 5000ms' -> 'milliseconds'). Default to empty string if not specified."}
            },
            "required":["name","direction","threshold","priority","required","unit"]
          },
          "minItems":1,
          "description":"Array of metric specifications. Each metric gets its own spec. Supports multiple metrics from original_metric string."
        },
        "metric_sources":{
          "type":"array",
          "items":{"type":"string"},
          "description":"Where to look for metrics (e.g., 'artifacts/metrics.json:<metric_name>', 'stdout_regex:<metric_name>[:= ]([0-9.]+)', 'llm_fallback'). Replace <metric_name> with actual metric names.",
          "minItems":1
        },
        "command_plan":{
          "type":["object","null"],
          "additionalProperties": {
            "type":"array",
            "items":{"type":"string"}
          },
          "description":"Command plan with stage names as keys and command lists as values"
        },
        "non_retryable_conditions":{
          "type":"array",
          "items":{"type":"string"},
          "description":"Conditions that should trigger FAIL (e.g., 'dataset_missing_and_download_forbidden')"
        },
        "history":{
          "type":"object",
          "additionalProperties": False,
          "description":"Historical information (failed_commands, known_missing_deps, etc.)",
          "properties":{
            "failed_commands":{"type":"array","items":{"type":"string"}},
            "known_missing_deps":{"type":"array","items":{"type":"string"}}
          },
          "required":["failed_commands","known_missing_deps"]
        },
        "comparisons":{
          "type":["array","null"],
          "description":"Acceptance comparisons (baseline vs current, thresholds, etc.). This encodes success criteria separately from metric extraction.",
          "items":{
            "type":"object",
            "additionalProperties": False,
            "required":["id","left","op","right","required","notes"],
            "properties":{
              "id":{"type":"string"},
              "left":{
                "type":"object",
                "additionalProperties": False,
                "required":["kind","metric","source","constant","unit"],
                "properties":{
                  "kind":{"type":"string","enum":["metric"]},
                  "metric":{"type":["string","null"]},
                  "source":{"type":["string","null"],"enum":["current","baseline",None]},
                  "constant":{"type":["number","string","boolean","null"]},
                  "unit":{"type":"string"}
                }
              },
              "op":{"type":"string","enum":["lt","lte","gt","gte","eq","neq"]},
              "right":{
                "type":"object",
                "additionalProperties": False,
                "required":["kind","metric","source","constant","unit"],
                "properties":{
                  "kind":{"type":"string","enum":["metric","constant"]},
                  "metric":{"type":["string","null"]},
                  "source":{"type":["string","null"],"enum":["current","baseline",None]},
                  "constant":{"type":["number","string","boolean","null"]},
                  "unit":{"type":"string"}
                }
              },
              "required":{"type":"boolean"},
              "notes":{"type":"string"}
            }
          }
        }
      },
      # OpenAI structured outputs require that every key in properties is included in required.
      # We keep command_plan/comparisons nullable here so later turns can keep the key present while omitting updates.
      "required":["metric_specs","metric_sources","command_plan","non_retryable_conditions","history","comparisons"]
    },
    "patch_intent_bundle":{
      "description":"PatchIntentBundle - array of micro-goals for Patcher. Will be validated separately against PATCH_INTENT_BUNDLE_SCHEMA.",
      "type":"object",
      "additionalProperties":False,
      "properties":{
        "intents":{
          "type":"array",
          "items":{
            "type":"object",
            "additionalProperties":False,
            "properties":{
              "intent_id":{"type":"string"},
              "description":{"type":"string"},
              "target_files":{"type":"array","items":{"type":"string"},"minItems":1},
              "priority":{"type":"string","enum":["high","medium","low"]},
              "dependencies":{"type":"array","items":{"type":"string"}}
            },
            "required":["intent_id","description","target_files","priority","dependencies"]
          },
          "minItems":1
        },
        "target_files":{"type":"array","items":{"type":"string"}}
      },
      "required":["intents","target_files"]
    },
    "risks":{"type":"array","items":{"type":"string"}},
    "stop_early":{"type":"boolean"},
    "suggested_timeouts":{
      "type":["object","null"],
      "additionalProperties":{"type":"number"},
      "description":"Optional per-stage timeout suggestions in seconds (stage_name -> timeout_seconds). If not provided, commands run until completion."
    },
    "focus_modules":{
      "type":["array","null"],
      "items":{"type":"string"},
      "description":"Module names to focus on (e.g., 'package.module'). Optional, for large-repo migration v2. Can be null if not needed."
    },
    "focus_symbols":{
      "type":["array","null"],
      "items":{"type":"string"},
      "description":"Symbol IDs to focus on (e.g., 'sym:pkg.mod:foo#L123'). Optional, for large-repo migration v2. Can be null if not needed."
    },
    "hydration_request":{
      "type":["object","null"],
      "description":"Optional: Request full file text for specific paths (escalation). Limited to 3 files per turn. Can be null if not needed.",
      "additionalProperties": False,
      "required":["file_paths","reasons"],
      "properties":{
        "file_paths":{
          "type":"array",
          "items":{"type":"string"},
          "maxItems": 3,
          "description":"List of file paths to request full text for (max 3)"
        },
        "reasons":{
          "type":"array",
          "items":{"type":"string"},
          "description":"Reasons for requesting each file"
        }
      }
    },
    "exploration_request": EXPLORATION_REQUEST_SCHEMA
  }
}

def normalize_experiment_contract(contract: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    """
    Normalize ExperimentContract to new format (metric_specs array).
    
    Converts old format (metric_spec: single object) to new format (metric_specs: array).
    This ensures backward compatibility with existing contracts.
    Also applies defaults for optional fields (threshold, priority, required).
    
    Args:
        contract: ExperimentContract dict, may be None or in old/new format
    
    Returns:
        Normalized contract with metric_specs array (never None)
    """
    if not contract:
        return {}
    
    # Convert old format to new format
    if "metric_spec" in contract and "metric_specs" not in contract:
        old_spec = contract.pop("metric_spec")
        # Apply defaults for optional fields
        if "threshold" not in old_spec:
            old_spec["threshold"] = ""
        if "priority" not in old_spec:
            old_spec["priority"] = "primary"
        if "required" not in old_spec:
            old_spec["required"] = True
        contract["metric_specs"] = [old_spec]
    
    # Normalize all metric_specs to ensure optional fields have defaults
    if "metric_specs" in contract:
        for i, spec in enumerate(contract["metric_specs"]):
            # Apply defaults for optional fields if missing or empty
            if "threshold" not in spec or spec.get("threshold") is None:
                spec["threshold"] = ""
            if "priority" not in spec or spec.get("priority") is None:
                # Default to primary for first metric, secondary for others
                spec["priority"] = "primary"
            if "required" not in spec or spec.get("required") is None:
                # Default based on priority
                #priority = spec.get("priority", "primary" if i == 0 else "secondary")
                spec["required"] = True #(priority == "primary")
            if "unit" not in spec or spec.get("unit") is None:
                spec["unit"] = ""  # Default to empty string if unit not specified

    # Ensure required fields have default values if missing
    if "non_retryable_conditions" not in contract:
        contract["non_retryable_conditions"] = []
    if "history" not in contract:
        contract["history"] = {
            "failed_commands": [],
            "known_missing_deps": []
        }
    # Coerce nullable fields into stable container types (common in later-turn contract_patch merges).
    if contract.get("command_plan") is None:
        contract["command_plan"] = {}
    if contract.get("comparisons") is None:
        contract["comparisons"] = []
    
    return contract


# Aliases for backward compatibility and LLM expectations
PATCH_EDIT_SCHEMA = EDIT_SCHEMA
ENV_DOCTOR_FIX_SCHEMA = ENV_DOCTOR_SCHEMA

METRICS_ARTIFACT_SCHEMA = MERGED_METRICS_ARTIFACT_SCHEMA

# More aliases for backward compatibility
VALIDATOR_OUTPUT_SCHEMA = PATCH_PROPOSAL_SCHEMA
CHECK_RESULT_SCHEMA = PATCH_PROPOSAL_SCHEMA
