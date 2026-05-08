"""Runtime helpers for the Macro Flux briefing generator."""

import json
import os
from pathlib import Path


def env_int(name, default, min_value=None, max_value=None):
    """Read an integer environment variable with bounds and a safe default."""
    raw = os.environ.get(name, "")
    try:
        value = int(raw) if raw else default
    except ValueError:
        value = default
    if min_value is not None:
        value = max(min_value, value)
    if max_value is not None:
        value = min(max_value, value)
    return value


def artifact_dir():
    """Return the directory used for GitHub Actions debug artifacts."""
    return Path(os.environ.get("RUN_ARTIFACTS_DIR", "run_artifacts"))


def write_text_artifact(filename, content):
    """Write a text debug artifact. Failures are non-fatal."""
    try:
        path = artifact_dir() / filename
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(content, encoding="utf-8")
    except OSError:
        pass


def write_json_artifact(filename, payload):
    """Write a JSON debug artifact. Failures are non-fatal."""
    try:
        text = json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True)
    except TypeError:
        text = json.dumps({"error": "payload not JSON serializable"}, indent=2)
    write_text_artifact(filename, text + "\n")
