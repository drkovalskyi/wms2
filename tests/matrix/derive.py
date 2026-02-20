"""Helper for creating workflow variants from a base definition."""

from __future__ import annotations

from dataclasses import asdict
from typing import Any

from tests.matrix.definitions import WorkflowDef


def derive(base: WorkflowDef, wf_id: float, title: str, **overrides: Any) -> WorkflowDef:
    """Create a new WorkflowDef by copying *base* and applying overrides.

    The *wf_id* and *title* are always required so every variant is explicit.
    """
    d = asdict(base)
    d["wf_id"] = wf_id
    d["title"] = title
    d.update(overrides)
    return WorkflowDef(**d)
