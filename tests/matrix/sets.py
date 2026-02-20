"""Predefined workflow sets for the test matrix."""

from __future__ import annotations

from tests.matrix.catalog import CATALOG

# Explicitly listed sets
_SMOKE_IDS = [100.0, 500.0, 501.0, 510.0]
_INTEGRATION_IDS = [100.0, 100.1, 300.0, 350.0, 350.1, 500.0, 501.0, 510.0]


def _synthetic_ids() -> list[float]:
    return sorted(wf.wf_id for wf in CATALOG.values() if wf.sandbox_mode == "synthetic")


def _fault_ids() -> list[float]:
    return sorted(wf.wf_id for wf in CATALOG.values() if wf.fault is not None)


def _adaptive_ids() -> list[float]:
    return sorted(wf.wf_id for wf in CATALOG.values() if wf.adaptive)


def _all_ids() -> list[float]:
    return sorted(CATALOG.keys())


# Lazy-evaluated set definitions: each value is either a static list
# or a callable that returns a list (for dynamic "all synthetic" etc.).
_SET_DEFS: dict[str, list[float] | callable] = {
    "smoke": _SMOKE_IDS,
    "integration": _INTEGRATION_IDS,
    "synthetic": _synthetic_ids,
    "faults": _fault_ids,
    "adaptive": _adaptive_ids,
    "full": _all_ids,
}


def get_set(name: str) -> list[float]:
    """Return the workflow IDs for a named set, or raise KeyError."""
    defn = _SET_DEFS[name]
    if callable(defn):
        return defn()
    return list(defn)


def list_sets() -> list[str]:
    """Return all available set names."""
    return sorted(_SET_DEFS.keys())
