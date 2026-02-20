"""Capability detection for the test matrix.

Wraps tests/environment/checks.py — no duplication of check logic.
"""

from __future__ import annotations

from tests.environment.checks import (
    check_apptainer,
    check_condor_python_bindings,
    check_condor_schedd,
    check_condor_slots,
    check_cvmfs_mounted,
    check_dbs_reachable,
    check_postgres_reachable,
    check_reqmgr2_reachable,
    check_siteconf,
    check_x509_proxy,
)
from tests.matrix.definitions import WorkflowDef

# Each capability maps to one or more check functions.
# All checks must pass for the capability to be considered available.
CAPABILITY_CHECKS: dict[str, list] = {
    "condor": [check_condor_schedd, check_condor_slots, check_condor_python_bindings],
    "cvmfs": [check_cvmfs_mounted],
    "siteconf": [check_siteconf],
    "apptainer": [check_apptainer],
    "x509": [check_x509_proxy],
    "postgres": [check_postgres_reachable],
    "reqmgr2": [check_reqmgr2_reachable],
    "dbs": [check_dbs_reachable],
}


def detect_capabilities() -> dict[str, bool]:
    """Probe the environment and return {capability: available}."""
    caps: dict[str, bool] = {}
    for cap_name, checks in CAPABILITY_CHECKS.items():
        ok = True
        for fn in checks:
            try:
                result, _ = fn()
                if not result:
                    ok = False
                    break
            except Exception:
                ok = False
                break
        caps[cap_name] = ok
    return caps


def detect_capabilities_detailed() -> dict[str, tuple[bool, str]]:
    """Like detect_capabilities but returns (ok, detail) per capability."""
    caps: dict[str, tuple[bool, str]] = {}
    for cap_name, checks in CAPABILITY_CHECKS.items():
        details: list[str] = []
        ok = True
        for fn in checks:
            try:
                result, detail = fn()
                details.append(detail)
                if not result:
                    ok = False
            except Exception as exc:
                ok = False
                details.append(str(exc))
        caps[cap_name] = (ok, "; ".join(details))
    return caps


def workflow_can_run(wf: WorkflowDef, caps: dict[str, bool]) -> tuple[bool, str]:
    """Check whether *wf* can run given detected capabilities.

    Returns (can_run, reason).  reason is empty on success.
    """
    missing = [r for r in wf.requires if not caps.get(r, False)]
    if missing:
        return False, f"missing capabilities: {', '.join(missing)}"
    return True, ""
