"""Level 2 environment checks — local HTCondor.

Requirements: HTCondor binaries, running schedd, available slots,
Python bindings, plus all Level 1 requirements.
"""

from tests.environment.checks import (
    check_condor_binaries,
    check_condor_python_bindings,
    check_condor_schedd,
    check_condor_slots,
)


class TestLevel2:
    """Prerequisites for running HTCondor integration tests."""

    def test_condor_binaries(self):
        ok, detail = check_condor_binaries()
        assert ok, detail

    def test_condor_schedd_reachable(self):
        ok, detail = check_condor_schedd()
        assert ok, detail

    def test_condor_slots_available(self):
        ok, detail = check_condor_slots()
        assert ok, detail

    def test_condor_python_bindings(self):
        ok, detail = check_condor_python_bindings()
        assert ok, detail
