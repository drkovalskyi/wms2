"""Level 0 environment checks — unit test prerequisites.

Requirements: Python >= 3.11, all core packages, PostgreSQL reachable.
"""

import pytest

from tests.environment.checks import (
    LEVEL0_PACKAGES,
    check_package_importable,
    check_postgres_reachable,
    check_python_version,
)


class TestLevel0:
    """Prerequisites for running unit tests."""

    def test_python_version(self):
        ok, detail = check_python_version()
        assert ok, detail

    @pytest.mark.parametrize("package", LEVEL0_PACKAGES)
    def test_package_importable(self, package):
        ok, detail = check_package_importable(package)
        assert ok, detail

    def test_postgres_reachable(self):
        ok, detail = check_postgres_reachable()
        assert ok, detail
