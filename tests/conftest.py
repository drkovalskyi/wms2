import pytest
from httpx import ASGITransport, AsyncClient

from wms2.config import Settings
from wms2.main import create_app

# ---------------------------------------------------------------------------
# Environment level gating
# ---------------------------------------------------------------------------
# Run environment checks once per session. Tests marked with @pytest.mark.level1,
# @pytest.mark.level2, or @pytest.mark.level3 are auto-skipped if the corresponding
# environment checks fail.
#
# Usage:
#   pytest tests/environment/          — run only environment checks
#   pytest tests/ -m level1            — run only level-1 tests
#   pytest tests/ -m "not level2"      — skip level-2 tests
#   pytest tests/                      — run everything, auto-skip failing levels

_level_status: dict[int, tuple[bool, str]] = {}


def _check_level(level: int) -> tuple[bool, str]:
    """Run checks for a level, cache the result."""
    if level in _level_status:
        return _level_status[level]

    from tests.environment.checks import run_level_checks

    results = run_level_checks(level)
    failed = [(name, detail) for name, ok, detail in results if not ok]
    if failed:
        summary = "; ".join(f"{n}: {d}" for n, d in failed)
        _level_status[level] = (False, summary)
    else:
        _level_status[level] = (True, "all checks passed")
    return _level_status[level]


def pytest_configure(config):
    """Register level markers."""
    for lvl, desc in [
        ("level1", "Requires local CMSSW (CVMFS, X509, siteconf, apptainer)"),
        ("level2", "Requires local HTCondor (schedd, slots, Python bindings)"),
        ("level3", "Requires production CMS services (ReqMgr2, DBS, CouchDB)"),
    ]:
        config.addinivalue_line("markers", f"{lvl}: {desc}")
    # Keep existing condor marker
    config.addinivalue_line("markers", "condor: HTCondor integration tests (legacy)")


def pytest_collection_modifyitems(config, items):
    """Auto-skip tests whose level prerequisites are not met.

    Levels are cumulative: level2 requires level1, level3 requires level1+2.
    """
    level_deps = {
        "level1": [1],
        "level2": [1, 2],
        "level3": [1, 2, 3],
    }
    for item in items:
        for marker_name, required_levels in level_deps.items():
            if marker_name in item.keywords:
                for lvl in required_levels:
                    ok, detail = _check_level(lvl)
                    if not ok:
                        item.add_marker(pytest.mark.skip(
                            reason=f"Level {lvl} environment not ready: {detail}"
                        ))
                        break


# ---------------------------------------------------------------------------
# Existing fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def settings():
    return Settings(
        database_url="postgresql+asyncpg://wms2test:wms2test@localhost:5433/wms2test",
        lifecycle_cycle_interval=1,
        max_active_dags=10,
    )


@pytest.fixture
def app(settings):
    application = create_app()
    application.state.settings = settings
    return application


@pytest.fixture
async def client(app):
    async with AsyncClient(
        transport=ASGITransport(app=app),
        base_url="http://test",
    ) as ac:
        yield ac
