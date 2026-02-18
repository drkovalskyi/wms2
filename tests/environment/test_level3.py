"""Level 3 environment checks — production CMS services.

Requirements: ReqMgr2, DBS, CouchDB reachable via HTTPS with X509 auth.
"""

from tests.environment.checks import (
    check_couchdb_reachable,
    check_dbs_reachable,
    check_reqmgr2_reachable,
)


class TestLevel3:
    """Prerequisites for production service integration tests."""

    def test_reqmgr2_reachable(self):
        ok, detail = check_reqmgr2_reachable()
        assert ok, detail

    def test_dbs_reachable(self):
        ok, detail = check_dbs_reachable()
        assert ok, detail

    def test_couchdb_reachable(self):
        ok, detail = check_couchdb_reachable()
        assert ok, detail
