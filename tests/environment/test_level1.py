"""Level 1 environment checks — local real CMSSW workflow.

Requirements: CVMFS mounted, CMSSW releases available, valid X509 proxy,
CMS siteconf, apptainer for cross-arch execution.
"""

from tests.environment.checks import (
    check_apptainer,
    check_cmssw_release_available,
    check_cvmfs_mounted,
    check_siteconf,
    check_x509_proxy,
)


class TestLevel1:
    """Prerequisites for running real CMSSW workflows locally."""

    def test_cvmfs_mounted(self):
        ok, detail = check_cvmfs_mounted()
        assert ok, detail

    def test_cmssw_releases_available(self):
        ok, detail = check_cmssw_release_available()
        assert ok, detail

    def test_x509_proxy_valid(self):
        ok, detail = check_x509_proxy()
        assert ok, detail

    def test_siteconf_exists(self):
        ok, detail = check_siteconf()
        assert ok, detail

    def test_apptainer_available(self):
        ok, detail = check_apptainer()
        assert ok, detail
