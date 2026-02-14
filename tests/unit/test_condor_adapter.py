"""Unit tests for HTCondorAdapter — htcondor2 calls are mocked."""

from unittest.mock import MagicMock, mock_open, patch

import pytest


@pytest.fixture
def mock_htcondor2():
    """Patch htcondor2 module before importing the adapter."""
    mock_mod = MagicMock()
    # Setup Collector → locate → Schedd chain
    mock_schedd_ad = {"Name": "test-schedd"}
    mock_mod.Collector.return_value.locate.return_value = mock_schedd_ad
    mock_mod.Schedd.return_value = MagicMock()
    mock_mod.DaemonType.Schedd = "Schedd"
    mock_mod.JobAction.Remove = "Remove"
    return mock_mod


@pytest.fixture
def adapter(mock_htcondor2):
    with patch.dict("sys.modules", {"htcondor2": mock_htcondor2}):
        from wms2.adapters.condor import HTCondorAdapter

        adapter = HTCondorAdapter("localhost:9618")
    adapter._mock = mock_htcondor2
    return adapter


async def test_submit_dag(adapter):
    mock_result = MagicMock()
    mock_result.cluster.return_value = 99999
    adapter._schedd.submit.return_value = mock_result

    with patch.dict("sys.modules", {"htcondor2": adapter._mock}):
        cluster_id, schedd = await adapter.submit_dag("/tmp/test.dag")

    assert cluster_id == "99999"
    assert schedd == "test-schedd"
    adapter._mock.Submit.from_dag.assert_called_once_with("/tmp/test.dag")


async def test_submit_job(adapter):
    mock_result = MagicMock()
    mock_result.cluster.return_value = 12345
    adapter._schedd.submit.return_value = mock_result

    m = mock_open(read_data="universe = vanilla\nqueue 1\n")
    with patch("builtins.open", m), patch.dict("sys.modules", {"htcondor2": adapter._mock}):
        cluster_id, schedd = await adapter.submit_job("/tmp/test.sub")

    assert cluster_id == "12345"
    adapter._mock.Submit.assert_called_once()


async def test_query_job_found(adapter):
    mock_ad = MagicMock()
    mock_ad.keys.return_value = ["ClusterId", "JobStatus"]
    mock_ad.__getitem__ = lambda self, k: {"ClusterId": 100, "JobStatus": 2}[k]
    adapter._schedd.query.return_value = [mock_ad]

    result = await adapter.query_job("test-schedd", "100")
    assert result is not None
    assert result["ClusterId"] == 100
    assert result["JobStatus"] == 2


async def test_query_job_not_found(adapter):
    adapter._schedd.query.return_value = []

    result = await adapter.query_job("test-schedd", "999")
    assert result is None


async def test_check_job_completed_still_running(adapter):
    mock_ad = MagicMock()
    mock_ad.__getitem__ = lambda self, k: 2  # Running
    adapter._schedd.query.return_value = [mock_ad]

    result = await adapter.check_job_completed("100", "test-schedd")
    assert result is False


async def test_check_job_completed_in_history(adapter):
    adapter._schedd.query.return_value = []  # not in queue

    mock_ad = MagicMock()
    mock_ad.__getitem__ = lambda self, k: 4  # Completed
    adapter._schedd.history.return_value = [mock_ad]

    result = await adapter.check_job_completed("100", "test-schedd")
    assert result is True


async def test_check_job_completed_gone_from_history(adapter):
    adapter._schedd.query.return_value = []
    adapter._schedd.history.return_value = []  # not found anywhere

    result = await adapter.check_job_completed("100", "test-schedd")
    assert result is True


async def test_remove_job(adapter):
    await adapter.remove_job("test-schedd", "100")
    adapter._schedd.act.assert_called_once()


async def test_ping_schedd_success(adapter):
    adapter._schedd.query.return_value = []
    result = await adapter.ping_schedd("test-schedd")
    assert result is True


async def test_ping_schedd_failure(adapter):
    adapter._schedd.query.side_effect = Exception("connection refused")
    result = await adapter.ping_schedd("test-schedd")
    assert result is False
