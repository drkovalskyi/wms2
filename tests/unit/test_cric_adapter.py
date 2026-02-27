"""Tests for the CRICClient adapter."""

import httpx
import pytest
import respx

from wms2.adapters.cric import CRICClient, _map_cric_site

# Sample CRIC response: dict keyed by site name
SAMPLE_CRIC_RESPONSE = {
    "T1_US_FNAL": {
        "name": "T1_US_FNAL",
        "facility": "US_FNAL",
        "tier_level": 1,
        "rcsite_state": "ACTIVE",
        "total_slots": 50000,
        "schedd_name": "schedd1.fnal.gov",
        "collector_host": "collector.fnal.gov",
        "se": "se.fnal.gov",
        "storage_path": "/store/",
    },
    "T2_US_MIT": {
        "name": "T2_US_MIT",
        "facility": "US_MIT",
        "tier_level": 2,
        "rcsite_state": "ACTIVE",
        "total_slots": 10000,
        "schedd_name": "schedd1.mit.edu",
        "collector_host": "collector.mit.edu",
        "se": "se.mit.edu",
        "storage_path": "/store/",
    },
    "T3_XX_Disabled": {
        "name": "T3_XX_Disabled",
        "tier_level": 3,
        "rcsite_state": "DISABLED",
    },
}

BASE_URL = "https://cric-test.example.com"


@pytest.fixture
def client():
    """Create CRICClient with a plain httpx client (no certs) for testing."""
    c = CRICClient.__new__(CRICClient)
    c._base_url = BASE_URL
    c._client = httpx.AsyncClient()
    return c


# ── Field mapping ────────────────────────────────────────────


def test_map_cric_site_active():
    """ACTIVE state maps to 'enabled' status."""
    result = _map_cric_site("T1_US_FNAL", SAMPLE_CRIC_RESPONSE["T1_US_FNAL"])
    assert result["name"] == "T1_US_FNAL"
    assert result["status"] == "enabled"
    assert result["total_slots"] == 50000
    assert result["schedd_name"] == "schedd1.fnal.gov"
    assert result["collector_host"] == "collector.fnal.gov"
    assert result["storage_element"] == "se.fnal.gov"
    assert result["storage_path"] == "/store/"
    assert result["config_data"]["tier_level"] == 1


def test_map_cric_site_disabled():
    """DISABLED state maps to 'disabled' status."""
    result = _map_cric_site("T3_XX_Disabled", SAMPLE_CRIC_RESPONSE["T3_XX_Disabled"])
    assert result["status"] == "disabled"
    assert result["total_slots"] is None
    assert result["schedd_name"] is None


def test_map_cric_site_standby():
    """STANDBY state maps to 'drain' status."""
    info = {"rcsite_state": "STANDBY", "total_slots": 100}
    result = _map_cric_site("T2_XX_Standby", info)
    assert result["status"] == "drain"


def test_map_cric_site_waiting():
    """WAITING state maps to 'drain' status."""
    info = {"rcsite_state": "WAITING"}
    result = _map_cric_site("T2_XX_Wait", info)
    assert result["status"] == "drain"


def test_map_cric_site_missing_state():
    """Missing rcsite_state defaults to 'disabled'."""
    info = {"total_slots": 5000}
    result = _map_cric_site("T2_XX_NoState", info)
    assert result["status"] == "disabled"


def test_map_cric_site_stores_full_response():
    """config_data stores the full CRIC response dict."""
    info = {"rcsite_state": "ACTIVE", "extra_field": "extra_value"}
    result = _map_cric_site("T2_XX_Extra", info)
    assert result["config_data"]["extra_field"] == "extra_value"


# ── get_sites (with mocked HTTP) ─────────────────────────────


@respx.mock
async def test_get_sites_success(client):
    """Successful fetch returns mapped site list."""
    respx.get(f"{BASE_URL}/api/cms/site/query/").mock(
        return_value=httpx.Response(200, json=SAMPLE_CRIC_RESPONSE)
    )

    sites = await client.get_sites()

    assert len(sites) == 3
    names = {s["name"] for s in sites}
    assert names == {"T1_US_FNAL", "T2_US_MIT", "T3_XX_Disabled"}


@respx.mock
async def test_get_sites_empty_response(client):
    """Empty dict from CRIC returns empty list."""
    respx.get(f"{BASE_URL}/api/cms/site/query/").mock(
        return_value=httpx.Response(200, json={})
    )

    sites = await client.get_sites()

    assert sites == []


@respx.mock
async def test_get_sites_unexpected_type(client):
    """Non-dict response returns empty list."""
    respx.get(f"{BASE_URL}/api/cms/site/query/").mock(
        return_value=httpx.Response(200, json=[])
    )

    sites = await client.get_sites()

    assert sites == []


@respx.mock
async def test_get_sites_partial_data(client):
    """Sites with minimal data are still mapped (missing fields become None)."""
    response = {
        "T2_XX_Minimal": {"rcsite_state": "ACTIVE"},
    }
    respx.get(f"{BASE_URL}/api/cms/site/query/").mock(
        return_value=httpx.Response(200, json=response)
    )

    sites = await client.get_sites()

    assert len(sites) == 1
    assert sites[0]["name"] == "T2_XX_Minimal"
    assert sites[0]["status"] == "enabled"
    assert sites[0]["total_slots"] is None


@respx.mock
async def test_get_sites_skips_non_dict_entries(client):
    """Non-dict values in the response dict are skipped."""
    response = {
        "T1_US_FNAL": SAMPLE_CRIC_RESPONSE["T1_US_FNAL"],
        "bad_entry": "not a dict",
        "another_bad": 42,
    }
    respx.get(f"{BASE_URL}/api/cms/site/query/").mock(
        return_value=httpx.Response(200, json=response)
    )

    sites = await client.get_sites()

    assert len(sites) == 1
    assert sites[0]["name"] == "T1_US_FNAL"


# ── Retry behavior ────────────────────────────────────────────


@respx.mock
async def test_get_sites_retries_on_server_error(client):
    """Retries on 500, succeeds on second attempt."""
    route = respx.get(f"{BASE_URL}/api/cms/site/query/")
    route.side_effect = [
        httpx.Response(500, text="Internal Server Error"),
        httpx.Response(200, json={"T1_US_FNAL": SAMPLE_CRIC_RESPONSE["T1_US_FNAL"]}),
    ]

    sites = await client.get_sites()

    assert len(sites) == 1
    assert route.call_count == 2


@respx.mock
async def test_get_sites_retries_on_transport_error(client):
    """Retries on connection error, succeeds on second attempt."""
    route = respx.get(f"{BASE_URL}/api/cms/site/query/")
    route.side_effect = [
        httpx.ConnectError("Connection refused"),
        httpx.Response(200, json={"T1_US_FNAL": SAMPLE_CRIC_RESPONSE["T1_US_FNAL"]}),
    ]

    sites = await client.get_sites()

    assert len(sites) == 1
    assert route.call_count == 2


@respx.mock
async def test_get_sites_raises_after_max_retries(client):
    """Raises after exhausting all retries."""
    route = respx.get(f"{BASE_URL}/api/cms/site/query/")
    route.side_effect = [
        httpx.Response(503, text="Service Unavailable"),
        httpx.Response(503, text="Service Unavailable"),
        httpx.Response(503, text="Service Unavailable"),
    ]

    with pytest.raises(httpx.HTTPStatusError):
        await client.get_sites()

    assert route.call_count == 3
