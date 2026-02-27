"""Tests for the SiteManager component."""

import uuid
from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock, MagicMock

import pytest

from wms2.adapters.mock import MockCRICAdapter
from wms2.config import Settings
from wms2.core.site_manager import SiteManager


def _make_settings(**overrides):
    defaults = dict(
        database_url="postgresql+asyncpg://test:test@localhost:5433/test",
    )
    defaults.update(overrides)
    return Settings(**defaults)


def _make_site_row(name="T2_US_Test"):
    row = MagicMock()
    row.name = name
    return row


def _make_ban_row(site_name="T2_US_Test", workflow_id=None, **kwargs):
    row = MagicMock()
    row.id = uuid.uuid4()
    row.site_name = site_name
    row.workflow_id = workflow_id
    row.reason = kwargs.get("reason", "test ban")
    row.failure_data = kwargs.get("failure_data")
    row.created_at = datetime.now(timezone.utc)
    row.expires_at = datetime.now(timezone.utc) + timedelta(days=7)
    row.removed_at = None
    row.removed_by = None
    for k, v in kwargs.items():
        setattr(row, k, v)
    return row


@pytest.fixture
def repo():
    r = MagicMock()
    r.get_site = AsyncMock(return_value=_make_site_row())
    r.create_site_ban = AsyncMock()
    r.get_active_bans_for_site = AsyncMock(return_value=[])
    r.get_active_system_bans = AsyncMock(return_value=[])
    r.get_active_workflow_bans = AsyncMock(return_value=[])
    r.count_active_workflow_bans_for_site = AsyncMock(return_value=0)
    r.remove_active_bans_for_site = AsyncMock(return_value=0)
    r.list_all_active_bans = AsyncMock(return_value=[])
    return r


@pytest.fixture
def settings():
    return _make_settings()


@pytest.fixture
def site_manager(repo, settings):
    return SiteManager(repo, settings)


# ── ban_site ────────────────────────────────────────────────


async def test_ban_site_creates_record(site_manager, repo):
    """ban_site calls create_site_ban with correct expires_at."""
    ban_row = _make_ban_row()
    repo.create_site_ban.return_value = ban_row

    result = await site_manager.ban_site(
        site_name="T2_US_Test",
        workflow_id=uuid.uuid4(),
        reason="High failure rate",
    )

    assert result is ban_row
    repo.create_site_ban.assert_called_once()
    call_kwargs = repo.create_site_ban.call_args[1]
    assert call_kwargs["site_name"] == "T2_US_Test"
    assert call_kwargs["reason"] == "High failure rate"
    # Default duration is 7 days
    delta = call_kwargs["expires_at"] - datetime.now(timezone.utc)
    assert 6.9 < delta.total_seconds() / 86400 < 7.1


async def test_ban_site_missing_site(site_manager, repo):
    """Site not in DB returns None with no ban created."""
    repo.get_site.return_value = None

    result = await site_manager.ban_site(
        site_name="T2_XX_Missing",
        reason="Should not ban",
    )

    assert result is None
    repo.create_site_ban.assert_not_called()


async def test_ban_site_custom_duration(site_manager, repo):
    """Custom duration_days overrides settings default."""
    ban_row = _make_ban_row()
    repo.create_site_ban.return_value = ban_row

    await site_manager.ban_site(
        site_name="T2_US_Test",
        reason="Extended ban",
        duration_days=14,
    )

    call_kwargs = repo.create_site_ban.call_args[1]
    delta = call_kwargs["expires_at"] - datetime.now(timezone.utc)
    assert 13.9 < delta.total_seconds() / 86400 < 14.1


# ── Promotion ──────────────────────────────────────────────


async def test_promotion_below_threshold(repo):
    """2 workflow bans with threshold=3 does not promote."""
    settings = _make_settings(site_ban_promotion_threshold=3)
    sm = SiteManager(repo, settings)
    ban_row = _make_ban_row()
    repo.create_site_ban.return_value = ban_row
    repo.count_active_workflow_bans_for_site.return_value = 2

    wf_id = uuid.uuid4()
    await sm.ban_site(site_name="T2_US_Test", workflow_id=wf_id, reason="test")

    # Only the initial ban should be created, no promotion
    assert repo.create_site_ban.call_count == 1


async def test_promotion_at_threshold(repo):
    """3 workflow bans with threshold=3 creates system-wide ban."""
    settings = _make_settings(site_ban_promotion_threshold=3)
    sm = SiteManager(repo, settings)
    ban_row = _make_ban_row()
    repo.create_site_ban.return_value = ban_row
    repo.count_active_workflow_bans_for_site.return_value = 3
    # No existing system-wide ban
    repo.get_active_bans_for_site.return_value = [
        _make_ban_row(workflow_id=uuid.uuid4()),
        _make_ban_row(workflow_id=uuid.uuid4()),
        _make_ban_row(workflow_id=uuid.uuid4()),
    ]

    wf_id = uuid.uuid4()
    await sm.ban_site(site_name="T2_US_Test", workflow_id=wf_id, reason="test")

    # Initial ban + promotion ban = 2 calls
    assert repo.create_site_ban.call_count == 2
    promotion_kwargs = repo.create_site_ban.call_args_list[1][1]
    assert promotion_kwargs["workflow_id"] is None
    assert "Promoted" in promotion_kwargs["reason"]


async def test_promotion_already_exists(repo):
    """No duplicate system-wide ban if one already exists."""
    settings = _make_settings(site_ban_promotion_threshold=3)
    sm = SiteManager(repo, settings)
    ban_row = _make_ban_row()
    repo.create_site_ban.return_value = ban_row
    repo.count_active_workflow_bans_for_site.return_value = 5
    # Existing system-wide ban (workflow_id=None)
    repo.get_active_bans_for_site.return_value = [
        _make_ban_row(workflow_id=None),  # system-wide
    ]

    wf_id = uuid.uuid4()
    await sm.ban_site(site_name="T2_US_Test", workflow_id=wf_id, reason="test")

    # Only the initial ban — no duplicate promotion
    assert repo.create_site_ban.call_count == 1


# ── get_banned_sites ───────────────────────────────────────


async def test_get_banned_sites_system_wide(site_manager, repo):
    """Returns system-wide banned sites."""
    repo.get_active_system_bans.return_value = [
        _make_ban_row(site_name="T2_US_Bad"),
        _make_ban_row(site_name="T2_DE_Bad"),
    ]

    result = await site_manager.get_banned_sites()

    assert result == ["T2_DE_Bad", "T2_US_Bad"]


async def test_get_banned_sites_per_workflow(site_manager, repo):
    """Returns workflow-specific banned sites."""
    wf_id = uuid.uuid4()
    repo.get_active_workflow_bans.return_value = [
        _make_ban_row(site_name="T2_US_WfBad", workflow_id=wf_id),
    ]

    result = await site_manager.get_banned_sites(workflow_id=wf_id)

    assert "T2_US_WfBad" in result


async def test_get_banned_sites_deduplicates(site_manager, repo):
    """Same site in both system and workflow bans appears once."""
    wf_id = uuid.uuid4()
    repo.get_active_system_bans.return_value = [
        _make_ban_row(site_name="T2_US_Both"),
    ]
    repo.get_active_workflow_bans.return_value = [
        _make_ban_row(site_name="T2_US_Both", workflow_id=wf_id),
    ]

    result = await site_manager.get_banned_sites(workflow_id=wf_id)

    assert result == ["T2_US_Both"]


async def test_get_banned_sites_empty(site_manager, repo):
    """No bans returns empty list."""
    result = await site_manager.get_banned_sites()
    assert result == []


# ── remove_ban / get_active_bans ───────────────────────────


async def test_remove_ban(site_manager, repo):
    """remove_ban delegates to repo and returns count."""
    repo.remove_active_bans_for_site.return_value = 2

    count = await site_manager.remove_ban("T2_US_Test", "operator")

    assert count == 2
    repo.remove_active_bans_for_site.assert_called_once_with(
        "T2_US_Test", "operator", workflow_id=None,
    )


async def test_get_active_bans(site_manager, repo):
    """get_active_bans returns ban list."""
    bans = [_make_ban_row(), _make_ban_row()]
    repo.get_active_bans_for_site.return_value = bans

    result = await site_manager.get_active_bans(site_name="T2_US_Test")

    assert len(result) == 2
    repo.get_active_bans_for_site.assert_called_once_with("T2_US_Test")


# ── sync_from_cric ────────────────────────────────────────────


async def test_sync_from_cric_no_adapter(repo, settings):
    """sync_from_cric with no adapter returns zeros."""
    sm = SiteManager(repo, settings, cric_adapter=None)

    stats = await sm.sync_from_cric()

    assert stats == {"added": 0, "updated": 0, "total": 0, "errors": 0}


async def test_sync_from_cric_adds_new_sites(repo, settings):
    """sync_from_cric upserts sites from CRIC adapter and counts additions."""
    cric = MockCRICAdapter()
    repo.get_site.return_value = None  # all sites are new
    repo.upsert_site = AsyncMock(return_value=_make_site_row())

    sm = SiteManager(repo, settings, cric_adapter=cric)
    stats = await sm.sync_from_cric()

    assert stats["added"] == 2
    assert stats["updated"] == 0
    assert stats["total"] == 2
    assert stats["errors"] == 0
    assert repo.upsert_site.call_count == 2
    assert cric.calls == [("get_sites", (), {})]


async def test_sync_from_cric_updates_existing_sites(repo, settings):
    """sync_from_cric counts updates when sites already exist."""
    cric = MockCRICAdapter()
    repo.get_site.return_value = _make_site_row()  # all sites exist
    repo.upsert_site = AsyncMock(return_value=_make_site_row())

    sm = SiteManager(repo, settings, cric_adapter=cric)
    stats = await sm.sync_from_cric()

    assert stats["added"] == 0
    assert stats["updated"] == 2
    assert stats["total"] == 2


async def test_sync_from_cric_mixed_add_update(repo, settings):
    """sync_from_cric handles a mix of new and existing sites."""
    cric = MockCRICAdapter()
    # First site is new, second exists
    repo.get_site = AsyncMock(side_effect=[None, _make_site_row()])
    repo.upsert_site = AsyncMock(return_value=_make_site_row())

    sm = SiteManager(repo, settings, cric_adapter=cric)
    stats = await sm.sync_from_cric()

    assert stats["added"] == 1
    assert stats["updated"] == 1
    assert stats["total"] == 2


async def test_sync_from_cric_handles_fetch_error(repo, settings):
    """sync_from_cric returns error count when CRIC fetch fails."""
    cric = AsyncMock()
    cric.get_sites = AsyncMock(side_effect=RuntimeError("CRIC down"))

    sm = SiteManager(repo, settings, cric_adapter=cric)
    stats = await sm.sync_from_cric()

    assert stats["errors"] == 1
    assert stats["total"] == 0


async def test_sync_from_cric_handles_upsert_error(repo, settings):
    """sync_from_cric counts per-site upsert errors without crashing."""
    cric = MockCRICAdapter()
    repo.get_site.return_value = None
    repo.upsert_site = AsyncMock(side_effect=RuntimeError("DB error"))

    sm = SiteManager(repo, settings, cric_adapter=cric)
    stats = await sm.sync_from_cric()

    assert stats["errors"] == 2
    assert stats["added"] == 0
    assert stats["total"] == 2


async def test_sync_from_cric_upsert_kwargs(repo, settings):
    """sync_from_cric passes correct site data to upsert_site."""
    cric = MockCRICAdapter()
    repo.get_site.return_value = None
    repo.upsert_site = AsyncMock(return_value=_make_site_row())

    sm = SiteManager(repo, settings, cric_adapter=cric)
    await sm.sync_from_cric()

    # Check the first upsert call has expected fields
    calls = repo.upsert_site.call_args_list
    first_kwargs = calls[0][1]
    assert first_kwargs["name"] == "T1_US_FNAL"
    assert first_kwargs["total_slots"] == 50000
    assert first_kwargs["status"] == "enabled"
