import uuid
from datetime import datetime, timezone

import pytest


async def test_create_and_get_request(repo):
    now = datetime.now(timezone.utc)
    row = await repo.create_request(
        request_name="test-req-001",
        requestor="testuser",
        requestor_dn="/CN=testuser",
        request_data={"cmssw_version": "CMSSW_14_0_0"},
        payload_config={},
        splitting_params={"files_per_job": 10},
        input_dataset="/TestPrimary/TestProcessed/RECO",
        campaign="TestCampaign",
        priority=100000,
        urgent=False,
        production_steps=[],
        status="new",
        status_transitions=[],
        cleanup_policy="keep_until_replaced",
        created_at=now,
        updated_at=now,
    )
    assert row.request_name == "test-req-001"

    fetched = await repo.get_request("test-req-001")
    assert fetched is not None
    assert fetched.requestor == "testuser"
    assert fetched.request_data == {"cmssw_version": "CMSSW_14_0_0"}


async def test_list_requests_by_status(repo):
    now = datetime.now(timezone.utc)
    for i, status in enumerate(["new", "new", "queued"]):
        await repo.create_request(
            request_name=f"list-test-{i}",
            requestor="testuser",
            request_data={},
            status=status,
            status_transitions=[],
            created_at=now,
            updated_at=now,
        )

    new_reqs = await repo.list_requests(status="new")
    assert len(new_reqs) == 2

    queued_reqs = await repo.list_requests(status="queued")
    assert len(queued_reqs) == 1


async def test_update_request(repo):
    now = datetime.now(timezone.utc)
    await repo.create_request(
        request_name="update-test",
        requestor="testuser",
        request_data={},
        status="new",
        status_transitions=[],
        priority=100000,
        created_at=now,
        updated_at=now,
    )

    updated = await repo.update_request("update-test", priority=200000, status="queued")
    assert updated.priority == 200000
    assert updated.status == "queued"


async def test_get_non_terminal_requests(repo):
    now = datetime.now(timezone.utc)
    for name, status in [
        ("active-1", "active"),
        ("completed-1", "completed"),
        ("failed-1", "failed"),
        ("queued-1", "queued"),
    ]:
        await repo.create_request(
            request_name=name,
            requestor="testuser",
            request_data={},
            status=status,
            status_transitions=[],
            created_at=now,
            updated_at=now,
        )

    non_terminal = await repo.get_non_terminal_requests()
    names = {r.request_name for r in non_terminal}
    assert "active-1" in names
    assert "queued-1" in names
    assert "completed-1" not in names
    assert "failed-1" not in names


async def test_create_workflow_and_get_by_request(repo):
    now = datetime.now(timezone.utc)
    await repo.create_request(
        request_name="wf-test",
        requestor="testuser",
        request_data={},
        status="new",
        status_transitions=[],
        created_at=now,
        updated_at=now,
    )

    wf = await repo.create_workflow(
        request_name="wf-test",
        splitting_algo="FileBased",
        status="new",
        created_at=now,
        updated_at=now,
    )
    assert wf.request_name == "wf-test"

    fetched = await repo.get_workflow_by_request("wf-test")
    assert fetched is not None
    assert fetched.id == wf.id


async def test_count_active_dags(repo):
    now = datetime.now(timezone.utc)
    await repo.create_request(
        request_name="dag-test",
        requestor="testuser",
        request_data={},
        status="active",
        status_transitions=[],
        created_at=now,
        updated_at=now,
    )
    wf = await repo.create_workflow(
        request_name="dag-test",
        status="active",
        created_at=now,
        updated_at=now,
    )

    for status in ["submitted", "running", "completed"]:
        await repo.create_dag(
            workflow_id=wf.id,
            dag_file_path="/tmp/test.dag",
            submit_dir="/tmp/submit",
            total_nodes=100,
            status=status,
            created_at=now,
        )

    count = await repo.count_active_dags()
    assert count == 2  # submitted + running


async def test_upsert_site(repo):
    now = datetime.now(timezone.utc)
    site = await repo.upsert_site(
        name="T1_US_FNAL",
        total_slots=50000,
        status="enabled",
        updated_at=now,
    )
    assert site.name == "T1_US_FNAL"
    assert site.total_slots == 50000

    # Upsert again with different slots
    updated = await repo.upsert_site(
        name="T1_US_FNAL",
        total_slots=60000,
        status="enabled",
        updated_at=now,
    )
    assert updated.total_slots == 60000

    fetched = await repo.get_site("T1_US_FNAL")
    assert fetched.total_slots == 60000


async def test_jsonb_status_transitions(repo):
    now = datetime.now(timezone.utc)
    await repo.create_request(
        request_name="jsonb-test",
        requestor="testuser",
        request_data={},
        status="new",
        status_transitions=[],
        created_at=now,
        updated_at=now,
    )

    transitions = [
        {"from": "new", "to": "submitted", "timestamp": now.isoformat()},
        {"from": "submitted", "to": "queued", "timestamp": now.isoformat()},
    ]
    await repo.update_request("jsonb-test", status_transitions=transitions)

    fetched = await repo.get_request("jsonb-test")
    assert len(fetched.status_transitions) == 2
    assert fetched.status_transitions[0]["from"] == "new"
