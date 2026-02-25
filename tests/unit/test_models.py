import pytest
from pydantic import ValidationError

from wms2.models.enums import CleanupPolicy, SplittingAlgo
from wms2.models.request import ProductionStep, RequestCreate


def _valid_request(**overrides):
    defaults = {
        "request_name": "test-request-001",
        "requestor": "testuser",
        "input_dataset": "/TestPrimary/TestProcessed/RECO",
        "cmssw_version": "CMSSW_14_0_0",
        "scram_arch": "el9_amd64_gcc12",
        "global_tag": "GT_TEST",
        "splitting_algo": SplittingAlgo.FILE_BASED,
        "campaign": "TestCampaign",
        "processing_string": "TestProc",
        "acquisition_era": "Run2024",
    }
    defaults.update(overrides)
    return defaults


def test_valid_request_creation():
    req = RequestCreate(**_valid_request())
    assert req.request_name == "test-request-001"
    assert req.priority == 100000
    assert req.urgent is False
    assert req.production_steps == []
    assert req.cleanup_policy == CleanupPolicy.KEEP_UNTIL_REPLACED


def test_request_with_production_steps():
    steps = [
        ProductionStep(fraction=0.1, priority=80000),
        ProductionStep(fraction=0.5, priority=50000),
    ]
    req = RequestCreate(**_valid_request(production_steps=steps))
    assert len(req.production_steps) == 2
    assert req.production_steps[0].fraction == 0.1


def test_urgent_and_steps_mutually_exclusive():
    steps = [ProductionStep(fraction=0.1, priority=80000)]
    with pytest.raises(ValidationError, match="mutually exclusive"):
        RequestCreate(**_valid_request(urgent=True, production_steps=steps))


def test_production_steps_fractions_must_increase():
    steps = [
        ProductionStep(fraction=0.5, priority=80000),
        ProductionStep(fraction=0.1, priority=50000),
    ]
    with pytest.raises(ValidationError, match="strictly increasing"):
        RequestCreate(**_valid_request(production_steps=steps))


def test_production_steps_priorities_must_decrease():
    steps = [
        ProductionStep(fraction=0.1, priority=50000),
        ProductionStep(fraction=0.5, priority=80000),
    ]
    with pytest.raises(ValidationError, match="strictly decreasing"):
        RequestCreate(**_valid_request(production_steps=steps))


def test_production_step_fraction_bounds():
    with pytest.raises(ValidationError):
        ProductionStep(fraction=0.0, priority=80000)
    with pytest.raises(ValidationError):
        ProductionStep(fraction=1.0, priority=80000)
    step = ProductionStep(fraction=0.5, priority=80000)
    assert step.fraction == 0.5


def test_single_production_step_is_valid():
    """Single step doesn't need ordering checks."""
    steps = [ProductionStep(fraction=0.5, priority=80000)]
    req = RequestCreate(**_valid_request(production_steps=steps))
    assert len(req.production_steps) == 1


def test_request_missing_required_fields():
    with pytest.raises(ValidationError):
        RequestCreate(request_name="test")


def test_request_adaptive_default_true():
    req = RequestCreate(**_valid_request())
    assert req.adaptive is True


def test_request_adaptive_false():
    req = RequestCreate(**_valid_request(adaptive=False))
    assert req.adaptive is False


def test_workflow_round_tracking_defaults():
    from datetime import datetime, timezone
    from uuid import uuid4

    from wms2.models.workflow import Workflow

    now = datetime.now(timezone.utc)
    wf = Workflow(
        id=uuid4(),
        request_name="test-request-001",
        created_at=now,
        updated_at=now,
    )
    assert wf.step_metrics is None
    assert wf.current_round == 0
    assert wf.next_first_event == 1
    assert wf.file_cursor == 0
