"""Tests for PilotMetrics parsing."""

import json
import os

import pytest

from wms2.core.dag_planner import PilotMetrics


class TestPilotMetrics:
    def test_from_json_complete(self):
        data = {
            "events_per_second": 2.5,
            "memory_peak_mb": 4000,
            "output_size_per_event_kb": 75.0,
            "time_per_event_sec": 0.4,
            "cpu_efficiency": 0.92,
        }
        m = PilotMetrics.from_json(data)
        assert m.events_per_second == 2.5
        assert m.memory_peak_mb == 4000
        assert m.output_size_per_event_kb == 75.0
        assert m.time_per_event_sec == 0.4
        assert m.cpu_efficiency == 0.92

    def test_from_json_defaults(self):
        m = PilotMetrics.from_json({})
        assert m.events_per_second == 1.0
        assert m.memory_peak_mb == 2000
        assert m.output_size_per_event_kb == 50.0

    def test_from_json_partial(self):
        data = {"memory_peak_mb": 8000}
        m = PilotMetrics.from_json(data)
        assert m.memory_peak_mb == 8000
        assert m.events_per_second == 1.0  # default

    def test_from_json_old_format_has_no_steps(self):
        """Old-format reports (flat fields) produce None for steps/per_output_module."""
        data = {
            "events_per_second": 2.5,
            "memory_peak_mb": 4000,
            "output_size_per_event_kb": 75.0,
            "time_per_event_sec": 0.4,
            "cpu_efficiency": 0.92,
        }
        m = PilotMetrics.from_json(data)
        assert m.steps is None
        assert m.per_output_module is None

    def test_from_json_new_format_with_summary(self):
        """New-format reports (summary section) populate all fields."""
        data = {
            "steps": [{"name": "GEN", "events_read": 200}],
            "summary": {
                "events_per_second": 5.0,
                "peak_rss_mb": 1200,
                "output_size_per_event_kb": 25.6,
                "total_time_per_event_sec": 0.2,
                "per_output_module": {"GENoutput": {"size_per_event_kb": 25.6}},
            },
        }
        m = PilotMetrics.from_json(data)
        assert m.events_per_second == 5.0
        assert m.memory_peak_mb == 1200
        assert m.output_size_per_event_kb == 25.6
        assert m.time_per_event_sec == 0.2
        assert m.steps is not None
        assert m.per_output_module is not None

    def test_parse_from_file(self, tmp_path):
        data = {
            "events_per_second": 3.0,
            "memory_peak_mb": 3500,
            "output_size_per_event_kb": 60.0,
            "time_per_event_sec": 0.33,
            "cpu_efficiency": 0.85,
        }
        path = str(tmp_path / "pilot_metrics.json")
        with open(path, "w") as f:
            json.dump(data, f)

        from wms2.core.dag_planner import DAGPlanner

        # Use _parse_pilot_report directly (it's a regular method)
        planner = DAGPlanner.__new__(DAGPlanner)
        metrics = planner._parse_pilot_report(path)
        assert metrics.events_per_second == 3.0
        assert metrics.memory_peak_mb == 3500


class TestPilotMetricsFromRequest:
    """Tests for PilotMetrics.from_request() class method."""

    def test_from_request_basic(self):
        """Create metrics from request config fields."""
        config = {
            "time_per_event": 0.5,
            "memory_mb": 4000,
            "size_per_event": 100.0,
        }
        m = PilotMetrics.from_request(config)
        assert m.time_per_event_sec == 0.5
        assert m.memory_peak_mb == 4000
        assert m.output_size_per_event_kb == 100.0
        assert m.events_per_second == pytest.approx(2.0)

    def test_from_request_defaults(self):
        """Empty config uses sensible defaults."""
        m = PilotMetrics.from_request({})
        assert m.time_per_event_sec == 1.0
        assert m.memory_peak_mb == 2000
        assert m.output_size_per_event_kb == 50.0
        assert m.events_per_second == pytest.approx(1.0)

    def test_from_request_zero_time_per_event(self):
        """Zero time_per_event doesn't cause division by zero."""
        config = {"time_per_event": 0}
        m = PilotMetrics.from_request(config)
        # max(0, 0.001) = 0.001 → events_per_second = 1000
        assert m.events_per_second == pytest.approx(1000.0)

    def test_from_request_very_fast(self):
        """Very fast time_per_event gives high events_per_second."""
        config = {"time_per_event": 0.001}
        m = PilotMetrics.from_request(config)
        assert m.events_per_second == pytest.approx(1000.0)
