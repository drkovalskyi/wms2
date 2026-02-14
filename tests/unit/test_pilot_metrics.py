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
