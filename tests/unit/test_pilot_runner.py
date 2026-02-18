"""Tests for pilot_runner: FJR parsing, summary computation, script generation."""

import json
import os
import textwrap

import pytest

from wms2.core.pilot_runner import compute_summary, parse_fjr, parse_pilot_report, write_pilot_script


# ── Sample FJR XML fragments ──────────────────────────────────────

BASIC_FJR = textwrap.dedent("""\
<?xml version="1.0" encoding="UTF-8"?>
<FrameworkJobReport>
  <InputFile>
    <LFN>/store/mc/file1.root</LFN>
    <PFN>root://xrd/store/mc/file1.root</PFN>
    <EventsRead>200</EventsRead>
  </InputFile>
  <File>
    <LFN>/store/mc/output.root</LFN>
    <PFN>{pfn}</PFN>
    <ModuleLabel>GENoutput</ModuleLabel>
    <TotalEvents>200</TotalEvents>
  </File>
  <PerformanceReport>
    <PerformanceSummary Metric="Timing">
      <Metric Name="TotalJobTime" Value="45.2"/>
    </PerformanceSummary>
    <PerformanceSummary Metric="ApplicationMemory">
      <Metric Name="PeakValueRss" Value="1200.0"/>
    </PerformanceSummary>
  </PerformanceReport>
</FrameworkJobReport>
""")

MULTI_OUTPUT_FJR = textwrap.dedent("""\
<?xml version="1.0" encoding="UTF-8"?>
<FrameworkJobReport>
  <InputFile>
    <LFN>/store/mc/input.root</LFN>
    <PFN>root://xrd/store/mc/input.root</PFN>
    <EventsRead>200</EventsRead>
  </InputFile>
  <File>
    <LFN>/store/mc/sim_output.root</LFN>
    <PFN>{pfn1}</PFN>
    <ModuleLabel>SIMoutput</ModuleLabel>
    <TotalEvents>180</TotalEvents>
  </File>
  <File>
    <LFN>/store/mc/digi_output.root</LFN>
    <PFN>{pfn2}</PFN>
    <ModuleLabel>DIGIoutput</ModuleLabel>
    <TotalEvents>150</TotalEvents>
  </File>
  <PerformanceReport>
    <PerformanceSummary Metric="Timing">
      <Metric Name="TotalJobTime" Value="120.5"/>
    </PerformanceSummary>
    <PerformanceSummary Metric="ApplicationMemory">
      <Metric Name="PeakValueRss" Value="1800.0"/>
    </PerformanceSummary>
  </PerformanceReport>
</FrameworkJobReport>
""")

MINIMAL_FJR = textwrap.dedent("""\
<?xml version="1.0" encoding="UTF-8"?>
<FrameworkJobReport>
  <InputFile>
    <LFN>/store/mc/input.root</LFN>
    <PFN>root://xrd/store/mc/input.root</PFN>
    <EventsRead>100</EventsRead>
  </InputFile>
  <File>
    <LFN>/store/mc/output.root</LFN>
    <PFN>/nonexistent/output.root</PFN>
    <ModuleLabel>RAWoutput</ModuleLabel>
    <TotalEvents>100</TotalEvents>
  </File>
</FrameworkJobReport>
""")


# ── FJR Parsing Tests ─────────────────────────────────────────────


class TestParseFjr:
    def test_parse_fjr_basic(self, tmp_path):
        """Single output module with events, timing, memory."""
        # Create a fake output file so size can be read
        out_file = tmp_path / "output.root"
        out_file.write_bytes(b"\x00" * 5242880)  # 5 MB

        xml_path = tmp_path / "report.xml"
        xml_path.write_text(BASIC_FJR.format(pfn=str(out_file)))

        result = parse_fjr(str(xml_path))

        assert result["events_read"] == 200
        assert len(result["output_modules"]) == 1
        assert result["output_modules"][0]["module"] == "GENoutput"
        assert result["output_modules"][0]["events_written"] == 200
        assert result["output_modules"][0]["size_bytes"] == 5242880
        assert result["wall_time_sec"] == pytest.approx(45.2)
        assert result["peak_rss_kb"] == 1228800

    def test_parse_fjr_multiple_outputs(self, tmp_path):
        """Two output modules with different event counts (filtering)."""
        out1 = tmp_path / "sim_output.root"
        out1.write_bytes(b"\x00" * 52428800)  # 50 MB
        out2 = tmp_path / "digi_output.root"
        out2.write_bytes(b"\x00" * 10485760)  # 10 MB

        xml_path = tmp_path / "report.xml"
        xml_path.write_text(MULTI_OUTPUT_FJR.format(
            pfn1=str(out1), pfn2=str(out2)
        ))

        result = parse_fjr(str(xml_path))

        assert result["events_read"] == 200
        assert len(result["output_modules"]) == 2

        sim_mod = result["output_modules"][0]
        assert sim_mod["module"] == "SIMoutput"
        assert sim_mod["events_written"] == 180
        assert sim_mod["size_bytes"] == 52428800

        digi_mod = result["output_modules"][1]
        assert digi_mod["module"] == "DIGIoutput"
        assert digi_mod["events_written"] == 150
        assert digi_mod["size_bytes"] == 10485760

    def test_parse_fjr_missing_fields(self, tmp_path):
        """Graceful defaults for missing timing/memory."""
        xml_path = tmp_path / "report.xml"
        xml_path.write_text(MINIMAL_FJR)

        result = parse_fjr(str(xml_path))

        assert result["events_read"] == 100
        assert result["wall_time_sec"] == 0.0
        assert result["peak_rss_kb"] == 0
        # PFN points to nonexistent file — size should be 0
        assert result["output_modules"][0]["size_bytes"] == 0

    def test_parse_fjr_file_sizes(self, tmp_path):
        """File sizes obtained via os.path.getsize on PFN."""
        out_file = tmp_path / "output.root"
        out_file.write_bytes(b"\x00" * 1024)

        xml_path = tmp_path / "report.xml"
        xml_path.write_text(BASIC_FJR.format(pfn=str(out_file)))

        result = parse_fjr(str(xml_path))
        assert result["output_modules"][0]["size_bytes"] == 1024


# ── Summary Computation Tests ─────────────────────────────────────


class TestComputeSummary:
    def test_compute_summary_single_step(self):
        """One step — summary matches step data."""
        steps = [{
            "events_read": 200,
            "wall_time_sec": 40.0,
            "peak_rss_kb": 1228800,
            "output_modules": [
                {"module": "GENoutput", "events_written": 200, "size_bytes": 5242880}
            ],
        }]
        summary = compute_summary(steps, initial_events=200)

        # time_per_event = 40/200 = 0.2
        assert summary["total_time_per_event_sec"] == pytest.approx(0.2)
        # events_per_second = 1/0.2 = 5.0
        assert summary["events_per_second"] == pytest.approx(5.0)
        # peak_rss_mb = 1228800 / 1024 = 1200.0
        assert summary["peak_rss_mb"] == pytest.approx(1200.0)
        # output_size_per_event_kb = (5242880 / 1024) / 200 = 25.6
        assert summary["output_size_per_event_kb"] == pytest.approx(25.6)

    def test_compute_summary_chain_with_filtering(self):
        """3-step chain with filtering — uses initial_events as denominator."""
        steps = [
            {
                "events_read": 200,
                "wall_time_sec": 40.0,
                "peak_rss_kb": 1024000,
                "output_modules": [
                    {"module": "GENoutput", "events_written": 200, "size_bytes": 5242880}
                ],
            },
            {
                "events_read": 200,
                "wall_time_sec": 300.0,
                "peak_rss_kb": 2048000,
                "output_modules": [
                    {"module": "SIMoutput", "events_written": 180, "size_bytes": 52428800}
                ],
            },
            {
                "events_read": 180,
                "wall_time_sec": 36.0,
                "peak_rss_kb": 512000,
                "output_modules": [
                    {"module": "DIGIoutput", "events_written": 150, "size_bytes": 10485760}
                ],
            },
        ]
        summary = compute_summary(steps, initial_events=200)

        # total_time_per_event = 40/200 + 300/200 + 36/180 = 0.2 + 1.5 + 0.2 = 1.9
        assert summary["total_time_per_event_sec"] == pytest.approx(1.9, rel=1e-3)
        # peak across steps = 2048000 / 1024 = 2000.0
        assert summary["peak_rss_mb"] == pytest.approx(2000.0)
        # output from last step / initial_events = (10485760/1024)/200 = 51.2
        assert summary["output_size_per_event_kb"] == pytest.approx(51.2)

    def test_compute_summary_per_output_module(self):
        """Multiple output modules tracked separately in summary."""
        steps = [{
            "events_read": 200,
            "wall_time_sec": 100.0,
            "peak_rss_kb": 1000000,
            "output_modules": [
                {"module": "SIMoutput", "events_written": 180, "size_bytes": 52428800},
                {"module": "DIGIoutput", "events_written": 150, "size_bytes": 10485760},
            ],
        }]
        summary = compute_summary(steps, initial_events=200)

        pom = summary["per_output_module"]
        assert "SIMoutput" in pom
        assert "DIGIoutput" in pom
        # SIMoutput: filtering_ratio = 180/200 = 0.9
        assert pom["SIMoutput"]["filtering_ratio"] == pytest.approx(0.9)
        # DIGIoutput: filtering_ratio = 150/200 = 0.75
        assert pom["DIGIoutput"]["filtering_ratio"] == pytest.approx(0.75)
        # SIMoutput: size_per_event_kb = (52428800/1024)/200 = 256.0
        assert pom["SIMoutput"]["size_per_event_kb"] == pytest.approx(256.0)

    def test_compute_summary_empty(self):
        """Empty step list returns zero summary."""
        summary = compute_summary([], initial_events=200)
        assert summary["total_time_per_event_sec"] == 0.0
        assert summary["events_per_second"] == 0.0


# ── Script Generation Tests ───────────────────────────────────────


class TestWritePilotScript:
    def test_pilot_script_has_timeout_handling(self, tmp_path):
        """Script contains subprocess timeout logic."""
        script_path = tmp_path / "wms2_pilot.py"
        write_pilot_script(str(script_path))
        content = script_path.read_text()

        assert "TimeoutExpired" in content or "timeout" in content
        assert "subprocess.Popen" in content

    def test_pilot_script_no_retry_loop(self, tmp_path):
        """Script does NOT contain retry loop (simplified pilot)."""
        script_path = tmp_path / "wms2_pilot.py"
        write_pilot_script(str(script_path))
        content = script_path.read_text()

        assert "retry_fraction" not in content
        assert "min_events" not in content

    def test_pilot_script_has_fjr_parsing(self, tmp_path):
        """Script contains XML parsing for FrameworkJobReport."""
        script_path = tmp_path / "wms2_pilot.py"
        write_pilot_script(str(script_path))
        content = script_path.read_text()

        assert "xml.etree.ElementTree" in content
        assert "FrameworkJobReport" in content or "parse_fjr" in content

    def test_pilot_script_is_executable(self, tmp_path):
        """Script has executable permission."""
        script_path = tmp_path / "wms2_pilot.py"
        write_pilot_script(str(script_path))

        assert os.access(str(script_path), os.X_OK)

    def test_pilot_script_has_shebang(self, tmp_path):
        """Script starts with Python shebang."""
        script_path = tmp_path / "wms2_pilot.py"
        write_pilot_script(str(script_path))
        content = script_path.read_text()

        assert content.startswith("#!/usr/bin/env python3")

    def test_pilot_script_has_timeout_arg(self, tmp_path):
        """Script accepts --timeout argument."""
        script_path = tmp_path / "wms2_pilot.py"
        write_pilot_script(str(script_path))
        content = script_path.read_text()

        assert "--timeout" in content
        assert "--initial-events" in content

    def test_pilot_script_writes_partial_on_failure(self, tmp_path):
        """Script writes partial results on step failure."""
        script_path = tmp_path / "wms2_pilot.py"
        write_pilot_script(str(script_path))
        content = script_path.read_text()

        assert "failed_step" in content
        assert "pilot_metrics.json" in content


# ── Report Parsing Tests ──────────────────────────────────────────


class TestParseReport:
    def test_parse_pilot_report(self, tmp_path):
        """Round-trip: write JSON, parse back."""
        report = {
            "steps": [
                {
                    "name": "GEN",
                    "events_requested": 200,
                    "events_read": 200,
                    "wall_time_sec": 45.2,
                    "peak_rss_kb": 1228800,
                    "output_modules": [
                        {"module": "GENoutput", "events_written": 200,
                         "size_bytes": 5242880, "file_path": "GEN_output.root"}
                    ],
                }
            ],
            "summary": {
                "total_time_per_event_sec": 0.226,
                "peak_rss_mb": 1200.0,
                "events_per_second": 4.425,
                "output_size_per_event_kb": 25.6,
                "per_output_module": {
                    "GENoutput": {"size_per_event_kb": 25.6, "filtering_ratio": 1.0}
                },
            },
        }
        path = tmp_path / "pilot_metrics.json"
        path.write_text(json.dumps(report))

        parsed = parse_pilot_report(str(path))
        assert parsed["steps"][0]["name"] == "GEN"
        assert parsed["summary"]["peak_rss_mb"] == 1200.0

    def test_pilot_metrics_from_new_format(self):
        """PilotMetrics.from_json with steps + summary sections."""
        from wms2.core.dag_planner import PilotMetrics

        data = {
            "steps": [
                {
                    "name": "GEN",
                    "events_read": 200,
                    "wall_time_sec": 45.2,
                    "peak_rss_kb": 1228800,
                    "output_modules": [
                        {"module": "GENoutput", "events_written": 200, "size_bytes": 5242880}
                    ],
                },
                {
                    "name": "SIM",
                    "events_read": 200,
                    "wall_time_sec": 300.0,
                    "peak_rss_kb": 1843200,
                    "output_modules": [
                        {"module": "SIMoutput", "events_written": 180, "size_bytes": 52428800}
                    ],
                },
            ],
            "summary": {
                "total_time_per_event_sec": 1.726,
                "peak_rss_mb": 1800,
                "events_per_second": 0.579,
                "output_size_per_event_kb": 256.0,
                "per_output_module": {
                    "SIMoutput": {"size_per_event_kb": 256.0, "filtering_ratio": 0.9}
                },
            },
        }
        m = PilotMetrics.from_json(data)

        # Summary fields populated from summary section
        assert m.events_per_second == pytest.approx(0.579)
        assert m.memory_peak_mb == 1800
        assert m.output_size_per_event_kb == pytest.approx(256.0)
        assert m.time_per_event_sec == pytest.approx(1.726)
        # New fields
        assert m.steps is not None
        assert len(m.steps) == 2
        assert m.per_output_module is not None
        assert "SIMoutput" in m.per_output_module


# ── Pilot Config Computation Tests ───────────────────────────


class TestComputePilotConfig:
    def test_low_filter_efficiency_increases_events(self):
        """Low filter efficiency requires more events for functional test."""
        from wms2.config import Settings
        from wms2.core.dag_planner import _compute_pilot_config

        settings = Settings()
        config = {
            "time_per_event": 0.111,
            "filter_efficiency": 0.003,
            "multicore": 8,
        }
        pc = _compute_pilot_config(config, settings)

        # 10 / 0.003 ≈ 3334 events to get 10 through the filter
        assert pc["initial_events"] == 3333

    def test_very_low_filter_efficiency_caps_at_10000(self):
        """Very low filter eff still capped at 10000 events."""
        from wms2.config import Settings
        from wms2.core.dag_planner import _compute_pilot_config

        settings = Settings()
        config = {
            "time_per_event": 0.001,
            "filter_efficiency": 0.0001,  # 10/0.0001 = 100000
        }
        pc = _compute_pilot_config(config, settings)
        assert pc["initial_events"] == 10000

    def test_no_hints_uses_defaults(self):
        """Without filter efficiency, fall back to settings defaults."""
        from wms2.config import Settings
        from wms2.core.dag_planner import _compute_pilot_config

        settings = Settings()
        config = {}
        pc = _compute_pilot_config(config, settings)

        assert pc["initial_events"] == settings.pilot_initial_events
        assert pc["timeout"] == settings.pilot_step_timeout
        assert pc["ncpus"] == 0
        assert pc["memory_mb"] == 0

    def test_timeout_from_time_per_event(self):
        """Timeout is based on initial_events * time_per_event * 5."""
        from wms2.config import Settings
        from wms2.core.dag_planner import _compute_pilot_config

        settings = Settings()
        config = {"time_per_event": 1.0, "memory_mb": 4000, "multicore": 4}
        pc = _compute_pilot_config(config, settings)

        # initial_events = 200 (default, filter_eff=1.0)
        # timeout = max(600, 200 * 1.0 * 5) = 1000
        assert pc["initial_events"] == 200
        assert pc["timeout"] == 1000
        assert pc["ncpus"] == 4
        assert pc["memory_mb"] == 4000

    def test_timeout_minimum_600(self):
        """Timeout never goes below 600s."""
        from wms2.config import Settings
        from wms2.core.dag_planner import _compute_pilot_config

        settings = Settings()
        config = {"time_per_event": 0.001}  # very fast
        pc = _compute_pilot_config(config, settings)

        # 200 * 0.001 * 5 = 1 → clamped to 600
        assert pc["timeout"] == 600

    def test_no_retry_params(self):
        """Simplified config has no retry_fraction or min_events."""
        from wms2.config import Settings
        from wms2.core.dag_planner import _compute_pilot_config

        settings = Settings()
        pc = _compute_pilot_config({}, settings)

        assert "retry_fraction" not in pc
        assert "min_events" not in pc
        assert "step_timeout" not in pc
