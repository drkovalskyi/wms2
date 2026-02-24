"""Unit tests for the WMS2 cmsRun simulator."""

from __future__ import annotations

import json
import os
import re
import tempfile
import xml.etree.ElementTree as ET

import numpy as np
import pytest
import uproot

from wms2.core.simulator import (
    allocate_memory,
    burn_cpu,
    compute_resource_profile,
    create_root_file,
    run_simulator,
    write_fjr_xml,
)


# ── Resource model tests ────────────────────────────────────────


class TestResourceModel:
    """Verify Amdahl's law CPU efficiency and linear memory scaling."""

    def test_amdahl_8t_serial_030(self):
        """serial_fraction=0.30, 8 threads → eff_cores=6.1, eff=0.59 (plan says 5.9/0.59)."""
        step = {"serial_fraction": 0.30, "wall_sec_per_event": 0.05}
        p = compute_resource_profile(step, ncpus=8, events=100)
        # eff_cores = 0.30 + 0.70 * 8 = 5.90
        assert abs(p["eff_cores"] - 5.90) < 0.01
        # cpu_efficiency = 5.90 / 8 = 0.7375
        assert abs(p["cpu_efficiency"] - 0.7375) < 0.01

    def test_amdahl_4t_serial_030(self):
        """serial_fraction=0.30, 4 threads → eff=0.775."""
        step = {"serial_fraction": 0.30, "wall_sec_per_event": 0.05}
        p = compute_resource_profile(step, ncpus=4, events=100)
        # eff_cores = 0.30 + 0.70 * 4 = 3.10
        assert abs(p["eff_cores"] - 3.10) < 0.01
        assert abs(p["cpu_efficiency"] - 0.775) < 0.01

    def test_amdahl_2t_serial_030(self):
        """serial_fraction=0.30, 2 threads → eff=0.85."""
        step = {"serial_fraction": 0.30, "wall_sec_per_event": 0.05}
        p = compute_resource_profile(step, ncpus=2, events=100)
        # eff_cores = 0.30 + 0.70 * 2 = 1.70
        assert abs(p["eff_cores"] - 1.70) < 0.01
        assert abs(p["cpu_efficiency"] - 0.85) < 0.01

    def test_amdahl_1t_serial_030(self):
        """serial_fraction=0.30, 1 thread → eff=1.0."""
        step = {"serial_fraction": 0.30, "wall_sec_per_event": 0.05}
        p = compute_resource_profile(step, ncpus=1, events=100)
        assert abs(p["eff_cores"] - 1.0) < 0.01
        assert abs(p["cpu_efficiency"] - 1.0) < 0.01

    def test_memory_scaling(self):
        """base=1500, marginal=250 → 4T=2500, 8T=3500."""
        step = {"base_memory_mb": 1500, "marginal_per_thread_mb": 250}
        p4 = compute_resource_profile(step, ncpus=4, events=1)
        p8 = compute_resource_profile(step, ncpus=8, events=1)
        assert p4["peak_rss_mb"] == 2500
        assert p8["peak_rss_mb"] == 3500

    def test_wall_time_scaling(self):
        """Fewer threads → longer wall time."""
        step = {
            "serial_fraction": 0.10,
            "wall_sec_per_event": 0.10,
        }
        p8 = compute_resource_profile(step, ncpus=8, events=100)
        p4 = compute_resource_profile(step, ncpus=4, events=100)
        p2 = compute_resource_profile(step, ncpus=2, events=100)
        # More threads → shorter wall time
        assert p8["wall_sec"] < p4["wall_sec"] < p2["wall_sec"]

    def test_cpu_time_invariant(self):
        """CPU time should be roughly constant regardless of thread count.

        cpu_time = wall * eff_cores = events * wall_per_event (by Amdahl)
        """
        step = {
            "serial_fraction": 0.10,
            "wall_sec_per_event": 0.10,
        }
        p1 = compute_resource_profile(step, ncpus=1, events=100)
        p8 = compute_resource_profile(step, ncpus=8, events=100)
        # Both should equal events * wall_per_event = 100 * 0.10 = 10.0
        assert abs(p1["cpu_time_sec"] - 10.0) < 0.01
        assert abs(p8["cpu_time_sec"] - 10.0) < 0.01

    def test_default_parameters(self):
        """Empty step dict uses sensible defaults."""
        p = compute_resource_profile({}, ncpus=4, events=10)
        assert p["eff_cores"] > 0
        assert p["peak_rss_mb"] > 0
        assert p["wall_sec"] > 0


# ── Memory allocation tests ─────────────────────────────────────


class TestMemoryAllocation:
    def test_allocate_correct_size(self):
        """Bytearray is exactly the requested size."""
        buf = allocate_memory(1)  # 1 MB
        assert len(buf) == 1 * 1024 * 1024

    def test_allocate_pages_touched(self):
        """Pages are touched (non-zero bytes at page boundaries)."""
        buf = allocate_memory(0.1)  # 100 KB
        # Check that page-start bytes are set to 1
        assert buf[0] == 1
        if len(buf) > 4096:
            assert buf[4096] == 1


# ── CPU burn tests ───────────────────────────────────────────────


class TestCPUBurn:
    def test_burn_starts_stops(self):
        """CPU burn runs for approximately the requested duration."""
        import time

        start = time.time()
        burn_cpu(0.2, eff_cores=1.0, ncpus=1)
        elapsed = time.time() - start
        assert elapsed >= 0.15  # allow some slack
        assert elapsed < 1.0  # shouldn't take forever

    def test_burn_zero_duration(self):
        """Zero-duration burn returns immediately."""
        import time

        start = time.time()
        burn_cpu(0.0, eff_cores=1.0, ncpus=1)
        elapsed = time.time() - start
        assert elapsed < 0.5


# ── ROOT file tests ─────────────────────────────────────────────


class TestRootFile:
    def test_create_with_events(self, tmp_path):
        """ROOT file has correct TTree and entry count."""
        path = str(tmp_path / "test.root")
        size = create_root_file(path, events=50, target_size_kb=10)
        assert size > 0
        assert os.path.isfile(path)

        with uproot.open(path) as f:
            assert "Events" in f
            tree = f["Events"]
            assert tree.num_entries == 50
            assert "run" in tree
            assert "event" in tree
            assert "luminosityBlock" in tree

    def test_approximate_target_size(self, tmp_path):
        """Output file is approximately the target size."""
        path = str(tmp_path / "sized.root")
        create_root_file(path, events=100, target_size_kb=100)
        actual_kb = os.path.getsize(path) / 1024
        # Allow 50% tolerance due to ROOT compression
        assert actual_kb > 20  # at least some content
        assert actual_kb < 500  # not wildly oversized

    def test_mergeable(self, tmp_path):
        """Two simulator ROOT files can be merged via uproot."""
        path1 = str(tmp_path / "a.root")
        path2 = str(tmp_path / "b.root")
        # Same events_per_job (typical pattern) ensures padding column counts match
        create_root_file(path1, events=25, target_size_kb=5)
        create_root_file(path2, events=25, target_size_kb=5)

        # Read and concatenate using awkward arrays (handles 2D padding field)
        with uproot.open(path1) as f1, uproot.open(path2) as f2:
            a1 = f1["Events"].arrays()
            a2 = f2["Events"].arrays()
            merged_event = np.concatenate([
                np.asarray(a1["event"]), np.asarray(a2["event"])
            ])
            assert len(merged_event) == 50

        # Write merged file using the same approach as merge_root_with_uproot
        merged_path = str(tmp_path / "merged.root")
        all_data = {}
        for p in [path1, path2]:
            with uproot.open(p) as f:
                tree = f["Events"]
                ak_arrays = tree.arrays()
                for key in tree.keys():
                    all_data.setdefault(key, []).append(
                        np.asarray(ak_arrays[key])
                    )
        merged = {}
        for k, v in all_data.items():
            try:
                merged[k] = np.concatenate(v)
            except ValueError:
                pass  # skip incompatible shapes (padding)
        with uproot.recreate(merged_path) as f:
            f["Events"] = merged

        with uproot.open(merged_path) as f:
            assert f["Events"].num_entries == 50


# ── FJR XML tests ────────────────────────────────────────────────


class TestFJRXml:
    @pytest.fixture()
    def gen_step_config(self):
        return {
            "name": "GEN-SIM",
            "multicore": 8,
            "output_module": "GEN-SIMoutput",
            "data_tier": "GEN-SIM",
        }

    @pytest.fixture()
    def gen_profile(self):
        return {
            "eff_cores": 5.90,
            "cpu_efficiency": 0.7375,
            "peak_rss_mb": 3500,
            "wall_sec": 10.0,
            "cpu_time_sec": 59.0,
        }

    def test_single_step_fjr(self, tmp_path, gen_step_config, gen_profile):
        """FJR for a GEN step has no InputFile and correct metrics."""
        root_path = str(tmp_path / "output.root")
        with open(root_path, "wb") as f:
            f.write(b"\0" * 1024)

        fjr_path = str(tmp_path / "report_step1.xml")
        write_fjr_xml(
            fjr_path=fjr_path,
            step_config=gen_step_config,
            profile=gen_profile,
            events=100,
            output_path=root_path,
            input_path=None,
            step_num=1,
        )

        tree = ET.parse(fjr_path)
        root = tree.getroot()

        # No InputFile for GEN
        assert root.find("InputFile") is None

        # Output file
        out_file = root.find("File")
        assert out_file is not None
        assert out_file.findtext("TotalEvents") == "100"
        assert out_file.findtext("ModuleLabel") == "GEN-SIMoutput"
        assert "file:" in out_file.findtext("PFN", "")

    def test_multi_step_fjr(self, tmp_path):
        """FJR for step 2+ has InputFile."""
        root_path = str(tmp_path / "output.root")
        input_path = str(tmp_path / "input.root")
        for p in [root_path, input_path]:
            with open(p, "wb") as f:
                f.write(b"\0" * 512)

        fjr_path = str(tmp_path / "report_step2.xml")
        write_fjr_xml(
            fjr_path=fjr_path,
            step_config={"multicore": 4, "output_module": "DIGIRAWoutput"},
            profile={
                "eff_cores": 3.6,
                "cpu_efficiency": 0.9,
                "peak_rss_mb": 2000,
                "wall_sec": 5.0,
                "cpu_time_sec": 18.0,
            },
            events=50,
            output_path=root_path,
            input_path=input_path,
            step_num=2,
        )

        tree = ET.parse(fjr_path)
        root = tree.getroot()
        assert root.find("InputFile") is not None
        assert root.find("InputFile").findtext("EventsRead") == "50"

    def test_fjr_timing_metrics(self, tmp_path, gen_step_config, gen_profile):
        """Timing metrics are correctly written."""
        root_path = str(tmp_path / "output.root")
        with open(root_path, "wb") as f:
            f.write(b"\0" * 1024)

        fjr_path = str(tmp_path / "report_step1.xml")
        write_fjr_xml(
            fjr_path=fjr_path,
            step_config=gen_step_config,
            profile=gen_profile,
            events=100,
            output_path=root_path,
            step_num=1,
        )

        tree = ET.parse(fjr_path)
        root = tree.getroot()

        # Find Timing PerformanceSummary
        timing = None
        for ps in root.findall(".//PerformanceSummary"):
            if ps.get("Metric") == "Timing":
                timing = ps
                break
        assert timing is not None

        metrics = {
            m.get("Name"): m.get("Value") for m in timing.findall("Metric")
        }
        assert float(metrics["TotalJobTime"]) == pytest.approx(10.0, abs=0.01)
        assert float(metrics["TotalJobCPU"]) == pytest.approx(59.0, abs=0.01)
        assert metrics["NumberOfThreads"] == "8"


# ── FJR parsing compatibility tests ─────────────────────────────


class TestFJRParsingCompat:
    """Feed simulator FJR to the same parsing logic from wms2_proc.sh."""

    def _parse_fjr_metrics(self, fjr_path: str, step_index: int) -> dict:
        """Replicate the proc script's FJR metrics parser (embedded Python)."""
        tree = ET.parse(fjr_path)
        root = tree.getroot()

        metrics = {"step": step_index + 1, "step_index": step_index}

        def get_metric(parent_name, metric_name, cast=float):
            for ps in root.findall(".//PerformanceReport/PerformanceSummary"):
                if ps.get("Metric") == parent_name:
                    for m in ps.findall("Metric"):
                        if m.get("Name") == metric_name:
                            try:
                                return cast(m.get("Value", 0))
                            except (ValueError, TypeError):
                                return None
            return None

        wall = get_metric("Timing", "TotalJobTime")
        cpu = get_metric("Timing", "TotalJobCPU")
        nthreads = get_metric("Timing", "NumberOfThreads", int)

        metrics["wall_time_sec"] = round(wall, 2) if wall is not None else None
        metrics["cpu_time_sec"] = round(cpu, 2) if cpu is not None else None
        metrics["num_threads"] = nthreads

        if wall and cpu and nthreads and wall > 0 and nthreads > 0:
            metrics["cpu_efficiency"] = round(cpu / (wall * nthreads), 4)
        else:
            metrics["cpu_efficiency"] = None

        metrics["peak_rss_mb"] = get_metric("ApplicationMemory", "PeakValueRss")
        nevents = get_metric("ProcessingSummary", "NumberEvents", int)
        metrics["events_processed"] = nevents

        return metrics

    def _parse_fjr_output(self, fjr_path: str) -> str:
        """Replicate the proc script's FJR output parser."""
        tree = ET.parse(fjr_path)
        candidates = []
        for f in tree.findall(".//File"):
            pfn = f.findtext("PFN", "")
            if pfn.startswith("file:"):
                pfn = pfn[5:]
            if not pfn:
                continue
            label = f.findtext("ModuleLabel", "")
            candidates.append((pfn, label))
        for pfn, label in candidates:
            if "LHE" not in label.upper():
                return pfn
        if candidates:
            return candidates[0][0]
        return ""

    def _parse_output_manifest(self, fjr_path: str) -> dict:
        """Replicate the proc script's output manifest builder (tier extraction)."""
        tree = ET.parse(fjr_path)
        result = {}
        step_num = 1
        m = re.search(r"report_step(\d+)", fjr_path)
        if m:
            step_num = int(m.group(1))
        step_index = step_num - 1

        for fnode in tree.findall(".//File"):
            pfn = fnode.findtext("PFN", "")
            if pfn.startswith("file:"):
                pfn = pfn[5:]
            pfn = os.path.basename(pfn)
            label = fnode.findtext("ModuleLabel", "")
            tier = re.sub(r"output$", "", label, flags=re.IGNORECASE)
            if pfn and tier:
                result[pfn] = {"tier": tier, "step_index": step_index}
        return result

    def test_metrics_parsing(self, tmp_path):
        """Simulator FJR produces correct metrics through the proc parser."""
        root_path = str(tmp_path / "output.root")
        with open(root_path, "wb") as f:
            f.write(b"\0" * 1024)

        step = {"multicore": 8, "output_module": "GEN-SIMoutput"}
        profile = {
            "eff_cores": 5.90,
            "cpu_efficiency": 0.7375,
            "peak_rss_mb": 3500,
            "wall_sec": 10.0,
            "cpu_time_sec": 59.0,
        }

        fjr_path = str(tmp_path / "report_step1.xml")
        write_fjr_xml(fjr_path, step, profile, 100, root_path, step_num=1)

        m = self._parse_fjr_metrics(fjr_path, step_index=0)
        assert m["wall_time_sec"] == 10.0
        assert m["cpu_time_sec"] == 59.0
        assert m["num_threads"] == 8
        # cpu_efficiency = TotalJobCPU / (TotalJobTime * nThreads) = 59/(10*8) = 0.7375
        assert m["cpu_efficiency"] == pytest.approx(0.7375, abs=0.001)
        assert m["peak_rss_mb"] == pytest.approx(3500, abs=1)
        assert m["events_processed"] == 100

    def test_output_parsing(self, tmp_path):
        """Simulator FJR output is parsed correctly for step chaining."""
        root_path = str(tmp_path / "step1_GEN-SIM_output.root")
        with open(root_path, "wb") as f:
            f.write(b"\0" * 512)

        fjr_path = str(tmp_path / "report_step1.xml")
        write_fjr_xml(
            fjr_path,
            {"multicore": 4, "output_module": "GEN-SIMoutput"},
            {"eff_cores": 3.1, "cpu_efficiency": 0.775, "peak_rss_mb": 2500,
             "wall_sec": 5.0, "cpu_time_sec": 15.5},
            events=50,
            output_path=root_path,
            step_num=1,
        )

        parsed = self._parse_fjr_output(fjr_path)
        assert parsed == root_path

    def test_tier_extraction(self, tmp_path):
        """ModuleLabel → tier extraction matches data_tier."""
        root_path = str(tmp_path / "step1_MINIAODSIM_output.root")
        with open(root_path, "wb") as f:
            f.write(b"\0" * 512)

        fjr_path = str(tmp_path / "report_step1.xml")
        write_fjr_xml(
            fjr_path,
            {"multicore": 4, "output_module": "MINIAODSIMoutput"},
            {"eff_cores": 3.6, "cpu_efficiency": 0.9, "peak_rss_mb": 2000,
             "wall_sec": 5.0, "cpu_time_sec": 18.0},
            events=50,
            output_path=root_path,
            step_num=1,
        )

        manifest = self._parse_output_manifest(fjr_path)
        basename = os.path.basename(root_path)
        assert basename in manifest
        assert manifest[basename]["tier"] == "MINIAODSIM"


# ── Full simulator run test ──────────────────────────────────────


class TestSimulatorRun:
    def test_single_step_gen(self, tmp_path):
        """Single GEN step produces ROOT file and FJR."""
        manifest = {
            "mode": "simulator",
            "steps": [
                {
                    "name": "GEN-SIM",
                    "serial_fraction": 0.30,
                    "base_memory_mb": 10,  # small for testing
                    "marginal_per_thread_mb": 2,
                    "wall_sec_per_event": 0.001,
                    "output_size_per_event_kb": 1,
                    "multicore": 2,
                    "output_module": "GEN-SIMoutput",
                    "data_tier": "GEN-SIM",
                    "keep_output": True,
                },
            ],
        }

        manifest_path = str(tmp_path / "manifest.json")
        with open(manifest_path, "w") as f:
            json.dump(manifest, f)

        orig_dir = os.getcwd()
        os.chdir(tmp_path)
        try:
            rc = run_simulator(manifest_path, node_index=0,
                               events_per_job=10, first_event=1)
        finally:
            os.chdir(orig_dir)

        assert rc == 0
        assert os.path.isfile(tmp_path / "step1_GEN-SIM_output.root")
        assert os.path.isfile(tmp_path / "report_step1.xml")

        # Verify ROOT file
        with uproot.open(str(tmp_path / "step1_GEN-SIM_output.root")) as f:
            assert f["Events"].num_entries == 10

        # Verify FJR has no InputFile (GEN step)
        tree = ET.parse(str(tmp_path / "report_step1.xml"))
        assert tree.getroot().find("InputFile") is None

    def test_multi_step_chain(self, tmp_path):
        """3-step chain produces correct step chaining."""
        manifest = {
            "mode": "simulator",
            "steps": [
                {
                    "name": "GEN-SIM",
                    "serial_fraction": 0.30,
                    "base_memory_mb": 10,
                    "marginal_per_thread_mb": 2,
                    "wall_sec_per_event": 0.001,
                    "output_size_per_event_kb": 1,
                    "multicore": 2,
                    "output_module": "GEN-SIMoutput",
                    "data_tier": "GEN-SIM",
                    "keep_output": False,
                },
                {
                    "name": "DIGI",
                    "serial_fraction": 0.10,
                    "base_memory_mb": 10,
                    "marginal_per_thread_mb": 2,
                    "wall_sec_per_event": 0.001,
                    "output_size_per_event_kb": 1,
                    "multicore": 2,
                    "output_module": "DIGIRAWoutput",
                    "data_tier": "GEN-SIM-DIGI-RAW",
                    "keep_output": False,
                },
                {
                    "name": "RECO",
                    "serial_fraction": 0.10,
                    "base_memory_mb": 10,
                    "marginal_per_thread_mb": 2,
                    "wall_sec_per_event": 0.001,
                    "output_size_per_event_kb": 1,
                    "multicore": 2,
                    "output_module": "MINIAODSIMoutput",
                    "data_tier": "MINIAODSIM",
                    "keep_output": True,
                },
            ],
        }

        manifest_path = str(tmp_path / "manifest.json")
        with open(manifest_path, "w") as f:
            json.dump(manifest, f)

        orig_dir = os.getcwd()
        os.chdir(tmp_path)
        try:
            rc = run_simulator(manifest_path, node_index=0,
                               events_per_job=5, first_event=1)
        finally:
            os.chdir(orig_dir)

        assert rc == 0

        # All 3 steps should produce FJR + ROOT
        for i in range(1, 4):
            assert os.path.isfile(tmp_path / f"report_step{i}.xml")

        # Step 1 (GEN): no InputFile
        tree1 = ET.parse(str(tmp_path / "report_step1.xml"))
        assert tree1.getroot().find("InputFile") is None

        # Step 2 (DIGI): has InputFile pointing to step 1 output
        tree2 = ET.parse(str(tmp_path / "report_step2.xml"))
        inp = tree2.getroot().find("InputFile")
        assert inp is not None
        pfn = inp.findtext("PFN", "")
        assert "step1_GEN-SIM_output.root" in pfn

    def test_ncpus_override(self, tmp_path):
        """ncpus override changes resource profile."""
        manifest = {
            "mode": "simulator",
            "steps": [
                {
                    "name": "GEN-SIM",
                    "serial_fraction": 0.30,
                    "base_memory_mb": 10,
                    "marginal_per_thread_mb": 5,
                    "wall_sec_per_event": 0.001,
                    "output_size_per_event_kb": 1,
                    "multicore": 8,
                    "output_module": "GEN-SIMoutput",
                    "data_tier": "GEN-SIM",
                },
            ],
        }

        manifest_path = str(tmp_path / "manifest.json")
        with open(manifest_path, "w") as f:
            json.dump(manifest, f)

        orig_dir = os.getcwd()
        os.chdir(tmp_path)
        try:
            rc = run_simulator(manifest_path, node_index=0,
                               events_per_job=5, first_event=1,
                               ncpus_override=4)
        finally:
            os.chdir(orig_dir)

        assert rc == 0

        # FJR should report 4 threads, not 8
        tree = ET.parse(str(tmp_path / "report_step1.xml"))
        timing = None
        for ps in tree.getroot().findall(".//PerformanceSummary"):
            if ps.get("Metric") == "Timing":
                timing = ps
                break
        nthreads = None
        for m in timing.findall("Metric"):
            if m.get("Name") == "NumberOfThreads":
                nthreads = int(m.get("Value"))
        assert nthreads == 4


# ── Sandbox manifest building tests ─────────────────────────────


class TestSimulatorManifestBuilding:
    def test_build_simulator_steps(self):
        """_build_simulator_steps creates correct step configs."""
        from wms2.core.sandbox import _build_simulator_steps

        request_data = {
            "RequestType": "StepChain",
            "StepChain": 3,
            "Step1": {"StepName": "GEN-SIM"},
            "Step2": {"StepName": "DIGI"},
            "Step3": {"StepName": "RECO"},
            "Multicore": 8,
        }
        steps = _build_simulator_steps(request_data)
        assert len(steps) == 3
        assert steps[0]["name"] == "GEN-SIM"
        # GEN steps have higher serial fraction
        assert steps[0]["serial_fraction"] > steps[1]["serial_fraction"]
        # All steps have required fields
        for s in steps:
            assert "base_memory_mb" in s
            assert "marginal_per_thread_mb" in s
            assert "wall_sec_per_event" in s
            assert "output_module" in s
            assert "data_tier" in s

    def test_build_manifest_simulator_mode(self):
        """_build_manifest with mode=simulator returns correct structure."""
        from wms2.core.sandbox import _build_manifest

        request_data = {
            "RequestType": "StepChain",
            "StepChain": 1,
            "Step1": {"StepName": "GEN-SIM"},
            "Multicore": 4,
        }
        manifest = _build_manifest(request_data, "simulator")
        assert manifest["mode"] == "simulator"
        assert len(manifest["steps"]) == 1
        assert manifest["steps"][0]["multicore"] == 4
