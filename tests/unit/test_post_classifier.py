"""Tests for POST script error classification and collector script generation."""

import json
import os
import textwrap

import pytest

from wms2.core.post_classifier import (
    DATA_CODES,
    INFRASTRUCTURE_CODES,
    MEMORY_CODES,
    PERMANENT_CODES,
    classify_error,
    generate_collector_script,
)


# ── Classification Tests ──────────────────────────────────────


class TestClassifySuccess:
    def test_both_zero_is_success(self):
        result = classify_error(0, 0)
        assert result["category"] == "success"
        assert result["retryable"] is False
        assert result["memory_exceeded"] is False


class TestClassifyPermanent:
    @pytest.mark.parametrize("code", [70, 73, 8001, 8009, 8501, 50664, 50110])
    def test_permanent_cmssw_codes(self, code):
        result = classify_error(code, 1)
        assert result["category"] == "permanent"
        assert result["retryable"] is False
        assert result["action"] == "permanent_failure"

    def test_permanent_as_job_exit(self):
        """When CMSSW exit is 0, use job exit code."""
        result = classify_error(0, 8001)
        assert result["category"] == "permanent"

    @pytest.mark.parametrize("code", [7000, 7001, 7002, 8006, 8023, 8026,
                                       50113, 71102, 71104, 71105])
    def test_all_permanent_codes(self, code):
        result = classify_error(code, 1)
        assert result["category"] == "permanent"


class TestClassifyData:
    @pytest.mark.parametrize("code", [8021, 8028, 8016, 8027, 8034])
    def test_data_codes(self, code):
        result = classify_error(code, 1)
        assert result["category"] == "data"
        assert result["retryable"] is False
        assert result["action"] == "permanent_failure"


class TestClassifyInfrastructure:
    @pytest.mark.parametrize("code", [50, 71, 72, 74, 81, 8020, 8033, 8035,
                                       10003, 10018, 60321, 60311, 60315, 60316])
    def test_infrastructure_codes(self, code):
        result = classify_error(code, 1)
        assert result["category"] == "infrastructure"
        assert result["retryable"] is True
        assert result["action"] == "retry_with_cooloff"
        assert result["memory_exceeded"] is False

    @pytest.mark.parametrize("code", [50660, 50661, 8004, 8030, 8031])
    def test_memory_codes_are_infrastructure_memory(self, code):
        result = classify_error(code, 1)
        assert result["category"] == "infrastructure_memory"
        assert result["retryable"] is True
        assert result["memory_exceeded"] is True


class TestClassifyTransient:
    @pytest.mark.parametrize("code", [1, 11, 139, 195, 99999])
    def test_unknown_codes_are_transient(self, code):
        result = classify_error(code, 1)
        assert result["category"] == "transient"
        assert result["retryable"] is True
        assert result["action"] == "retry"

    def test_signal_codes_are_transient(self):
        """Signals (SIGSEGV=139, SIGKILL=137) default to transient."""
        result = classify_error(139, 1)
        assert result["category"] == "transient"

    def test_cmssw_preferred_over_job_exit(self):
        """When both are non-zero, CMSSW code takes priority."""
        # CMSSW=8021 (data), job=1 → data
        result = classify_error(8021, 1)
        assert result["category"] == "data"

        # CMSSW=0, job=8021 → data (falls back to job)
        result = classify_error(0, 8021)
        assert result["category"] == "data"


class TestCodeSetConsistency:
    def test_memory_codes_subset_of_infrastructure(self):
        """All memory codes should also be in infrastructure codes."""
        assert MEMORY_CODES.issubset(INFRASTRUCTURE_CODES)

    def test_no_overlap_permanent_data(self):
        assert PERMANENT_CODES.isdisjoint(DATA_CODES)

    def test_no_overlap_permanent_infrastructure(self):
        assert PERMANENT_CODES.isdisjoint(INFRASTRUCTURE_CODES)

    def test_no_overlap_data_infrastructure(self):
        assert DATA_CODES.isdisjoint(INFRASTRUCTURE_CODES)


# ── Collector Script Tests ────────────────────────────────────


class TestGenerateCollectorScript:
    def test_valid_python(self):
        """Generated script should be valid Python (compile without errors)."""
        script = generate_collector_script()
        compile(script, "wms2_post_collect.py", "exec")

    def test_contains_classification_tables(self):
        script = generate_collector_script()
        assert "PERMANENT_CODES" in script
        assert "DATA_CODES" in script
        assert "INFRASTRUCTURE_CODES" in script
        assert "MEMORY_CODES" in script

    def test_contains_main_entry(self):
        script = generate_collector_script()
        assert "def main():" in script
        assert '__name__ == "__main__"' in script

    def test_contains_fjr_parser(self):
        script = generate_collector_script()
        assert "def parse_fjr(" in script
        assert "FrameworkError" in script
        assert "PeakValueRss" in script

    def test_contains_condor_log_parser(self):
        script = generate_collector_script()
        assert "def parse_condor_log(" in script
        assert "MATCH_GLIDEIN_CMSSite" in script

    def test_shebang(self):
        script = generate_collector_script()
        assert script.startswith("#!/usr/bin/env python3")


class TestCollectorWritesPostJson:
    """Integration test: run the collector and validate post.json output."""

    def test_collector_writes_json_on_failure(self, tmp_path):
        """Run collector with a mock failed job and check output."""
        script = generate_collector_script()
        script_path = tmp_path / "wms2_post_collect.py"
        script_path.write_text(script)

        # Create a minimal FJR XML with exit code 8021 (FileReadError)
        fjr = tmp_path / "report_step1.xml"
        fjr.write_text(textwrap.dedent("""\
            <?xml version="1.0" ?>
            <FrameworkJobReport>
              <FrameworkError ExitStatus="8021" Type="FileReadError">
                Unable to read /store/data/file_001.root
              </FrameworkError>
              <InputFile>
                <LFN>/store/data/file_001.root</LFN>
                <EventsRead>0</EventsRead>
              </InputFile>
            </FrameworkJobReport>
        """))

        # Create a mock .err file
        err_file = tmp_path / "proc_000001.err"
        err_file.write_text("Error: file read failure\n")

        # Run the collector
        import subprocess
        result = subprocess.run(
            ["python3", str(script_path), "proc_000001", "8021", "0", "3"],
            cwd=str(tmp_path),
            capture_output=True, text=True, timeout=10,
        )

        assert result.returncode == 0
        # stdout should be the classification category
        assert result.stdout.strip() == "data"

        # Check post.json was written
        post_json = tmp_path / "proc_000001.post.json"
        assert post_json.exists()

        data = json.loads(post_json.read_text())
        assert data["node_name"] == "proc_000001"
        assert data["attempt"] == 0
        assert data["max_retries"] == 3
        assert data["final"] is True  # data errors are non-retryable
        assert data["cmssw"]["exit_code"] == 8021
        assert data["classification"]["category"] == "data"
        assert data["classification"]["retryable"] is False
        assert "/store/data/file_001.root" in data["classification"]["bad_input_files"]

    def test_collector_success_case(self, tmp_path):
        """Exit code 0 produces success classification."""
        script = generate_collector_script()
        script_path = tmp_path / "wms2_post_collect.py"
        script_path.write_text(script)

        import subprocess
        result = subprocess.run(
            ["python3", str(script_path), "proc_000001", "0", "0", "3"],
            cwd=str(tmp_path),
            capture_output=True, text=True, timeout=10,
        )

        assert result.returncode == 0
        assert result.stdout.strip() == "success"

        post_json = tmp_path / "proc_000001.post.json"
        assert post_json.exists()
        data = json.loads(post_json.read_text())
        assert data["classification"]["category"] == "success"
        assert data["final"] is True

    def test_collector_transient_not_final_on_first_attempt(self, tmp_path):
        """Transient error on attempt 0 of 3 → final=False."""
        script = generate_collector_script()
        script_path = tmp_path / "wms2_post_collect.py"
        script_path.write_text(script)

        import subprocess
        result = subprocess.run(
            ["python3", str(script_path), "proc_000001", "139", "0", "3"],
            cwd=str(tmp_path),
            capture_output=True, text=True, timeout=10,
        )

        assert result.returncode == 0
        assert result.stdout.strip() == "transient"

        data = json.loads((tmp_path / "proc_000001.post.json").read_text())
        assert data["final"] is False  # retryable, retries remain

    def test_collector_transient_final_on_last_attempt(self, tmp_path):
        """Transient error on attempt 3 of 3 → final=True (retries exhausted)."""
        script = generate_collector_script()
        script_path = tmp_path / "wms2_post_collect.py"
        script_path.write_text(script)

        import subprocess
        result = subprocess.run(
            ["python3", str(script_path), "proc_000001", "139", "3", "3"],
            cwd=str(tmp_path),
            capture_output=True, text=True, timeout=10,
        )

        assert result.returncode == 0
        data = json.loads((tmp_path / "proc_000001.post.json").read_text())
        assert data["final"] is True
