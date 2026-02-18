"""Tests for sandbox builder."""

import json
import os
import tarfile

import pytest

from wms2.core.sandbox import (
    create_sandbox,
    create_sandbox_from_spec,
    _build_manifest,
    _create_test_pset,
)
from wms2.core.stepchain import StepChainSpec, StepSpec, parse_stepchain


class TestBuildManifest:
    def test_synthetic_mode(self):
        reqdata = {"SizePerEvent": 100.0, "TimePerEvent": 1.0, "Memory": 4096}
        manifest = _build_manifest(reqdata, "synthetic")
        assert manifest["mode"] == "synthetic"
        assert manifest["size_per_event_kb"] == 100.0
        assert manifest["time_per_event_sec"] == 1.0
        assert manifest["memory_mb"] == 4096
        assert "output_tiers" in manifest

    def test_synthetic_defaults(self):
        manifest = _build_manifest({}, "synthetic")
        assert manifest["mode"] == "synthetic"
        assert manifest["size_per_event_kb"] == 50.0
        assert manifest["time_per_event_sec"] == 0.5
        assert manifest["memory_mb"] == 2048

    def test_cmssw_mode_single_step(self):
        reqdata = {
            "CMSSWVersion": "CMSSW_14_0_0",
            "ScramArch": "el9_amd64_gcc12",
            "GlobalTag": "140X_mcRun3_2024_realistic_v26",
            "Multicore": 8,
            "Memory": 4096,
            "RequestType": "MonteCarlo",
        }
        manifest = _build_manifest(reqdata, "cmssw")
        assert manifest["mode"] == "cmssw"
        assert manifest["cmssw_version"] == "CMSSW_14_0_0"
        assert manifest["scram_arch"] == "el9_amd64_gcc12"
        assert manifest["global_tag"] == "140X_mcRun3_2024_realistic_v26"
        assert manifest["multicore"] == 8
        assert manifest["memory_mb"] == 4096
        assert len(manifest["steps"]) == 1

    def test_stepchain_uses_parser(self):
        """StepChain requests produce per-step manifest via parse_stepchain."""
        reqdata = {
            "CMSSWVersion": "CMSSW_14_0_0",
            "ScramArch": "el9_amd64_gcc12",
            "GlobalTag": "140X_mcRun3_2024_realistic_v26",
            "RequestType": "StepChain",
            "StepChain": 2,
            "Multicore": 4,
            "Memory": 8000,
            "Step1": {
                "StepName": "DIGI",
                "CMSSWVersion": "CMSSW_12_4_25",
                "ScramArch": "el8_amd64_gcc10",
                "KeepOutput": False,
            },
            "Step2": {
                "StepName": "RECO",
                "KeepOutput": True,
                "InputStep": "DIGI",
                "InputFromOutputModule": "RAWSIMoutput",
            },
        }
        manifest = _build_manifest(reqdata, "cmssw")
        assert manifest["mode"] == "cmssw"
        assert len(manifest["steps"]) == 2

        # New per-step format: each step has its own cmssw_version
        s1 = manifest["steps"][0]
        assert s1["name"] == "DIGI"
        assert s1["cmssw_version"] == "CMSSW_12_4_25"
        assert s1["scram_arch"] == "el8_amd64_gcc10"
        assert s1["keep_output"] is False
        assert s1["pset"] == "steps/step1/cfg.py"

        s2 = manifest["steps"][1]
        assert s2["name"] == "RECO"
        assert s2["cmssw_version"] == "CMSSW_14_0_0"  # inherits top-level
        assert s2["keep_output"] is True
        assert s2["input_step"] == "DIGI"
        assert s2["input_from_output_module"] == "RAWSIMoutput"


class TestCreateTestPset:
    def test_pset_content(self):
        pset = _create_test_pset("RECO", "140X_mcRun3_2024_realistic_v26")
        assert "cms.Process" in pset
        assert "RECO" in pset
        assert "140X_mcRun3_2024_realistic_v26" in pset
        assert "PoolSource" in pset
        assert "PoolOutputModule" in pset


class TestCreateSandbox:
    def test_synthetic_sandbox(self, tmp_path):
        reqdata = {"SizePerEvent": 50.0, "TimePerEvent": 0.5}
        output = str(tmp_path / "sandbox.tar.gz")
        result = create_sandbox(output, reqdata, mode="synthetic")
        assert result == output
        assert os.path.exists(output)

        # Verify tarball contents
        with tarfile.open(output, "r:gz") as tar:
            names = tar.getnames()
            assert "manifest.json" in names

            # Read manifest
            f = tar.extractfile("manifest.json")
            manifest = json.loads(f.read())
            assert manifest["mode"] == "synthetic"
            assert manifest["size_per_event_kb"] == 50.0

    def test_cmssw_sandbox(self, tmp_path):
        reqdata = {
            "CMSSWVersion": "CMSSW_14_0_0",
            "ScramArch": "el9_amd64_gcc12",
            "GlobalTag": "140X_mcRun3_2024_realistic_v26",
            "RequestType": "StepChain",
            "StepChain": 2,
            "Step1": {"StepName": "DIGI"},
            "Step2": {"StepName": "RECO", "InputStep": "DIGI"},
        }
        output = str(tmp_path / "sandbox.tar.gz")
        create_sandbox(output, reqdata, mode="cmssw")

        with tarfile.open(output, "r:gz") as tar:
            names = tar.getnames()
            assert "manifest.json" in names
            assert "steps/step1/cfg.py" in names
            assert "steps/step2/cfg.py" in names

            f = tar.extractfile("manifest.json")
            manifest = json.loads(f.read())
            assert manifest["mode"] == "cmssw"
            # StepChain uses per-step format
            assert len(manifest["steps"]) == 2
            assert manifest["steps"][0]["cmssw_version"] == "CMSSW_14_0_0"

    def test_auto_mode_detects_cmssw(self, tmp_path):
        reqdata = {"CMSSWVersion": "CMSSW_14_0_0", "RequestType": "MonteCarlo"}
        output = str(tmp_path / "sandbox.tar.gz")
        create_sandbox(output, reqdata, mode="auto")

        with tarfile.open(output, "r:gz") as tar:
            f = tar.extractfile("manifest.json")
            manifest = json.loads(f.read())
            assert manifest["mode"] == "cmssw"

    def test_auto_mode_falls_back_to_synthetic(self, tmp_path):
        reqdata = {"SizePerEvent": 25.0}
        output = str(tmp_path / "sandbox.tar.gz")
        create_sandbox(output, reqdata, mode="auto")

        with tarfile.open(output, "r:gz") as tar:
            f = tar.extractfile("manifest.json")
            manifest = json.loads(f.read())
            assert manifest["mode"] == "synthetic"


class TestCreateSandboxFromSpec:
    def _make_spec(self, num_steps=2):
        steps = []
        for i in range(1, num_steps + 1):
            steps.append(StepSpec(
                name=f"step{i}",
                cmssw_version=f"CMSSW_14_{i}_0",
                scram_arch="el9_amd64_gcc12",
                global_tag=f"GT_{i}",
                pset=f"steps/step{i}/cfg.py",
                multicore=4,
                memory_mb=8000,
                keep_output=True,
                input_step=f"step{i-1}" if i > 1 else "",
                input_from_output_module="output" if i > 1 else "",
                mc_pileup="",
                data_pileup="",
                event_streams=0,
                request_num_events=0,
            ))
        return StepChainSpec(
            request_name="test_spec",
            steps=steps,
            time_per_event=1.0,
            size_per_event=1.5,
            filter_efficiency=1.0,
        )

    def test_creates_sandbox_with_psets(self, tmp_path):
        spec = self._make_spec(2)

        # Create real PSet files
        pset_dir = tmp_path / "psets"
        pset_dir.mkdir()
        (pset_dir / "pset1.py").write_text("# PSet for step1")
        (pset_dir / "pset2.py").write_text("# PSet for step2")

        pset_paths = {
            "step1": str(pset_dir / "pset1.py"),
            "step2": str(pset_dir / "pset2.py"),
        }

        output = str(tmp_path / "sandbox.tar.gz")
        result = create_sandbox_from_spec(output, spec, pset_paths)
        assert result == output
        assert os.path.exists(output)

        with tarfile.open(output, "r:gz") as tar:
            names = tar.getnames()
            assert "manifest.json" in names
            assert "steps/step1/cfg.py" in names
            assert "steps/step2/cfg.py" in names

            # Verify PSet content is from our files
            f = tar.extractfile("steps/step1/cfg.py")
            assert f.read().decode() == "# PSet for step1"

            # Verify manifest
            f = tar.extractfile("manifest.json")
            manifest = json.loads(f.read())
            assert manifest["mode"] == "cmssw"
            assert manifest["request_name"] == "test_spec"
            assert len(manifest["steps"]) == 2

    def test_missing_pset_generates_placeholder(self, tmp_path):
        spec = self._make_spec(1)
        output = str(tmp_path / "sandbox.tar.gz")
        # No pset_paths provided — should generate test PSets
        create_sandbox_from_spec(output, spec, {})

        with tarfile.open(output, "r:gz") as tar:
            f = tar.extractfile("steps/step1/cfg.py")
            content = f.read().decode()
            assert "cms.Process" in content
            assert "STEP1" in content

    def test_rejects_wrong_spec_type(self, tmp_path):
        output = str(tmp_path / "sandbox.tar.gz")
        with pytest.raises(TypeError, match="Expected StepChainSpec"):
            create_sandbox_from_spec(output, {"not": "a spec"}, {})

    def test_manifest_has_per_step_data(self, tmp_path):
        spec = self._make_spec(3)
        output = str(tmp_path / "sandbox.tar.gz")
        create_sandbox_from_spec(output, spec, {})

        with tarfile.open(output, "r:gz") as tar:
            f = tar.extractfile("manifest.json")
            manifest = json.loads(f.read())
            assert len(manifest["steps"]) == 3
            for i, step in enumerate(manifest["steps"]):
                assert step["name"] == f"step{i+1}"
                assert step["cmssw_version"] == f"CMSSW_14_{i+1}_0"
                assert step["pset"] == f"steps/step{i+1}/cfg.py"
