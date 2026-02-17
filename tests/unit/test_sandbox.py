"""Tests for sandbox builder."""

import json
import os
import tarfile

import pytest

from wms2.core.sandbox import create_sandbox, _build_manifest, _create_test_pset


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

    def test_cmssw_mode(self):
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

    def test_stepchain_steps(self):
        reqdata = {
            "CMSSWVersion": "CMSSW_14_0_0",
            "ScramArch": "el9_amd64_gcc12",
            "GlobalTag": "140X_mcRun3_2024_realistic_v26",
            "RequestType": "StepChain",
            "StepChain": 2,
            "Step1": {"StepName": "DIGI", "KeepOutput": False},
            "Step2": {"StepName": "RECO", "KeepOutput": True},
        }
        manifest = _build_manifest(reqdata, "cmssw")
        assert len(manifest["steps"]) == 2
        assert manifest["steps"][0]["name"] == "DIGI"
        assert manifest["steps"][0]["keep_output"] is False
        assert manifest["steps"][1]["name"] == "RECO"
        assert manifest["steps"][1]["keep_output"] is True
        assert manifest["steps"][1]["input_from_step"] == "DIGI"


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
            "Step2": {"StepName": "RECO"},
        }
        output = str(tmp_path / "sandbox.tar.gz")
        create_sandbox(output, reqdata, mode="cmssw")

        with tarfile.open(output, "r:gz") as tar:
            names = tar.getnames()
            assert "manifest.json" in names
            assert "steps/step1/PSet.py" in names
            assert "steps/step2/PSet.py" in names

            f = tar.extractfile("manifest.json")
            manifest = json.loads(f.read())
            assert manifest["mode"] == "cmssw"
            assert manifest["cmssw_version"] == "CMSSW_14_0_0"

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
