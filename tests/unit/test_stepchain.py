"""Tests for StepChain parser."""

import pytest

from wms2.core.stepchain import StepChainSpec, StepSpec, parse_stepchain, to_manifest


class TestParseStepchain:
    def test_basic_stepchain(self):
        """2-step chain, step overrides top-level."""
        data = {
            "RequestName": "test_req",
            "StepChain": 2,
            "CMSSWVersion": "CMSSW_14_0_0",
            "ScramArch": "el9_amd64_gcc12",
            "GlobalTag": "GT_DEFAULT",
            "Multicore": 4,
            "Memory": 8000,
            "TimePerEvent": 2.0,
            "SizePerEvent": 3.0,
            "FilterEfficiency": 0.9,
            "Step1": {
                "StepName": "GEN-SIM",
                "CMSSWVersion": "CMSSW_12_4_25",
                "ScramArch": "el8_amd64_gcc10",
                "GlobalTag": "GT_STEP1",
                "Multicore": 8,
                "Memory": 16000,
            },
            "Step2": {
                "StepName": "DIGI",
                "InputStep": "GEN-SIM",
                "InputFromOutputModule": "RAWSIMoutput",
            },
        }
        spec = parse_stepchain(data)

        assert spec.request_name == "test_req"
        assert len(spec.steps) == 2
        assert spec.time_per_event == 2.0
        assert spec.size_per_event == 3.0
        assert spec.filter_efficiency == 0.9

        # Step 1 overrides
        s1 = spec.steps[0]
        assert s1.name == "GEN-SIM"
        assert s1.cmssw_version == "CMSSW_12_4_25"
        assert s1.scram_arch == "el8_amd64_gcc10"
        assert s1.global_tag == "GT_STEP1"
        assert s1.multicore == 8
        assert s1.memory_mb == 16000
        assert s1.pset == "steps/step1/cfg.py"
        assert s1.input_step == ""
        assert s1.keep_output is True

        # Step 2 inherits top-level CMSSW/arch
        s2 = spec.steps[1]
        assert s2.name == "DIGI"
        assert s2.cmssw_version == "CMSSW_14_0_0"
        assert s2.scram_arch == "el9_amd64_gcc12"
        assert s2.global_tag == "GT_DEFAULT"
        assert s2.multicore == 4
        assert s2.memory_mb == 8000
        assert s2.input_step == "GEN-SIM"
        assert s2.input_from_output_module == "RAWSIMoutput"
        assert s2.pset == "steps/step2/cfg.py"

    def test_step_inherits_defaults(self):
        """Step without CMSSWVersion gets top-level."""
        data = {
            "StepChain": 1,
            "CMSSWVersion": "CMSSW_13_0_0",
            "ScramArch": "el8_amd64_gcc11",
            "GlobalTag": "GT_TOP",
            "Multicore": 2,
            "Memory": 4096,
            "EventStreams": 4,
            "RequestNumEvents": 1000,
            "Step1": {"StepName": "GEN"},
        }
        spec = parse_stepchain(data)
        s = spec.steps[0]
        assert s.cmssw_version == "CMSSW_13_0_0"
        assert s.scram_arch == "el8_amd64_gcc11"
        assert s.global_tag == "GT_TOP"
        assert s.multicore == 2
        assert s.memory_mb == 4096
        assert s.event_streams == 4
        assert s.request_num_events == 1000

    def test_step_overrides_all_fields(self):
        """Step overrides every overrideable field."""
        data = {
            "StepChain": 1,
            "CMSSWVersion": "CMSSW_14_0_0",
            "ScramArch": "el9_amd64_gcc12",
            "GlobalTag": "GT_TOP",
            "Multicore": 1,
            "Memory": 2048,
            "EventStreams": 0,
            "RequestNumEvents": 100,
            "Step1": {
                "StepName": "RECO",
                "CMSSWVersion": "CMSSW_12_4_25",
                "ScramArch": "el8_amd64_gcc10",
                "GlobalTag": "GT_STEP",
                "Multicore": 8,
                "Memory": 16000,
                "EventStreams": 8,
                "RequestNumEvents": 5000,
                "KeepOutput": False,
                "MCPileup": "/MinBias/Run3/GEN-SIM",
                "DataPileup": "/ZeroBias/Run2024/RAW",
            },
        }
        spec = parse_stepchain(data)
        s = spec.steps[0]
        assert s.cmssw_version == "CMSSW_12_4_25"
        assert s.scram_arch == "el8_amd64_gcc10"
        assert s.global_tag == "GT_STEP"
        assert s.multicore == 8
        assert s.memory_mb == 16000
        assert s.event_streams == 8
        assert s.request_num_events == 5000
        assert s.keep_output is False
        assert s.mc_pileup == "/MinBias/Run3/GEN-SIM"
        assert s.data_pileup == "/ZeroBias/Run2024/RAW"

    def test_single_step(self):
        """StepChain with 1 step."""
        data = {
            "StepChain": 1,
            "CMSSWVersion": "CMSSW_14_0_0",
            "ScramArch": "el9_amd64_gcc12",
            "Step1": {"StepName": "RECO"},
        }
        spec = parse_stepchain(data)
        assert len(spec.steps) == 1
        assert spec.steps[0].name == "RECO"

    def test_four_step_chain(self):
        """Real 4-step chain from NPS request."""
        data = {
            "RequestName": "NPS-Run3Summer22EEGS-00049",
            "StepChain": 4,
            "CMSSWVersion": "CMSSW_12_4_25",
            "ScramArch": "el8_amd64_gcc10",
            "GlobalTag": "124X_mcRun3_2022_realistic_postEE_v1",
            "Multicore": 4,
            "Memory": 8000,
            "TimePerEvent": 5.0,
            "SizePerEvent": 2.5,
            "FilterEfficiency": 1.0,
            "RequestNumEvents": 100,
            "Step1": {
                "StepName": "GEN-SIM",
                "CMSSWVersion": "CMSSW_12_4_25",
                "ScramArch": "el8_amd64_gcc10",
                "GlobalTag": "124X_mcRun3_2022_realistic_postEE_v1",
                "Multicore": 4,
                "Memory": 8000,
                "RequestNumEvents": 100,
            },
            "Step2": {
                "StepName": "DIGI-RECO",
                "CMSSWVersion": "CMSSW_12_4_25",
                "ScramArch": "el8_amd64_gcc10",
                "InputStep": "GEN-SIM",
                "InputFromOutputModule": "RAWSIMoutput",
                "MCPileup": "/MinBias_TuneCP5_13p6TeV-pythia8/Run3Summer22EEGS-124X/GEN-SIM",
            },
            "Step3": {
                "StepName": "MiniAOD",
                "CMSSWVersion": "CMSSW_13_0_23",
                "ScramArch": "el8_amd64_gcc11",
                "InputStep": "DIGI-RECO",
                "InputFromOutputModule": "AODSIMoutput",
                "Multicore": 2,
                "Memory": 6000,
            },
            "Step4": {
                "StepName": "NanoAOD",
                "CMSSWVersion": "CMSSW_13_0_23",
                "ScramArch": "el8_amd64_gcc11",
                "InputStep": "MiniAOD",
                "InputFromOutputModule": "MINIAODSIMoutput",
                "Multicore": 2,
                "Memory": 4000,
            },
        }
        spec = parse_stepchain(data)
        assert spec.request_name == "NPS-Run3Summer22EEGS-00049"
        assert len(spec.steps) == 4
        assert spec.time_per_event == 5.0
        assert spec.size_per_event == 2.5

        # Step 1: GEN-SIM
        assert spec.steps[0].name == "GEN-SIM"
        assert spec.steps[0].cmssw_version == "CMSSW_12_4_25"
        assert spec.steps[0].scram_arch == "el8_amd64_gcc10"
        assert spec.steps[0].input_step == ""
        assert spec.steps[0].request_num_events == 100

        # Step 2: DIGI-RECO, same CMSSW, has pileup
        assert spec.steps[1].name == "DIGI-RECO"
        assert spec.steps[1].cmssw_version == "CMSSW_12_4_25"
        assert spec.steps[1].input_step == "GEN-SIM"
        assert spec.steps[1].mc_pileup.startswith("/MinBias")

        # Step 3: MiniAOD, different CMSSW
        assert spec.steps[2].name == "MiniAOD"
        assert spec.steps[2].cmssw_version == "CMSSW_13_0_23"
        assert spec.steps[2].scram_arch == "el8_amd64_gcc11"
        assert spec.steps[2].multicore == 2
        assert spec.steps[2].memory_mb == 6000
        assert spec.steps[2].input_step == "DIGI-RECO"

        # Step 4: NanoAOD, same CMSSW as step 3
        assert spec.steps[3].name == "NanoAOD"
        assert spec.steps[3].cmssw_version == "CMSSW_13_0_23"
        assert spec.steps[3].input_step == "MiniAOD"
        assert spec.steps[3].memory_mb == 4000

    def test_pileup_fields(self):
        """MCPileup and DataPileup propagation."""
        data = {
            "StepChain": 2,
            "CMSSWVersion": "CMSSW_14_0_0",
            "Step1": {
                "StepName": "GEN",
            },
            "Step2": {
                "StepName": "DIGI",
                "MCPileup": "/MinBias/Run3/GEN-SIM",
                "DataPileup": "/ZeroBias/Run2024/RAW",
                "InputStep": "GEN",
            },
        }
        spec = parse_stepchain(data)
        assert spec.steps[0].mc_pileup == ""
        assert spec.steps[0].data_pileup == ""
        assert spec.steps[1].mc_pileup == "/MinBias/Run3/GEN-SIM"
        assert spec.steps[1].data_pileup == "/ZeroBias/Run2024/RAW"

    def test_input_chaining(self):
        """InputStep and InputFromOutputModule."""
        data = {
            "StepChain": 3,
            "CMSSWVersion": "CMSSW_14_0_0",
            "Step1": {"StepName": "GEN"},
            "Step2": {
                "StepName": "SIM",
                "InputStep": "GEN",
                "InputFromOutputModule": "RAWSIMoutput",
            },
            "Step3": {
                "StepName": "DIGI",
                "InputStep": "SIM",
                "InputFromOutputModule": "SIMoutput",
            },
        }
        spec = parse_stepchain(data)
        assert spec.steps[0].input_step == ""
        assert spec.steps[0].input_from_output_module == ""
        assert spec.steps[1].input_step == "GEN"
        assert spec.steps[1].input_from_output_module == "RAWSIMoutput"
        assert spec.steps[2].input_step == "SIM"
        assert spec.steps[2].input_from_output_module == "SIMoutput"

    def test_manifest_roundtrip(self):
        """to_manifest() serializes and can be read back."""
        data = {
            "RequestName": "roundtrip_test",
            "StepChain": 2,
            "CMSSWVersion": "CMSSW_14_0_0",
            "ScramArch": "el9_amd64_gcc12",
            "GlobalTag": "GT_TEST",
            "Multicore": 4,
            "Memory": 8000,
            "TimePerEvent": 1.5,
            "SizePerEvent": 2.0,
            "FilterEfficiency": 0.8,
            "Step1": {
                "StepName": "GEN-SIM",
                "CMSSWVersion": "CMSSW_12_4_25",
                "ScramArch": "el8_amd64_gcc10",
                "MCPileup": "/PU/dataset/GEN-SIM",
            },
            "Step2": {
                "StepName": "DIGI",
                "InputStep": "GEN-SIM",
                "InputFromOutputModule": "RAWSIMoutput",
            },
        }
        spec = parse_stepchain(data)
        manifest = to_manifest(spec)

        assert manifest["mode"] == "cmssw"
        assert manifest["request_name"] == "roundtrip_test"
        assert len(manifest["steps"]) == 2

        # Verify step data survives serialization
        s1 = manifest["steps"][0]
        assert s1["name"] == "GEN-SIM"
        assert s1["cmssw_version"] == "CMSSW_12_4_25"
        assert s1["scram_arch"] == "el8_amd64_gcc10"
        assert s1["mc_pileup"] == "/PU/dataset/GEN-SIM"
        assert s1["pset"] == "steps/step1/cfg.py"

        s2 = manifest["steps"][1]
        assert s2["name"] == "DIGI"
        assert s2["cmssw_version"] == "CMSSW_14_0_0"
        assert s2["input_step"] == "GEN-SIM"
        assert s2["input_from_output_module"] == "RAWSIMoutput"

    def test_default_step_name(self):
        """Steps without StepName get a default."""
        data = {
            "StepChain": 1,
            "CMSSWVersion": "CMSSW_14_0_0",
            "Step1": {},
        }
        spec = parse_stepchain(data)
        assert spec.steps[0].name == "step1"

    def test_defaults_when_minimal(self):
        """Minimal request — everything uses defaults."""
        data = {
            "StepChain": 1,
            "Step1": {"StepName": "TEST"},
        }
        spec = parse_stepchain(data)
        s = spec.steps[0]
        assert s.cmssw_version == ""
        assert s.scram_arch == ""
        assert s.global_tag == ""
        assert s.multicore == 1
        assert s.memory_mb == 2048
        assert s.keep_output is True
        assert s.event_streams == 0
        assert s.request_num_events == 0
        assert spec.time_per_event == 1.0
        assert spec.size_per_event == 1.5
        assert spec.filter_efficiency == 1.0
