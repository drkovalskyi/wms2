"""Tests for DAG file generation."""

import json
import os
from pathlib import Path

import pytest

from wms2.core.dag_planner import (
    PlanningMergeGroup,
    _generate_dag_files,
    _generate_group_dag,
    _plan_merge_groups,
)
from wms2.core.splitters import DAGNodeSpec, InputFile


def _make_node(index, events=10000, location="T1_US_FNAL",
               first_event=0, last_event=0, events_per_job=0):
    return DAGNodeSpec(
        node_index=index,
        input_files=[
            InputFile(
                lfn=f"/store/file_{index}.root",
                file_size=1_000_000_000,
                event_count=events,
                locations=[location],
            )
        ],
        primary_location=location,
        first_event=first_event,
        last_event=last_event,
        events_per_job=events_per_job,
    )


def _make_merge_groups(num_groups=2, nodes_per_group=3):
    groups = []
    node_idx = 0
    for g in range(num_groups):
        nodes = [_make_node(node_idx + i) for i in range(nodes_per_group)]
        mg = PlanningMergeGroup(
            group_index=g,
            processing_nodes=nodes,
        )
        groups.append(mg)
        node_idx += nodes_per_group
    return groups


class TestDAGFileGeneration:
    def test_outer_dag_created(self, tmp_path):
        groups = _make_merge_groups(2, 3)
        dag_path = _generate_dag_files(
            submit_dir=str(tmp_path),
            workflow_id="test-wf-001",
            merge_groups=groups,
            sandbox_url="https://example.com/sandbox.tar.gz",
            category_throttles={"Processing": 5000, "Merge": 100, "Cleanup": 50},
        )
        assert os.path.exists(dag_path)
        assert dag_path.endswith("workflow.dag")

    def test_outer_dag_content(self, tmp_path):
        groups = _make_merge_groups(2, 3)
        dag_path = _generate_dag_files(
            submit_dir=str(tmp_path),
            workflow_id="test-wf-001",
            merge_groups=groups,
            sandbox_url="https://example.com/sandbox.tar.gz",
            category_throttles={"Processing": 5000, "Merge": 100, "Cleanup": 50},
        )
        content = Path(dag_path).read_text()
        assert "CONFIG" in content
        assert "SUBDAG EXTERNAL mg_000000" in content
        assert "SUBDAG EXTERNAL mg_000001" in content
        assert "CATEGORY mg_000000 MergeGroup" in content
        assert "MAXJOBS MergeGroup 10" in content

    def test_group_dag_created(self, tmp_path):
        groups = _make_merge_groups(1, 3)
        _generate_dag_files(
            submit_dir=str(tmp_path),
            workflow_id="test-wf-001",
            merge_groups=groups,
            sandbox_url="https://example.com/sandbox.tar.gz",
            category_throttles={"Processing": 5000, "Merge": 100, "Cleanup": 50},
        )
        group_dag = tmp_path / "mg_000000" / "group.dag"
        assert group_dag.exists()

    def test_group_dag_content(self, tmp_path):
        groups = _make_merge_groups(1, 3)
        _generate_dag_files(
            submit_dir=str(tmp_path),
            workflow_id="test-wf-001",
            merge_groups=groups,
            sandbox_url="https://example.com/sandbox.tar.gz",
            category_throttles={"Processing": 5000, "Merge": 100, "Cleanup": 50},
        )
        content = (tmp_path / "mg_000000" / "group.dag").read_text()
        # Appendix C structure checks
        assert "JOB landing landing.sub" in content
        assert "SCRIPT POST landing" in content
        assert "elect_site.sh" in content
        assert "JOB proc_000000 proc_000000.sub" in content
        assert "JOB merge merge.sub" in content
        assert "JOB cleanup cleanup.sub" in content
        assert "PARENT landing CHILD" in content
        assert "CHILD merge" in content
        assert "PARENT merge CHILD cleanup" in content
        assert "RETRY proc_000000 3 UNLESS-EXIT 2" in content
        assert "RETRY merge 2 UNLESS-EXIT 2" in content
        assert "RETRY cleanup 1" in content
        assert "CATEGORY proc_000000 Processing" in content
        assert "CATEGORY merge Merge" in content
        assert "CATEGORY cleanup Cleanup" in content
        assert "MAXJOBS Processing 5000" in content

    def test_submit_files_created(self, tmp_path):
        groups = _make_merge_groups(1, 2)
        _generate_dag_files(
            submit_dir=str(tmp_path),
            workflow_id="test-wf-001",
            merge_groups=groups,
            sandbox_url="https://example.com/sandbox.tar.gz",
            category_throttles={"Processing": 5000, "Merge": 100, "Cleanup": 50},
        )
        mg_dir = tmp_path / "mg_000000"
        assert (mg_dir / "landing.sub").exists()
        assert (mg_dir / "proc_000000.sub").exists()
        assert (mg_dir / "proc_000001.sub").exists()
        assert (mg_dir / "merge.sub").exists()
        assert (mg_dir / "cleanup.sub").exists()

    def test_site_scripts_created(self, tmp_path):
        groups = _make_merge_groups(1, 1)
        _generate_dag_files(
            submit_dir=str(tmp_path),
            workflow_id="test-wf-001",
            merge_groups=groups,
            sandbox_url="https://example.com/sandbox.tar.gz",
            category_throttles={"Processing": 5000, "Merge": 100, "Cleanup": 50},
        )
        assert (tmp_path / "elect_site.sh").exists()
        assert (tmp_path / "pin_site.sh").exists()
        assert (tmp_path / "post_script.sh").exists()
        # Check executable
        assert os.access(str(tmp_path / "elect_site.sh"), os.X_OK)
        assert os.access(str(tmp_path / "pin_site.sh"), os.X_OK)

    def test_dagman_config_created(self, tmp_path):
        groups = _make_merge_groups(1, 1)
        _generate_dag_files(
            submit_dir=str(tmp_path),
            workflow_id="test-wf-001",
            merge_groups=groups,
            sandbox_url="https://example.com/sandbox.tar.gz",
            category_throttles={"Processing": 5000, "Merge": 100, "Cleanup": 50},
        )
        config = tmp_path / "dagman.config"
        assert config.exists()
        content = config.read_text()
        assert "DAGMAN_MAX_RESCUE_NUM" in content

    def test_submit_file_content(self, tmp_path):
        groups = _make_merge_groups(1, 1)
        _generate_dag_files(
            submit_dir=str(tmp_path),
            workflow_id="test-wf-001",
            merge_groups=groups,
            sandbox_url="https://example.com/sandbox.tar.gz",
            category_throttles={"Processing": 5000, "Merge": 100, "Cleanup": 50},
        )
        content = (tmp_path / "mg_000000" / "proc_000000.sub").read_text()
        assert "universe = vanilla" in content
        assert "queue 1" in content

    def test_wrapper_scripts_generated(self, tmp_path):
        """wms2_proc.sh, wms2_merge.py, and wms2_cleanup.py are always generated."""
        groups = _make_merge_groups(1, 1)
        _generate_dag_files(
            submit_dir=str(tmp_path),
            workflow_id="test-wf-001",
            merge_groups=groups,
            sandbox_url="https://example.com/sandbox.tar.gz",
            category_throttles={"Processing": 5000, "Merge": 100, "Cleanup": 50},
        )
        assert (tmp_path / "wms2_proc.sh").exists()
        assert (tmp_path / "wms2_merge.py").exists()
        assert (tmp_path / "wms2_cleanup.py").exists()
        assert os.access(str(tmp_path / "wms2_proc.sh"), os.X_OK)
        assert os.access(str(tmp_path / "wms2_merge.py"), os.X_OK)
        assert os.access(str(tmp_path / "wms2_cleanup.py"), os.X_OK)

    def test_test_mode_uses_wrapper_scripts(self, tmp_path):
        """When executables are /bin/true, submit files use generated scripts."""
        groups = _make_merge_groups(1, 1)
        executables = {
            "processing": "/bin/true",
            "merge": "/bin/true",
            "cleanup": "/bin/true",
        }
        _generate_dag_files(
            submit_dir=str(tmp_path),
            workflow_id="test-wf-001",
            merge_groups=groups,
            sandbox_url="https://example.com/sandbox.tar.gz",
            category_throttles={"Processing": 5000, "Merge": 100, "Cleanup": 50},
            executables=executables,
        )
        proc_sub = (tmp_path / "mg_000000" / "proc_000000.sub").read_text()
        assert "wms2_proc.sh" in proc_sub
        assert "/bin/true" not in proc_sub

        merge_sub = (tmp_path / "mg_000000" / "merge.sub").read_text()
        assert "wms2_merge.py" in merge_sub
        assert "/bin/true" not in merge_sub

        # Cleanup now also uses generated script
        cleanup_sub = (tmp_path / "mg_000000" / "cleanup.sub").read_text()
        assert "wms2_cleanup.py" in cleanup_sub
        assert "/bin/true" not in cleanup_sub

    def test_output_info_json_format(self, tmp_path):
        """output_info.json includes dataset names, tiers, and LFN bases."""
        groups = _make_merge_groups(1, 1)
        output_datasets = [
            {
                "dataset_name": "/Primary/Era-Proc-v1/AODSIM",
                "merged_lfn_base": "/store/mc/Era/Primary/AODSIM/Proc-v1",
                "unmerged_lfn_base": "/store/unmerged/Era/Primary/AODSIM/Proc-v1",
                "data_tier": "AODSIM",
            },
        ]
        _generate_dag_files(
            submit_dir=str(tmp_path),
            workflow_id="test-wf-001",
            merge_groups=groups,
            sandbox_url="https://example.com/sandbox.tar.gz",
            category_throttles={"Processing": 5000, "Merge": 100, "Cleanup": 50},
            output_datasets=output_datasets,
            local_pfn_prefix="/mnt/shared",
        )
        info = json.loads((tmp_path / "mg_000000" / "output_info.json").read_text())
        assert "output_datasets" in info
        assert info["output_datasets"][0]["dataset_name"] == "/Primary/Era-Proc-v1/AODSIM"
        assert info["output_datasets"][0]["data_tier"] == "AODSIM"
        assert info["output_datasets"][0]["merged_lfn_base"] == "/store/mc/Era/Primary/AODSIM/Proc-v1"
        assert info["output_datasets"][0]["unmerged_lfn_base"] == "/store/unmerged/Era/Primary/AODSIM/Proc-v1"
        assert info["local_pfn_prefix"] == "/mnt/shared"
        assert info["group_index"] == 0
        # Must not have old field
        assert "output_base_dir" not in info

    def test_proc_transfer_includes_output_info(self, tmp_path):
        """Proc submit files include output_info.json in transfer_input_files."""
        groups = _make_merge_groups(1, 1)
        output_datasets = [
            {
                "dataset_name": "/Primary/Era-Proc-v1/AODSIM",
                "merged_lfn_base": "/store/mc/Era/Primary/AODSIM/Proc-v1",
                "unmerged_lfn_base": "/store/unmerged/Era/Primary/AODSIM/Proc-v1",
                "data_tier": "AODSIM",
            },
        ]
        _generate_dag_files(
            submit_dir=str(tmp_path),
            workflow_id="test-wf-001",
            merge_groups=groups,
            sandbox_url="https://example.com/sandbox.tar.gz",
            category_throttles={"Processing": 5000, "Merge": 100, "Cleanup": 50},
            output_datasets=output_datasets,
            local_pfn_prefix="/mnt/shared",
        )
        proc_sub = (tmp_path / "mg_000000" / "proc_000000.sub").read_text()
        assert "output_info.json" in proc_sub

    def test_proc_args_include_output_info(self, tmp_path):
        """Proc submit files include --output-info in arguments."""
        groups = _make_merge_groups(1, 1)
        output_datasets = [
            {
                "dataset_name": "/Primary/Era-Proc-v1/AODSIM",
                "merged_lfn_base": "/store/mc/Era/Primary/AODSIM/Proc-v1",
                "unmerged_lfn_base": "/store/unmerged/Era/Primary/AODSIM/Proc-v1",
                "data_tier": "AODSIM",
            },
        ]
        _generate_dag_files(
            submit_dir=str(tmp_path),
            workflow_id="test-wf-001",
            merge_groups=groups,
            sandbox_url="https://example.com/sandbox.tar.gz",
            category_throttles={"Processing": 5000, "Merge": 100, "Cleanup": 50},
            output_datasets=output_datasets,
            local_pfn_prefix="/mnt/shared",
        )
        proc_sub = (tmp_path / "mg_000000" / "proc_000000.sub").read_text()
        assert "--output-info output_info.json" in proc_sub


class TestProcessingWrapper:
    """Tests for the processing wrapper script content."""

    def _get_proc_content(self, tmp_path):
        groups = _make_merge_groups(1, 1)
        _generate_dag_files(
            submit_dir=str(tmp_path),
            workflow_id="test-wf-001",
            merge_groups=groups,
            sandbox_url="https://example.com/sandbox.tar.gz",
            category_throttles={"Processing": 5000, "Merge": 100, "Cleanup": 50},
        )
        return (tmp_path / "wms2_proc.sh").read_text()

    def test_proc_script_has_mode_dispatch(self, tmp_path):
        """Processing wrapper supports CMSSW and synthetic modes."""
        content = self._get_proc_content(tmp_path)
        assert "run_cmssw_mode" in content
        assert "run_synthetic_mode" in content
        assert "run_pilot_mode" in content
        assert "--sandbox" in content
        assert "manifest.json" in content

    def test_proc_script_has_sandbox_extraction(self, tmp_path):
        content = self._get_proc_content(tmp_path)
        assert "tar xzf" in content

    def test_proc_script_has_pilot_mode(self, tmp_path):
        content = self._get_proc_content(tmp_path)
        assert "--pilot" in content
        assert "pilot_metrics.json" in content

    def test_proc_script_per_step_cmssw(self, tmp_path):
        """Processing wrapper reads per-step CMSSW version from manifest."""
        content = self._get_proc_content(tmp_path)
        # Should read cmssw_version from each step, not top-level
        assert "cmssw_version" in content
        assert "scram_arch" in content
        assert "CMSSW_VER" in content
        assert "STEP_ARCH" in content

    def test_proc_script_apptainer_support(self, tmp_path):
        """Processing wrapper has apptainer container support."""
        content = self._get_proc_content(tmp_path)
        assert "resolve_container" in content
        assert "apptainer" in content
        assert "run_step_native" in content
        assert "run_step_apptainer" in content
        assert "detect_host_os" in content

    def test_proc_script_nthreads(self, tmp_path):
        """Processing wrapper injects nThreads into PSet at runtime."""
        content = self._get_proc_content(tmp_path)
        assert "NTHREADS" in content
        assert "numberOfThreads" in content

    def test_proc_script_maxevents_first_step_only(self, tmp_path):
        """maxEvents and firstEvent only applied to first step."""
        content = self._get_proc_content(tmp_path)
        # maxEvents should be conditional on step_idx == 0
        assert "step_idx -eq 0" in content
        assert "maxEvents" in content
        assert "firstEvent" in content

    def test_proc_script_fjr_strips_file_prefix(self, tmp_path):
        """FJR output parsing strips file: prefix from PFN."""
        content = self._get_proc_content(tmp_path)
        assert "file:" in content
        assert "pfn[5:]" in content or "pfn.startswith('file:')" in content

    def test_proc_script_has_stage_out(self, tmp_path):
        """Processing wrapper has stage-out to unmerged site storage."""
        content = self._get_proc_content(tmp_path)
        assert "stage_out_to_unmerged" in content
        assert "OUTPUT_INFO" in content
        assert "--output-info" in content
        assert "local_pfn_prefix" in content
        assert "unmerged_lfn_base" in content

    def test_proc_script_output_info_arg(self, tmp_path):
        """Processing wrapper parses --output-info argument."""
        content = self._get_proc_content(tmp_path)
        assert 'OUTPUT_INFO=""' in content
        assert "--output-info)" in content


class TestMergeWrapper:
    """Tests for the merge wrapper script content."""

    def test_merge_script_handles_root_files(self, tmp_path):
        groups = _make_merge_groups(1, 1)
        _generate_dag_files(
            submit_dir=str(tmp_path),
            workflow_id="test-wf-001",
            merge_groups=groups,
            sandbox_url="https://example.com/sandbox.tar.gz",
            category_throttles={"Processing": 5000, "Merge": 100, "Cleanup": 50},
        )
        content = (tmp_path / "wms2_merge.py").read_text()
        assert "root_files" in content
        assert "mergeProcess" in content
        assert "cmsRun" in content
        assert "merge_root_tier" in content
        assert "write_merge_pset" in content
        assert "batch_by_size" in content

    def test_merge_script_has_text_fallback(self, tmp_path):
        groups = _make_merge_groups(1, 1)
        _generate_dag_files(
            submit_dir=str(tmp_path),
            workflow_id="test-wf-001",
            merge_groups=groups,
            sandbox_url="https://example.com/sandbox.tar.gz",
            category_throttles={"Processing": 5000, "Merge": 100, "Cleanup": 50},
        )
        content = (tmp_path / "wms2_merge.py").read_text()
        assert "merge_text_files" in content
        assert "proc_*.out" in content

    def test_merge_script_uses_lfn_to_pfn(self, tmp_path):
        """Merge script uses lfn_to_pfn for path construction."""
        groups = _make_merge_groups(1, 1)
        _generate_dag_files(
            submit_dir=str(tmp_path),
            workflow_id="test-wf-001",
            merge_groups=groups,
            sandbox_url="https://example.com/sandbox.tar.gz",
            category_throttles={"Processing": 5000, "Merge": 100, "Cleanup": 50},
        )
        content = (tmp_path / "wms2_merge.py").read_text()
        assert "def lfn_to_pfn(" in content
        assert "local_pfn_prefix" in content
        # Must not have old lstrip("/") path construction bug
        # The merge script should use lfn_to_pfn() not manual lstrip

    def test_merge_script_reads_from_unmerged(self, tmp_path):
        """Merge script reads proc outputs from unmerged site storage."""
        groups = _make_merge_groups(1, 1)
        _generate_dag_files(
            submit_dir=str(tmp_path),
            workflow_id="test-wf-001",
            merge_groups=groups,
            sandbox_url="https://example.com/sandbox.tar.gz",
            category_throttles={"Processing": 5000, "Merge": 100, "Cleanup": 50},
        )
        content = (tmp_path / "wms2_merge.py").read_text()
        assert "unmerged_lfn_base" in content
        assert "unmerged_root_files" in content
        assert "has_unmerged" in content

    def test_merge_script_writes_cleanup_manifest(self, tmp_path):
        """Merge script writes cleanup_manifest.json for cleanup job."""
        groups = _make_merge_groups(1, 1)
        _generate_dag_files(
            submit_dir=str(tmp_path),
            workflow_id="test-wf-001",
            merge_groups=groups,
            sandbox_url="https://example.com/sandbox.tar.gz",
            category_throttles={"Processing": 5000, "Merge": 100, "Cleanup": 50},
        )
        content = (tmp_path / "wms2_merge.py").read_text()
        assert "cleanup_manifest.json" in content
        assert "cleanup_dirs" in content

    def test_merge_pset_adds_file_prefix(self, tmp_path):
        """write_merge_pset adds file: prefix to local paths for CMSSW PoolSource."""
        groups = _make_merge_groups(1, 1)
        _generate_dag_files(
            submit_dir=str(tmp_path),
            workflow_id="test-wf-001",
            merge_groups=groups,
            sandbox_url="https://example.com/sandbox.tar.gz",
            category_throttles={"Processing": 5000, "Merge": 100, "Cleanup": 50},
        )
        content = (tmp_path / "wms2_merge.py").read_text()
        # write_merge_pset must add file: prefix so CMSSW treats paths as local PFN
        assert 'f"file:{f}"' in content or "file:" in content
        assert 'f.startswith("file:")' in content
        assert 'output_file.startswith("file:")' in content

    def test_hadd_runs_inside_cmssw_env(self, tmp_path):
        """merge_root_with_hadd runs hadd inside CMSSW runtime (not bare binary)."""
        groups = _make_merge_groups(1, 1)
        _generate_dag_files(
            submit_dir=str(tmp_path),
            workflow_id="test-wf-001",
            merge_groups=groups,
            sandbox_url="https://example.com/sandbox.tar.gz",
            category_throttles={"Processing": 5000, "Merge": 100, "Cleanup": 50},
        )
        content = (tmp_path / "wms2_merge.py").read_text()
        # hadd must run inside full CMSSW env, not as a bare binary
        assert "scramv1 runtime -sh" in content
        assert "import shlex" in content
        # Must use apptainer when cross-OS
        assert "resolve_container(scram_arch)" in content

    def test_cmsrun_merge_binds_site_cfg(self, tmp_path):
        """run_cmsrun binds SITECONFIG_PATH into apptainer and sets CMS_PATH."""
        groups = _make_merge_groups(1, 1)
        _generate_dag_files(
            submit_dir=str(tmp_path),
            workflow_id="test-wf-001",
            merge_groups=groups,
            sandbox_url="https://example.com/sandbox.tar.gz",
            category_throttles={"Processing": 5000, "Merge": 100, "Cleanup": 50},
        )
        content = (tmp_path / "wms2_merge.py").read_text()
        # Must bind site_cfg dir into apptainer
        assert "bind_paths += \",\" + site_cfg" in content
        # Must export CMS_PATH for older CMSSW versions
        assert "export CMS_PATH=" in content
        # Must create SITECONF/local/ layout
        assert "SITECONF" in content
        assert "local" in content


class TestCleanupWrapper:
    """Tests for the cleanup wrapper script content."""

    def test_cleanup_script_generated(self, tmp_path):
        groups = _make_merge_groups(1, 1)
        _generate_dag_files(
            submit_dir=str(tmp_path),
            workflow_id="test-wf-001",
            merge_groups=groups,
            sandbox_url="https://example.com/sandbox.tar.gz",
            category_throttles={"Processing": 5000, "Merge": 100, "Cleanup": 50},
        )
        assert (tmp_path / "wms2_cleanup.py").exists()
        assert os.access(str(tmp_path / "wms2_cleanup.py"), os.X_OK)

    def test_cleanup_script_reads_manifest(self, tmp_path):
        groups = _make_merge_groups(1, 1)
        _generate_dag_files(
            submit_dir=str(tmp_path),
            workflow_id="test-wf-001",
            merge_groups=groups,
            sandbox_url="https://example.com/sandbox.tar.gz",
            category_throttles={"Processing": 5000, "Merge": 100, "Cleanup": 50},
        )
        content = (tmp_path / "wms2_cleanup.py").read_text()
        assert "cleanup_manifest.json" in content
        assert "shutil.rmtree" in content
        assert "unmerged_dirs" in content

    def test_cleanup_submit_has_output_info(self, tmp_path):
        """Cleanup submit file references output_info.json."""
        groups = _make_merge_groups(1, 1)
        output_datasets = [
            {
                "dataset_name": "/Primary/Era-Proc-v1/AODSIM",
                "merged_lfn_base": "/store/mc/Era/Primary/AODSIM/Proc-v1",
                "unmerged_lfn_base": "/store/unmerged/Era/Primary/AODSIM/Proc-v1",
                "data_tier": "AODSIM",
            },
        ]
        _generate_dag_files(
            submit_dir=str(tmp_path),
            workflow_id="test-wf-001",
            merge_groups=groups,
            sandbox_url="https://example.com/sandbox.tar.gz",
            category_throttles={"Processing": 5000, "Merge": 100, "Cleanup": 50},
            output_datasets=output_datasets,
            local_pfn_prefix="/mnt/shared",
        )
        cleanup_sub = (tmp_path / "mg_000000" / "cleanup.sub").read_text()
        assert "--output-info" in cleanup_sub
        assert "output_info.json" in cleanup_sub


class TestResourceRequests:
    """Tests for resource requests in submit files."""

    def test_resource_params_in_submit_file(self, tmp_path):
        groups = _make_merge_groups(1, 1)
        _generate_dag_files(
            submit_dir=str(tmp_path),
            workflow_id="test-wf-001",
            merge_groups=groups,
            sandbox_url="https://example.com/sandbox.tar.gz",
            category_throttles={"Processing": 5000, "Merge": 100, "Cleanup": 50},
            resource_params={"memory_mb": 4096, "disk_kb": 20_000_000},
        )
        content = (tmp_path / "mg_000000" / "proc_000000.sub").read_text()
        assert "request_memory = 4096" in content
        assert "request_disk = 20000000" in content

    def test_resource_params_with_ncpus(self, tmp_path):
        groups = _make_merge_groups(1, 1)
        _generate_dag_files(
            submit_dir=str(tmp_path),
            workflow_id="test-wf-001",
            merge_groups=groups,
            sandbox_url="https://example.com/sandbox.tar.gz",
            category_throttles={"Processing": 5000, "Merge": 100, "Cleanup": 50},
            resource_params={"memory_mb": 16000, "ncpus": 8},
        )
        content = (tmp_path / "mg_000000" / "proc_000000.sub").read_text()
        assert "request_cpus = 8" in content
        assert "request_memory = 16000" in content

    def test_no_resource_params_omits_lines(self, tmp_path):
        groups = _make_merge_groups(1, 1)
        _generate_dag_files(
            submit_dir=str(tmp_path),
            workflow_id="test-wf-001",
            merge_groups=groups,
            sandbox_url="https://example.com/sandbox.tar.gz",
            category_throttles={"Processing": 5000, "Merge": 100, "Cleanup": 50},
        )
        content = (tmp_path / "mg_000000" / "proc_000000.sub").read_text()
        assert "request_memory" not in content
        assert "request_disk" not in content
        assert "request_cpus" not in content

    def test_transfer_input_files(self, tmp_path):
        # Create a fake sandbox file so the path check passes
        sandbox = tmp_path / "sandbox.tar.gz"
        sandbox.write_bytes(b"fake")

        groups = _make_merge_groups(1, 1)
        _generate_dag_files(
            submit_dir=str(tmp_path),
            workflow_id="test-wf-001",
            merge_groups=groups,
            sandbox_url="https://example.com/sandbox.tar.gz",
            category_throttles={"Processing": 5000, "Merge": 100, "Cleanup": 50},
            sandbox_path=str(sandbox),
        )
        content = (tmp_path / "mg_000000" / "proc_000000.sub").read_text()
        assert "transfer_input_files" in content
        assert "sandbox.tar.gz" in content


class TestEventRangeArgs:
    """Tests for event range arguments in processing node submit files."""

    def test_event_range_in_arguments(self, tmp_path):
        node = _make_node(0, events=50000, first_event=1, last_event=50000,
                          events_per_job=50000)
        mg = PlanningMergeGroup(
            group_index=0,
            processing_nodes=[node],
        )
        _generate_dag_files(
            submit_dir=str(tmp_path),
            workflow_id="test-wf-001",
            merge_groups=[mg],
            sandbox_url="https://example.com/sandbox.tar.gz",
            category_throttles={"Processing": 5000, "Merge": 100, "Cleanup": 50},
        )
        content = (tmp_path / "mg_000000" / "proc_000000.sub").read_text()
        assert "--first-event 1" in content
        assert "--last-event 50000" in content
        assert "--events-per-job 50000" in content
        assert "--node-index 0" in content

    def test_node_index_always_present(self, tmp_path):
        groups = _make_merge_groups(1, 1)
        _generate_dag_files(
            submit_dir=str(tmp_path),
            workflow_id="test-wf-001",
            merge_groups=groups,
            sandbox_url="https://example.com/sandbox.tar.gz",
            category_throttles={"Processing": 5000, "Merge": 100, "Cleanup": 50},
        )
        content = (tmp_path / "mg_000000" / "proc_000000.sub").read_text()
        assert "--node-index 0" in content
