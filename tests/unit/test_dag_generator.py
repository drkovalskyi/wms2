"""Tests for DAG file generation."""

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


def _make_node(index, events=10000, location="T1_US_FNAL"):
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
    )


def _make_merge_groups(num_groups=2, nodes_per_group=3):
    groups = []
    node_idx = 0
    for g in range(num_groups):
        nodes = [_make_node(node_idx + i) for i in range(nodes_per_group)]
        mg = PlanningMergeGroup(
            group_index=g,
            processing_nodes=nodes,
            estimated_output_kb=500000.0,
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
        assert "executable = run_payload.sh" in content
        assert "queue 1" in content
