"""Tests for merge group planning (fixed job count)."""

from wms2.core.dag_planner import _plan_merge_groups
from wms2.core.splitters import DAGNodeSpec, InputFile


def _make_node(index, events=10000, size=1_000_000_000):
    return DAGNodeSpec(
        node_index=index,
        input_files=[
            InputFile(
                lfn=f"/store/file_{index}.root",
                file_size=size,
                event_count=events,
                locations=["T1_US_FNAL"],
            )
        ],
    )


class TestPlanMergeGroups:
    def test_single_group(self):
        """Fewer nodes than jobs_per_group → one group."""
        nodes = [_make_node(i) for i in range(5)]
        groups = _plan_merge_groups(nodes, jobs_per_group=8)
        assert len(groups) == 1
        assert len(groups[0].processing_nodes) == 5

    def test_multiple_groups(self):
        """Nodes evenly divisible by jobs_per_group."""
        nodes = [_make_node(i) for i in range(16)]
        groups = _plan_merge_groups(nodes, jobs_per_group=8)
        assert len(groups) == 2
        for g in groups:
            assert len(g.processing_nodes) == 8

    def test_remainder_group(self):
        """Last group smaller than jobs_per_group."""
        nodes = [_make_node(i) for i in range(10)]
        groups = _plan_merge_groups(nodes, jobs_per_group=8)
        assert len(groups) == 2
        assert len(groups[0].processing_nodes) == 8
        assert len(groups[1].processing_nodes) == 2

    def test_empty_nodes(self):
        """Empty input returns empty list."""
        groups = _plan_merge_groups([], jobs_per_group=8)
        assert groups == []

    def test_group_indices_sequential(self):
        """Group indices are 0, 1, 2, ..."""
        nodes = [_make_node(i) for i in range(20)]
        groups = _plan_merge_groups(nodes, jobs_per_group=8)
        for i, g in enumerate(groups):
            assert g.group_index == i

    def test_single_node_per_group(self):
        """jobs_per_group=1 creates one group per node."""
        nodes = [_make_node(i) for i in range(3)]
        groups = _plan_merge_groups(nodes, jobs_per_group=1)
        assert len(groups) == 3
        for g in groups:
            assert len(g.processing_nodes) == 1

    def test_exact_fit(self):
        """Exactly jobs_per_group nodes → one group."""
        nodes = [_make_node(i) for i in range(8)]
        groups = _plan_merge_groups(nodes, jobs_per_group=8)
        assert len(groups) == 1
        assert len(groups[0].processing_nodes) == 8
