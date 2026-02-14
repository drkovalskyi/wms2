"""Tests for merge group planning."""

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
        """All nodes fit in one group when output is small."""
        nodes = [_make_node(i, events=100) for i in range(5)]
        # 100 events * 50 kb/event = 5000 kb per node, 25000 kb total < 4GB
        groups = _plan_merge_groups(nodes, output_size_per_event_kb=50.0, target_kb=4_194_304)
        assert len(groups) == 1
        assert len(groups[0].processing_nodes) == 5

    def test_multiple_groups(self):
        """Nodes split into multiple groups when output exceeds target."""
        nodes = [_make_node(i, events=100000) for i in range(10)]
        # 100k events * 50 kb/event = 5_000_000 kb per node
        # target = 10_000_000 kb → 2 nodes per group → 5 groups
        groups = _plan_merge_groups(nodes, output_size_per_event_kb=50.0, target_kb=10_000_000)
        assert len(groups) == 5
        for g in groups:
            assert len(g.processing_nodes) == 2

    def test_group_boundary_exact(self):
        """When a node exactly fills a group, next node starts new group."""
        nodes = [_make_node(i, events=10000) for i in range(3)]
        # 10000 events * 100 kb = 1_000_000 kb per node, target = 1_000_000
        groups = _plan_merge_groups(nodes, output_size_per_event_kb=100.0, target_kb=1_000_000)
        # Each node exactly fills a group
        assert len(groups) == 3

    def test_empty_nodes(self):
        groups = _plan_merge_groups([], output_size_per_event_kb=50.0, target_kb=4_194_304)
        assert groups == []

    def test_group_indices_sequential(self):
        nodes = [_make_node(i, events=100000) for i in range(6)]
        groups = _plan_merge_groups(nodes, output_size_per_event_kb=50.0, target_kb=5_500_000)
        for i, g in enumerate(groups):
            assert g.group_index == i
