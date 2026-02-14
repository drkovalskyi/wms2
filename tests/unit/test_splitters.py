"""Tests for job splitters."""

import pytest

from wms2.core.splitters import (
    EventBasedSplitter,
    FileBasedSplitter,
    InputFile,
    get_splitter,
)


def _make_files(count, events=10000, size=1_000_000_000, locations=None):
    return [
        InputFile(
            lfn=f"/store/data/file_{i}.root",
            file_size=size,
            event_count=events,
            locations=locations or ["T1_US_FNAL", "T2_US_MIT"],
        )
        for i in range(count)
    ]


class TestFileBasedSplitter:
    def test_basic_split(self):
        files = _make_files(10)
        splitter = FileBasedSplitter(files_per_job=5)
        nodes = splitter.split(files)
        assert len(nodes) == 2
        assert len(nodes[0].input_files) == 5
        assert len(nodes[1].input_files) == 5

    def test_partial_batch(self):
        files = _make_files(7)
        splitter = FileBasedSplitter(files_per_job=5)
        nodes = splitter.split(files)
        assert len(nodes) == 2
        assert len(nodes[0].input_files) == 5
        assert len(nodes[1].input_files) == 2

    def test_single_file(self):
        files = _make_files(1)
        splitter = FileBasedSplitter(files_per_job=5)
        nodes = splitter.split(files)
        assert len(nodes) == 1
        assert len(nodes[0].input_files) == 1

    def test_empty_files(self):
        splitter = FileBasedSplitter(files_per_job=5)
        nodes = splitter.split([])
        assert nodes == []

    def test_files_per_job_one(self):
        files = _make_files(3)
        splitter = FileBasedSplitter(files_per_job=1)
        nodes = splitter.split(files)
        assert len(nodes) == 3
        for node in nodes:
            assert len(node.input_files) == 1

    def test_primary_location_set(self):
        files = _make_files(5, locations=["T1_US_FNAL"])
        splitter = FileBasedSplitter(files_per_job=5)
        nodes = splitter.split(files)
        assert nodes[0].primary_location == "T1_US_FNAL"

    def test_site_grouping(self):
        """Files at different sites should be grouped by site."""
        files_fnal = [
            InputFile(lfn=f"/store/fnal_{i}.root", file_size=100, event_count=100,
                      locations=["T1_US_FNAL"])
            for i in range(3)
        ]
        files_mit = [
            InputFile(lfn=f"/store/mit_{i}.root", file_size=100, event_count=100,
                      locations=["T2_US_MIT"])
            for i in range(3)
        ]
        splitter = FileBasedSplitter(files_per_job=10)
        nodes = splitter.split(files_fnal + files_mit)
        # Should create 2 nodes: one per site group
        assert len(nodes) == 2


class TestEventBasedSplitter:
    def test_basic_split(self):
        files = _make_files(2, events=150000)
        splitter = EventBasedSplitter(events_per_job=100000)
        nodes = splitter.split(files)
        # 300k events / 100k per job = 3 nodes
        assert len(nodes) == 3

    def test_exact_split(self):
        files = _make_files(2, events=100000)
        splitter = EventBasedSplitter(events_per_job=100000)
        nodes = splitter.split(files)
        assert len(nodes) == 2


class TestGetSplitter:
    def test_file_based(self):
        s = get_splitter("FileBased", {"files_per_job": 10})
        assert isinstance(s, FileBasedSplitter)
        assert s.files_per_job == 10

    def test_event_based(self):
        s = get_splitter("EventBased", {"events_per_job": 50000})
        assert isinstance(s, EventBasedSplitter)
        assert s.events_per_job == 50000

    def test_lumi_based_uses_file_splitter(self):
        s = get_splitter("LumiBased", {"files_per_job": 3})
        assert isinstance(s, FileBasedSplitter)

    def test_unknown_algo(self):
        with pytest.raises(ValueError, match="Unknown splitting"):
            get_splitter("InvalidAlgo", {})
