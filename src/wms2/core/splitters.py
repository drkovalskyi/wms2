"""Job splitters: pure stateless computation that groups input files into DAG nodes."""

from __future__ import annotations

import math
from dataclasses import dataclass, field


@dataclass
class InputFile:
    """An input file with its metadata and replica locations."""
    lfn: str
    file_size: int  # bytes
    event_count: int
    locations: list[str] = field(default_factory=list)


@dataclass
class DAGNodeSpec:
    """Specification for a single processing node in the DAG."""
    node_index: int
    input_files: list[InputFile]
    first_event: int = 0
    last_event: int = 0
    events_per_job: int = 0
    primary_location: str = ""

    @property
    def total_events(self) -> int:
        if self.events_per_job > 0:
            return self.events_per_job
        return sum(f.event_count for f in self.input_files)

    @property
    def total_size(self) -> int:
        return sum(f.file_size for f in self.input_files)


def _pick_primary_location(files: list[InputFile]) -> str:
    """Pick the site that has the most files in this group."""
    site_counts: dict[str, int] = {}
    for f in files:
        for site in f.locations:
            site_counts[site] = site_counts.get(site, 0) + 1
    if not site_counts:
        return ""
    return max(site_counts, key=lambda s: site_counts[s])


class FileBasedSplitter:
    """Split input files into batches of N files per job."""

    def __init__(self, files_per_job: int = 5):
        self.files_per_job = max(1, files_per_job)

    def split(self, files: list[InputFile]) -> list[DAGNodeSpec]:
        if not files:
            return []

        # Group files by their primary (most common) location for locality
        site_groups: dict[str, list[InputFile]] = {}
        for f in files:
            site = f.locations[0] if f.locations else "__any__"
            site_groups.setdefault(site, []).append(f)

        nodes: list[DAGNodeSpec] = []
        node_idx = 0
        for site, group_files in site_groups.items():
            for i in range(0, len(group_files), self.files_per_job):
                batch = group_files[i : i + self.files_per_job]
                location = _pick_primary_location(batch)
                nodes.append(
                    DAGNodeSpec(
                        node_index=node_idx,
                        input_files=batch,
                        primary_location=location,
                    )
                )
                node_idx += 1

        return nodes


class EventBasedSplitter:
    """Split input files into jobs by event count."""

    def __init__(self, events_per_job: int = 100_000):
        self.events_per_job = max(1, events_per_job)

    def split(self, files: list[InputFile]) -> list[DAGNodeSpec]:
        if not files:
            return []

        nodes: list[DAGNodeSpec] = []
        node_idx = 0
        current_files: list[InputFile] = []
        current_events = 0

        for f in files:
            remaining_in_file = f.event_count
            file_offset = 0

            while remaining_in_file > 0:
                space = self.events_per_job - current_events
                take = min(space, remaining_in_file)

                current_files.append(f)
                current_events += take
                file_offset += take
                remaining_in_file -= take

                if current_events >= self.events_per_job:
                    location = _pick_primary_location(current_files)
                    nodes.append(
                        DAGNodeSpec(
                            node_index=node_idx,
                            input_files=list({id(f): f for f in current_files}.values()),
                            first_event=max(0, file_offset - take),
                            last_event=file_offset,
                            events_per_job=self.events_per_job,
                            primary_location=location,
                        )
                    )
                    node_idx += 1
                    current_files = []
                    current_events = 0

        # Remaining partial batch
        if current_files:
            location = _pick_primary_location(current_files)
            nodes.append(
                DAGNodeSpec(
                    node_index=node_idx,
                    input_files=list({id(f): f for f in current_files}.values()),
                    first_event=0,
                    last_event=current_events,
                    events_per_job=current_events,
                    primary_location=location,
                )
            )

        return nodes


def get_splitter(algo: str, params: dict) -> FileBasedSplitter | EventBasedSplitter:
    """Factory: create the right splitter from algorithm name and params."""
    if algo in ("FileBased", "LumiBased"):
        return FileBasedSplitter(files_per_job=params.get("files_per_job", 5))
    elif algo in ("EventBased", "EventAwareLumiBased"):
        return EventBasedSplitter(events_per_job=params.get("events_per_job", 100_000))
    else:
        raise ValueError(f"Unknown splitting algorithm: {algo}")
