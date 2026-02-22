"""Data classes for the WMS2 test matrix."""

from __future__ import annotations

from dataclasses import dataclass, field


@dataclass(frozen=True)
class FaultSpec:
    """Describes a fault to inject into a DAG after planning."""

    target: str  # "proc" | "merge" | "cleanup"
    node_indices: tuple[int, ...] | None = None  # None = all nodes of that type
    exit_code: int = 0
    signal: int = 0
    delay_sec: int = 0
    skip_output: bool = False


@dataclass(frozen=True)
class VerifySpec:
    """Expected outcomes for a workflow run."""

    expect_success: bool = True
    expect_rescue_dag: bool = False
    expect_merged_outputs: bool = True
    expect_cleanup_ran: bool = True
    expect_dag_status: str | None = None


@dataclass(frozen=True)
class WorkflowDef:
    """A single numbered workflow in the test matrix."""

    wf_id: float
    title: str
    sandbox_mode: str  # "synthetic" | "cmssw" | "cached"
    request_spec: dict = field(default_factory=dict)
    events_per_job: int = 1
    num_jobs: int = 1
    output_datasets: list[dict] = field(default_factory=list)
    memory_mb: int = 512
    multicore: int = 1
    size: str = "small"  # "small" (<60s) | "medium" (<300s) | "large" (<900s)
    timeout_sec: int = 120
    requires: tuple[str, ...] = ("condor",)
    serial: bool = False
    fault: FaultSpec | None = None
    verify: VerifySpec = field(default_factory=VerifySpec)
    cached_sandbox_path: str = ""
    adaptive: bool = False
    num_work_units: int = 1
    adaptive_split: bool = True  # enable step 0 parallel splitting
    split_all_steps: bool = False  # all-step pipeline split (supersedes adaptive_split)
    split_uniform_threads: bool = False  # uniform nThreads across all steps in pipeline split
    overcommit_max: float = 1.0  # max CPU overcommit ratio (1.0 = disabled)
    split_tmpfs: bool = False  # use tmpfs for parallel split instances
    memory_per_core_mb: int = 2000  # MB per core for Round 1 request_memory
    max_memory_per_core_mb: int = 2000  # max MB per core for Round 2 request_memory
