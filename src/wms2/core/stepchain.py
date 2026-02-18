"""StepChain parser: typed execution specs from ReqMgr2 request dicts."""

from __future__ import annotations

from dataclasses import asdict, dataclass


@dataclass
class StepSpec:
    """Execution spec for one step. All values are resolved (step overrides applied)."""

    name: str  # StepName
    cmssw_version: str  # CMSSW_X_Y_Z
    scram_arch: str  # el8_amd64_gcc10
    global_tag: str  # conditions
    pset: str  # relative path in sandbox: "steps/step1/cfg.py"
    multicore: int  # nThreads
    memory_mb: int
    keep_output: bool
    input_step: str  # "" for first step, else StepName of input provider
    input_from_output_module: str  # "" for first step, else output module name
    mc_pileup: str  # "" if none, else DBS dataset path
    data_pileup: str  # "" if none
    event_streams: int
    request_num_events: int  # for GEN steps, 0 otherwise


@dataclass
class StepChainSpec:
    """Full execution spec for a StepChain request."""

    request_name: str
    steps: list[StepSpec]
    # Top-level resource hints for DAG planning (not per-step execution)
    time_per_event: float
    size_per_event: float
    filter_efficiency: float


def parse_stepchain(data: dict) -> StepChainSpec:
    """Parse a ReqMgr2 StepChain request. Step-level overrides top-level, always."""
    num_steps = int(data.get("StepChain", 1))
    steps = []
    for i in range(1, num_steps + 1):
        step = data[f"Step{i}"]
        steps.append(
            StepSpec(
                name=step.get("StepName", f"step{i}"),
                cmssw_version=step.get("CMSSWVersion") or data.get("CMSSWVersion", ""),
                scram_arch=step.get("ScramArch") or data.get("ScramArch", ""),
                global_tag=step.get("GlobalTag") or data.get("GlobalTag", ""),
                pset=f"steps/step{i}/cfg.py",
                multicore=int(step.get("Multicore") or data.get("Multicore", 1)),
                memory_mb=int(step.get("Memory") or data.get("Memory", 2048)),
                keep_output=step.get("KeepOutput", True),
                input_step=step.get("InputStep", ""),
                input_from_output_module=step.get("InputFromOutputModule", ""),
                mc_pileup=step.get("MCPileup", ""),
                data_pileup=step.get("DataPileup", ""),
                event_streams=int(
                    step.get("EventStreams") or data.get("EventStreams", 0)
                ),
                request_num_events=int(
                    step.get("RequestNumEvents") or data.get("RequestNumEvents", 0)
                ),
            )
        )
    return StepChainSpec(
        request_name=data.get("RequestName", ""),
        steps=steps,
        time_per_event=float(data.get("TimePerEvent", 1.0)),
        size_per_event=float(data.get("SizePerEvent", 1.5)),
        filter_efficiency=float(data.get("FilterEfficiency", 1.0)),
    )


def to_manifest(spec: StepChainSpec) -> dict:
    """Serialize to manifest.json for the sandbox."""
    return {
        "mode": "cmssw",
        "request_name": spec.request_name,
        "steps": [asdict(s) for s in spec.steps],
    }
