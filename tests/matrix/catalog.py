"""Workflow catalog — the single source of numbered workflow definitions.

ID scheme:
    1xx.x  — synthetic mode (fast, no CMSSW)
    2xx.x  — single-step CMSSW (GEN)
    3xx.x  — multi-step StepChain
    4xx.x  — pilot workflow
    5xx.x  — fault injection / recovery (synthetic, fast)
    9xx.x  — performance / scale

Variant convention:
    x00.0 = 1 job,  x00.1 = 8 jobs,  x00.2 = 64 jobs
"""

from __future__ import annotations

from tests.matrix.definitions import FaultSpec, VerifySpec, WorkflowDef

# ── Synthetic baseline ────────────────────────────────────────

_SYNTHETIC_OUTPUT = [
    {
        "dataset_name": "/TestPrimary/Test-v1/GEN-SIM",
        "merged_lfn_base": "/store/mc/Test/TestPrimary/GEN-SIM/v1",
        "unmerged_lfn_base": "/store/unmerged/Test/TestPrimary/GEN-SIM/v1",
        "data_tier": "GEN-SIM",
    }
]

_WF_100_0 = WorkflowDef(
    wf_id=100.0,
    title="Synthetic baseline, 1 job",
    sandbox_mode="synthetic",
    request_spec={
        "RequestName": "matrix_synthetic_100_0",
        "RequestType": "StepChain",
        "StepChain": 1,
        "Step1": {
            "StepName": "GEN-SIM",
            "GlobalTag": "auto:mc",
            "CMSSWVersion": "",
        },
        "Multicore": 1,
        "Memory": 512,
        "TimePerEvent": 0.01,
        "SizePerEvent": 1.0,
    },
    events_per_job=1,
    num_jobs=1,
    output_datasets=_SYNTHETIC_OUTPUT,
    memory_mb=512,
    multicore=1,
    size="small",
    timeout_sec=120,
    requires=("condor",),
    verify=VerifySpec(
        expect_success=True,
        expect_merged_outputs=True,
        expect_cleanup_ran=True,
    ),
)

_WF_100_1 = WorkflowDef(
    wf_id=100.1,
    title="Synthetic baseline, 8 jobs",
    sandbox_mode="synthetic",
    request_spec={
        "RequestName": "matrix_synthetic_100_1",
        "RequestType": "StepChain",
        "StepChain": 1,
        "Step1": {
            "StepName": "GEN-SIM",
            "GlobalTag": "auto:mc",
            "CMSSWVersion": "",
        },
        "Multicore": 1,
        "Memory": 512,
        "TimePerEvent": 0.01,
        "SizePerEvent": 1.0,
    },
    events_per_job=1,
    num_jobs=8,
    output_datasets=_SYNTHETIC_OUTPUT,
    memory_mb=512,
    multicore=1,
    size="medium",
    timeout_sec=300,
    requires=("condor",),
    verify=VerifySpec(
        expect_success=True,
        expect_merged_outputs=True,
        expect_cleanup_ran=True,
    ),
)

# ── Multi-step StepChain (real CMSSW via cached sandbox) ──────

_NPS_OUTPUT_DATASETS = [
    {
        "dataset_name": "/ADD-monojet_N-6_MD-6000_TuneCP5_13p6TeV_pythia8/Run3Summer22EEMiniAODv4-130X_mcRun3_2022_realistic_postEE_v6-v2/MINIAODSIM",
        "merged_lfn_base": "/store/mc/Run3Summer22EEMiniAODv4/ADD-monojet_N-6_MD-6000_TuneCP5_13p6TeV_pythia8/MINIAODSIM/130X_mcRun3_2022_realistic_postEE_v6-v2",
        "unmerged_lfn_base": "/store/unmerged/Run3Summer22EEMiniAODv4/ADD-monojet_N-6_MD-6000_TuneCP5_13p6TeV_pythia8/MINIAODSIM/130X_mcRun3_2022_realistic_postEE_v6-v2",
        "data_tier": "MINIAODSIM",
    },
    {
        "dataset_name": "/ADD-monojet_N-6_MD-6000_TuneCP5_13p6TeV_pythia8/Run3Summer22EENanoAODv12-130X_mcRun3_2022_realistic_postEE_v6-v2/NANOAODSIM",
        "merged_lfn_base": "/store/mc/Run3Summer22EENanoAODv12/ADD-monojet_N-6_MD-6000_TuneCP5_13p6TeV_pythia8/NANOAODSIM/130X_mcRun3_2022_realistic_postEE_v6-v2",
        "unmerged_lfn_base": "/store/unmerged/Run3Summer22EENanoAODv12/ADD-monojet_N-6_MD-6000_TuneCP5_13p6TeV_pythia8/NANOAODSIM/130X_mcRun3_2022_realistic_postEE_v6-v2",
        "data_tier": "NANOAODSIM",
    },
]

_WF_300_0 = WorkflowDef(
    wf_id=300.0,
    title="NPS 5-step StepChain, 1 work unit (8 jobs x 50 ev)",
    sandbox_mode="cached",
    cached_sandbox_path="/mnt/shared/work/wms2_real_condor_test/sandbox.tar.gz",
    request_spec={
        "RequestName": "cmsunified_task_NPS-Run3Summer22EEGS-00049__v1_T_260126_110934_54",
        "RequestType": "StepChain",
        "StepChain": 5,
        "Multicore": 8,
        "Memory": 16000,
        "TimePerEvent": 1.0,
        "SizePerEvent": 50.0,
    },
    events_per_job=50,
    num_jobs=8,
    output_datasets=_NPS_OUTPUT_DATASETS,
    memory_mb=16000,
    multicore=8,
    size="large",
    timeout_sec=3600,
    requires=("condor", "cvmfs", "siteconf", "apptainer"),
    verify=VerifySpec(
        expect_success=True,
        expect_merged_outputs=True,
        expect_cleanup_ran=True,
    ),
)

_GEN_DY2L_OUTPUT_DATASETS = [
    {
        "dataset_name": "/DYto2L-4Jets_MLL-4to50_HT-2500_TuneCP5_13p6TeV_madgraphMLM-pythia8/Run3Summer22EEMiniAODv4-130X_mcRun3_2022_realistic_postEE_v6-v2/MINIAODSIM",
        "merged_lfn_base": "/store/mc/Run3Summer22EEMiniAODv4/DYto2L-4Jets_MLL-4to50_HT-2500_TuneCP5_13p6TeV_madgraphMLM-pythia8/MINIAODSIM/130X_mcRun3_2022_realistic_postEE_v6-v2",
        "unmerged_lfn_base": "/store/unmerged/Run3Summer22EEMiniAODv4/DYto2L-4Jets_MLL-4to50_HT-2500_TuneCP5_13p6TeV_madgraphMLM-pythia8/MINIAODSIM/130X_mcRun3_2022_realistic_postEE_v6-v2",
        "data_tier": "MINIAODSIM",
    },
    {
        "dataset_name": "/DYto2L-4Jets_MLL-4to50_HT-2500_TuneCP5_13p6TeV_madgraphMLM-pythia8/Run3Summer22EENanoAODv12-130X_mcRun3_2022_realistic_postEE_v6-v2/NANOAODSIM",
        "merged_lfn_base": "/store/mc/Run3Summer22EENanoAODv12/DYto2L-4Jets_MLL-4to50_HT-2500_TuneCP5_13p6TeV_madgraphMLM-pythia8/NANOAODSIM/130X_mcRun3_2022_realistic_postEE_v6-v2",
        "unmerged_lfn_base": "/store/unmerged/Run3Summer22EENanoAODv12/DYto2L-4Jets_MLL-4to50_HT-2500_TuneCP5_13p6TeV_madgraphMLM-pythia8/NANOAODSIM/130X_mcRun3_2022_realistic_postEE_v6-v2",
        "data_tier": "NANOAODSIM",
    },
]

_WF_301_0 = WorkflowDef(
    wf_id=301.0,
    title="DY2L 5-step StepChain, 8 cores (8 jobs x 500 ev, ~10% filter)",
    sandbox_mode="cached",
    cached_sandbox_path="/mnt/shared/work/wms2_real_condor_test/sandbox_gen_dy2l.tar.gz",
    request_spec={
        "RequestName": "cmsunified_task_GEN-Run3Summer22EEwmLHEGS-00600__v1_T_250902_211552_8573",
        "RequestType": "StepChain",
        "StepChain": 5,
        "Multicore": 8,
        "Memory": 16000,
        "TimePerEvent": 11.35,
        "SizePerEvent": 1570.7,
    },
    events_per_job=500,
    num_jobs=8,
    output_datasets=_GEN_DY2L_OUTPUT_DATASETS,
    memory_mb=16000,
    multicore=8,
    size="large",
    timeout_sec=10800,
    requires=("condor", "cvmfs", "siteconf", "apptainer"),
    verify=VerifySpec(
        expect_success=True,
        expect_merged_outputs=True,
        expect_cleanup_ran=True,
    ),
)

_WF_351_0 = WorkflowDef(
    wf_id=351.0,
    title="DY2L adaptive step 1 split (8 jobs x 500 ev, ~10% filter)",
    sandbox_mode="cached",
    cached_sandbox_path="/mnt/shared/work/wms2_real_condor_test/sandbox_gen_dy2l.tar.gz",
    request_spec={
        "RequestName": "cmsunified_task_GEN-Run3Summer22EEwmLHEGS-00600__v1_T_250902_211552_8573",
        "RequestType": "StepChain",
        "StepChain": 5,
        "Multicore": 8,
        "Memory": 16000,
        "TimePerEvent": 11.35,
        "SizePerEvent": 1570.7,
    },
    events_per_job=500,
    num_jobs=8,
    num_work_units=2,
    adaptive=True,
    output_datasets=_GEN_DY2L_OUTPUT_DATASETS,
    memory_mb=16000,
    multicore=8,
    size="large",
    timeout_sec=10800,
    requires=("condor", "cvmfs", "siteconf", "apptainer"),
    verify=VerifySpec(
        expect_success=True,
        expect_merged_outputs=True,
        expect_cleanup_ran=True,
    ),
)

# ── Adaptive execution ────────────────────────────────────────

_WF_350_0 = WorkflowDef(
    wf_id=350.0,
    title="Adaptive nThreads tuning, 50 ev/job",
    sandbox_mode="cached",
    cached_sandbox_path="/mnt/shared/work/wms2_real_condor_test/sandbox.tar.gz",
    request_spec={
        "RequestName": "cmsunified_task_NPS-Run3Summer22EEGS-00049__v1_T_260126_110934_54",
        "RequestType": "StepChain",
        "StepChain": 5,
        "Multicore": 8,
        "Memory": 16000,
        "TimePerEvent": 1.0,
        "SizePerEvent": 50.0,
    },
    events_per_job=50,
    num_jobs=8,
    num_work_units=2,
    adaptive=True,
    output_datasets=_NPS_OUTPUT_DATASETS,
    memory_mb=16000,
    multicore=8,
    size="large",
    timeout_sec=7200,
    requires=("condor", "cvmfs", "siteconf", "apptainer"),
    verify=VerifySpec(
        expect_success=True,
        expect_merged_outputs=True,
        expect_cleanup_ran=True,
    ),
)

_WF_360_0 = WorkflowDef(
    wf_id=360.0,
    title="Adaptive overcommit only (no step 0 split)",
    sandbox_mode="cached",
    cached_sandbox_path="/mnt/shared/work/wms2_real_condor_test/sandbox.tar.gz",
    request_spec={
        "RequestName": "cmsunified_task_NPS-Run3Summer22EEGS-00049__v1_T_260126_110934_54",
        "RequestType": "StepChain",
        "StepChain": 5,
        "Multicore": 8,
        "Memory": 16000,
        "TimePerEvent": 1.0,
        "SizePerEvent": 50.0,
    },
    events_per_job=50,
    num_jobs=8,
    num_work_units=2,
    adaptive=True,
    adaptive_split=False,
    overcommit_max=1.25,
    output_datasets=_NPS_OUTPUT_DATASETS,
    memory_mb=16000,
    multicore=8,
    size="large",
    timeout_sec=7200,
    requires=("condor", "cvmfs", "siteconf", "apptainer"),
    verify=VerifySpec(
        expect_success=True,
        expect_merged_outputs=True,
        expect_cleanup_ran=True,
    ),
)

_WF_370_0 = WorkflowDef(
    wf_id=370.0,
    title="Adaptive split + overcommit (1.25x max)",
    sandbox_mode="cached",
    cached_sandbox_path="/mnt/shared/work/wms2_real_condor_test/sandbox.tar.gz",
    request_spec={
        "RequestName": "cmsunified_task_NPS-Run3Summer22EEGS-00049__v1_T_260126_110934_54",
        "RequestType": "StepChain",
        "StepChain": 5,
        "Multicore": 8,
        "Memory": 16000,
        "TimePerEvent": 1.0,
        "SizePerEvent": 50.0,
    },
    events_per_job=50,
    num_jobs=8,
    num_work_units=2,
    adaptive=True,
    overcommit_max=1.25,
    output_datasets=_NPS_OUTPUT_DATASETS,
    memory_mb=16000,
    multicore=8,
    size="large",
    timeout_sec=7200,
    requires=("condor", "cvmfs", "siteconf", "apptainer"),
    verify=VerifySpec(
        expect_success=True,
        expect_merged_outputs=True,
        expect_cleanup_ran=True,
    ),
)

_WF_380_0 = WorkflowDef(
    wf_id=380.0,
    title="Adaptive all-step pipeline split",
    sandbox_mode="cached",
    cached_sandbox_path="/mnt/shared/work/wms2_real_condor_test/sandbox.tar.gz",
    request_spec={
        "RequestName": "cmsunified_task_NPS-Run3Summer22EEGS-00049__v1_T_260126_110934_54",
        "RequestType": "StepChain",
        "StepChain": 5,
        "Multicore": 8,
        "Memory": 16000,
        "TimePerEvent": 1.0,
        "SizePerEvent": 50.0,
    },
    events_per_job=50,
    num_jobs=8,
    num_work_units=2,
    adaptive=True,
    adaptive_split=False,
    split_all_steps=True,
    output_datasets=_NPS_OUTPUT_DATASETS,
    memory_mb=16000,
    multicore=8,
    size="large",
    timeout_sec=7200,
    requires=("condor", "cvmfs", "siteconf", "apptainer"),
    verify=VerifySpec(
        expect_success=True,
        expect_merged_outputs=True,
        expect_cleanup_ran=True,
    ),
)

_WF_380_1 = WorkflowDef(
    wf_id=380.1,
    title="Pipeline split, uniform threads, 500 ev/job",
    sandbox_mode="cached",
    cached_sandbox_path="/mnt/shared/work/wms2_real_condor_test/sandbox.tar.gz",
    request_spec={
        "RequestName": "cmsunified_task_NPS-Run3Summer22EEGS-00049__v1_T_260126_110934_54",
        "RequestType": "StepChain",
        "StepChain": 5,
        "Multicore": 8,
        "Memory": 16000,
        "TimePerEvent": 1.0,
        "SizePerEvent": 50.0,
    },
    events_per_job=500,
    num_jobs=8,
    num_work_units=2,
    adaptive=True,
    adaptive_split=False,
    split_all_steps=True,
    split_uniform_threads=True,
    output_datasets=_NPS_OUTPUT_DATASETS,
    memory_mb=16000,
    multicore=8,
    size="large",
    timeout_sec=72000,
    requires=("condor", "cvmfs", "siteconf", "apptainer"),
    verify=VerifySpec(
        expect_success=True,
        expect_merged_outputs=True,
        expect_cleanup_ran=True,
    ),
)

_WF_300_1 = WorkflowDef(
    wf_id=300.1,
    title="NPS 5-step StepChain, 16-core jobs (4 jobs x 1000 ev)",
    sandbox_mode="cached",
    cached_sandbox_path="/mnt/shared/work/wms2_real_condor_test/sandbox.tar.gz",
    request_spec={
        "RequestName": "cmsunified_task_NPS-Run3Summer22EEGS-00049__v1_T_260126_110934_54",
        "RequestType": "StepChain",
        "StepChain": 5,
        "Multicore": 16,
        "Memory": 32000,
        "TimePerEvent": 1.0,
        "SizePerEvent": 50.0,
    },
    events_per_job=1000,
    num_jobs=4,
    output_datasets=_NPS_OUTPUT_DATASETS,
    memory_mb=32000,
    multicore=16,
    size="large",
    timeout_sec=36000,
    requires=("condor", "cvmfs", "siteconf", "apptainer"),
    verify=VerifySpec(
        expect_success=True,
        expect_merged_outputs=True,
        expect_cleanup_ran=True,
    ),
)

_WF_350_1 = WorkflowDef(
    wf_id=350.1,
    title="Adaptive nThreads tuning, 10 ev/job",
    sandbox_mode="cached",
    cached_sandbox_path="/mnt/shared/work/wms2_real_condor_test/sandbox.tar.gz",
    request_spec={
        "RequestName": "cmsunified_task_NPS-Run3Summer22EEGS-00049__v1_T_260126_110934_54",
        "RequestType": "StepChain",
        "StepChain": 5,
        "Multicore": 8,
        "Memory": 16000,
        "TimePerEvent": 1.0,
        "SizePerEvent": 50.0,
    },
    events_per_job=10,
    num_jobs=8,
    num_work_units=2,
    adaptive=True,
    output_datasets=_NPS_OUTPUT_DATASETS,
    memory_mb=16000,
    multicore=8,
    size="large",
    timeout_sec=3600,
    requires=("condor", "cvmfs", "siteconf", "apptainer"),
    verify=VerifySpec(
        expect_success=True,
        expect_merged_outputs=True,
        expect_cleanup_ran=True,
    ),
)

# ── Fault injection ───────────────────────────────────────────

_WF_500_0 = WorkflowDef(
    wf_id=500.0,
    title="Fault: proc exits 1 (retry then rescue)",
    sandbox_mode="synthetic",
    request_spec={
        "RequestName": "matrix_fault_500_0",
        "RequestType": "StepChain",
        "StepChain": 1,
        "Step1": {
            "StepName": "GEN-SIM",
            "GlobalTag": "auto:mc",
            "CMSSWVersion": "",
        },
        "Multicore": 1,
        "Memory": 512,
        "TimePerEvent": 0.01,
        "SizePerEvent": 1.0,
    },
    events_per_job=1,
    num_jobs=1,
    output_datasets=_SYNTHETIC_OUTPUT,
    memory_mb=512,
    multicore=1,
    size="medium",
    timeout_sec=300,
    requires=("condor",),
    fault=FaultSpec(target="proc", exit_code=1),
    verify=VerifySpec(
        expect_success=False,
        expect_rescue_dag=True,
        expect_merged_outputs=False,
        expect_cleanup_ran=False,
    ),
)

_WF_501_0 = WorkflowDef(
    wf_id=501.0,
    title="Fault: proc SIGKILL",
    sandbox_mode="synthetic",
    request_spec={
        "RequestName": "matrix_fault_501_0",
        "RequestType": "StepChain",
        "StepChain": 1,
        "Step1": {
            "StepName": "GEN-SIM",
            "GlobalTag": "auto:mc",
            "CMSSWVersion": "",
        },
        "Multicore": 1,
        "Memory": 512,
        "TimePerEvent": 0.01,
        "SizePerEvent": 1.0,
    },
    events_per_job=1,
    num_jobs=2,
    output_datasets=_SYNTHETIC_OUTPUT,
    memory_mb=512,
    multicore=1,
    size="small",
    timeout_sec=120,
    requires=("condor",),
    fault=FaultSpec(target="proc", signal=9),
    verify=VerifySpec(
        expect_success=False,
        expect_rescue_dag=True,
        expect_merged_outputs=False,
        expect_cleanup_ran=False,
    ),
)

_WF_510_0 = WorkflowDef(
    wf_id=510.0,
    title="Fault: merge exits 1",
    sandbox_mode="synthetic",
    request_spec={
        "RequestName": "matrix_fault_510_0",
        "RequestType": "StepChain",
        "StepChain": 1,
        "Step1": {
            "StepName": "GEN-SIM",
            "GlobalTag": "auto:mc",
            "CMSSWVersion": "",
        },
        "Multicore": 1,
        "Memory": 512,
        "TimePerEvent": 0.01,
        "SizePerEvent": 1.0,
    },
    events_per_job=1,
    num_jobs=2,
    output_datasets=_SYNTHETIC_OUTPUT,
    memory_mb=512,
    multicore=1,
    size="small",
    timeout_sec=120,
    requires=("condor",),
    fault=FaultSpec(target="merge", exit_code=1),
    verify=VerifySpec(
        expect_success=False,
        expect_rescue_dag=True,
        expect_merged_outputs=False,
        expect_cleanup_ran=False,
    ),
)

# ── Master catalog ────────────────────────────────────────────

CATALOG: dict[float, WorkflowDef] = {
    wf.wf_id: wf
    for wf in [
        _WF_100_0,
        _WF_100_1,
        _WF_300_0,
        _WF_300_1,
        _WF_301_0,
        _WF_351_0,
        _WF_350_0,
        _WF_360_0,
        _WF_370_0,
        _WF_380_0,
        _WF_380_1,
        _WF_350_1,
        _WF_500_0,
        _WF_501_0,
        _WF_510_0,
    ]
}
