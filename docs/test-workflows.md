# Test Workflows Reference

Workflow details are extracted from ReqMgr2 using `scripts/fetch-workflow-info.py`.

```bash
# Fetch from ReqMgr2
python scripts/fetch-workflow-info.py \
    cmsunified_task_NPS-Run3Summer22EEGS-00049__v1_T_260126_110934_54 \
    --proxy /mnt/creds/x509up

# From a local request.json
python scripts/fetch-workflow-info.py \
    --from-file /mnt/shared/work/wms2_real_test/request.json
```

## Quick Reference

| Name | Request | Type | Steps | Cores | Memory | FilterEff | CPU-days | Output Tiers |
|------|---------|------|-------|-------|--------|-----------|----------|--------------|
| NPS | `NPS-Run3Summer22EEGS-00049` | StepChain | 5 | 8 | 16 GB | 1.0 | 363.5 | MINIAODSIM, NANOAODSIM |
| B2G | `B2G-Run3Summer23BPixwmLHEGS-06000` | StepChain | 5 | 8 | 16 GB | 1.0 | 17.7 | AODSIM, MINIAODSIM, NANOAODSIM |
| GEN-QCD | `GEN-RunIII2024Summer24GS-00002` | StepChain | 5 | 8 | 16 GB | 0.0028 | 110,103.8 | AODSIM, MINIAODSIM, NANOAODSIM |
| DY2L | `GEN-Run3Summer22EEwmLHEGS-00600` | StepChain | 5 | 4 | 8 GB | 0.1023 | 17,970.5 | AODSIM, MINIAODSIM, NANOAODSIM |
| BPH | `BPH-RunIISummer20UL18GEN-00292` | StepChain | 7 | 16 | 32 GB | 0.00034 | 171,637.3 | MINIAODSIM, NANOAODSIM, AODSIM |

CPU-days = TimePerEvent × Multicore × RequestNumEvents / FilterEfficiency / 86400

TimePerEvent is per *generated* event (the full chain cost per one event entering step 1).
RequestNumEvents is desired *output* events. For GenFilter workflows, the actual number
of generated events is RequestNumEvents / FilterEfficiency.

---

## NPS

**Request name**: `cmsunified_task_NPS-Run3Summer22EEGS-00049__v1_T_260126_110934_54`

- **Type**: StepChain, 5-step (GEN)
- **Multicore**: 8
- **Memory**: 16,000 MB
- **TimePerEvent**: 39.26 s
- **SizePerEvent**: 2,628.3 KB
- **RequestNumEvents**: 100,000
- **CPU-days**: 363.5
- **Output tiers**: MINIAODSIM, NANOAODSIM

| Step | Name | CMSSW | Arch | Keep | GlobalTag |
|------|------|-------|------|------|-----------|
| 1 | NPS-Run3Summer22EEGS-00049_0 | CMSSW_12_4_25 | el8_amd64_gcc10 | no | 124X_mcRun3_2022_realistic_postEE_v1 |
| 2 | NPS-Run3Summer22EEDRPremix-00840_0 | CMSSW_12_4_25 | el8_amd64_gcc10 | no | 124X_mcRun3_2022_realistic_postEE_v1 |
| 3 | NPS-Run3Summer22EEDRPremix-00840_1 | CMSSW_12_4_25 | el8_amd64_gcc10 | no | 124X_mcRun3_2022_realistic_postEE_v1 |
| 4 | NPS-Run3Summer22EEMiniAODv4-00841_0 | CMSSW_13_0_23 | el8_amd64_gcc11 | yes | 130X_mcRun3_2022_realistic_postEE_v6 |
| 5 | NPS-Run3Summer22EENanoAODv12-00209_0 | CMSSW_13_0_23 | el8_amd64_gcc11 | yes | 130X_mcRun3_2022_realistic_postEE_v6 |

**Output datasets**:
```
/ADD-monojet_N-6_MD-6000_TuneCP5_13p6TeV-pythia8/Run3Summer22EEMiniAODv4-130X_mcRun3_2022_realistic_postEE_v6-v2/MINIAODSIM
/ADD-monojet_N-6_MD-6000_TuneCP5_13p6TeV-pythia8/Run3Summer22EENanoAODv12-130X_mcRun3_2022_realistic_postEE_v6-v2/NANOAODSIM
```

**What it exercises**: Full 5-step GEN-SIM → DRPremix (x2) → MiniAOD → NanoAOD pipeline. Two CMSSW versions (12_4 for steps 1-3, 13_0 for steps 4-5). High TimePerEvent (39 s) makes it slow per-job but good for testing resource profiling.

**Tested modes**: cached (WF 300.0), adaptive nThreads (WF 350.x), overcommit (WF 360.0), pipeline split (WF 380.x)

---

## B2G

**Request name**: `cmsunified_task_B2G-Run3Summer23BPixwmLHEGS-06000__v1_T_250628_211038_1313`

- **Type**: StepChain, 5-step (GEN)
- **Multicore**: 8
- **Memory**: 16,000 MB
- **TimePerEvent**: 7.66 s
- **SizePerEvent**: 1,615.7 KB
- **RequestNumEvents**: 25,000
- **CPU-days**: 17.7
- **Output tiers**: AODSIM, MINIAODSIM, NANOAODSIM

| Step | Name | CMSSW | Arch | Keep | GlobalTag |
|------|------|-------|------|------|-----------|
| 1 | B2G-Run3Summer23BPixwmLHEGS-06000_0 | CMSSW_13_0_23 | el8_amd64_gcc11 | no | 130X_mcRun3_2023_realistic_postBPix_v6 |
| 2 | B2G-Run3Summer23BPixDRPremix-05176_0 | CMSSW_13_0_23 | el8_amd64_gcc11 | no | 130X_mcRun3_2023_realistic_postBPix_v6 |
| 3 | B2G-Run3Summer23BPixDRPremix-05176_1 | CMSSW_13_0_23 | el8_amd64_gcc11 | yes | 130X_mcRun3_2023_realistic_postBPix_v6 |
| 4 | B2G-Run3Summer23BPixMiniAODv4-05176_0 | CMSSW_13_0_23 | el8_amd64_gcc11 | yes | 130X_mcRun3_2023_realistic_postBPix_v6 |
| 5 | B2G-Run3Summer23BPixNanoAODv12-05176_0 | CMSSW_13_0_23 | el8_amd64_gcc11 | yes | 130X_mcRun3_2023_realistic_postBPix_v6 |

**Output datasets**:
```
/WprimetoWZto3LNu_M800_TuneCP5_13p6TeV_madgraph-pythia8/Run3Summer23BPixDRPremix-130X_mcRun3_2023_realistic_postBPix_v6-v2/AODSIM
/WprimetoWZto3LNu_M800_TuneCP5_13p6TeV_madgraph-pythia8/Run3Summer23BPixMiniAODv4-130X_mcRun3_2023_realistic_postBPix_v6-v2/MINIAODSIM
/WprimetoWZto3LNu_M800_TuneCP5_13p6TeV_madgraph-pythia8/Run3Summer23BPixNanoAODv12-130X_mcRun3_2023_realistic_postBPix_v6-v2/NANOAODSIM
```

**What it exercises**: Small GEN workflow (25k events, 17.7 CPU-days, no filter). Single CMSSW version (13_0_23) across all steps. Three kept output tiers (AODSIM from step 3). Fast per-event (7.66 s) — good for quick smoke tests.

**Tested modes**: simulator (e2e verification during sandbox mode development)

---

## GEN-QCD

**Request name**: `cmsunified_task_GEN-RunIII2024Summer24GS-00002__v1_T_260204_161305_1359`

- **Type**: StepChain, 5-step (GEN)
- **Multicore**: 8
- **Memory**: 16,000 MB
- **TimePerEvent**: 0.11 s
- **SizePerEvent**: 608.5 KB
- **FilterEfficiency**: 0.00279287
- **RequestNumEvents**: 30,000,000
- **CPU-days**: 110,103.8
- **Output tiers**: AODSIM, MINIAODSIM, NANOAODSIM

| Step | Name | CMSSW | Arch | Keep | GlobalTag |
|------|------|-------|------|------|-----------|
| 1 | GEN-RunIII2024Summer24GS-00002_0 | CMSSW_14_0_21 | el8_amd64_gcc12 | no | 140X_mcRun3_2024_realistic_v26 |
| 2 | GEN-RunIII2024Summer24DRPremix-00625_0 | CMSSW_14_0_22 | el8_amd64_gcc12 | no | 140X_mcRun3_2024_realistic_v26 |
| 3 | GEN-RunIII2024Summer24DRPremix-00625_1 | CMSSW_14_0_22 | el8_amd64_gcc12 | yes | 140X_mcRun3_2024_realistic_v26 |
| 4 | GEN-RunIII2024Summer24MiniAODv6-00624_0 | CMSSW_15_0_18 | el8_amd64_gcc12 | yes | 150X_mcRun3_2024_realistic_v2 |
| 5 | GEN-RunIII2024Summer24NanoAODv15-00730_0 | CMSSW_15_0_18 | el8_amd64_gcc12 | yes | 150X_mcRun3_2024_realistic_v2 |

**Output datasets**:
```
/QCD_Bin-PT-15to20_Fil-bcToE_TuneCP5_13p6TeV_pythia8/RunIII2024Summer24DRPremix-140X_mcRun3_2024_realistic_v26-v2/AODSIM
/QCD_Bin-PT-15to20_Fil-bcToE_TuneCP5_13p6TeV_pythia8/RunIII2024Summer24MiniAODv6-150X_mcRun3_2024_realistic_v2-v2/MINIAODSIM
/QCD_Bin-PT-15to20_Fil-bcToE_TuneCP5_13p6TeV_pythia8/RunIII2024Summer24NanoAODv15-150X_mcRun3_2024_realistic_v2-v2/NANOAODSIM
```

**What it exercises**: High-event-count GEN workflow (30M events) with very fast per-event time (0.11 s) but low filter efficiency (0.28%), making it a large workflow (110k CPU-days). Three CMSSW versions (14_0_21, 14_0_22, 15_0_18) including Run3 2024 releases. Three kept output tiers. Uses the latest el8_amd64_gcc12 architecture throughout.

**Tested modes**: synthetic (production-path e2e), cached (real CMSSW e2e with 5 work units)

---

## DY2L

**Request name**: `cmsunified_task_GEN-Run3Summer22EEwmLHEGS-00600__v1_T_250902_211552_8573`

- **Type**: StepChain, 5-step (GEN)
- **Multicore**: 4
- **Memory**: 8,000 MB
- **TimePerEvent**: 11.35 s
- **SizePerEvent**: 1,570.7 KB
- **FilterEfficiency**: 0.10231
- **RequestNumEvents**: 3,500,000
- **CPU-days**: 17,970.5
- **Output tiers**: AODSIM, MINIAODSIM, NANOAODSIM

| Step | Name | CMSSW | Arch | Keep | GlobalTag |
|------|------|-------|------|------|-----------|
| 1 | GEN-Run3Summer22EEwmLHEGS-00600_0 | CMSSW_12_4_19 | el8_amd64_gcc10 | no | 124X_mcRun3_2022_realistic_postEE_v1 |
| 2 | GEN-Run3Summer22EEDRPremix-00448_0 | CMSSW_12_4_16 | el8_amd64_gcc10 | no | 124X_mcRun3_2022_realistic_postEE_v1 |
| 3 | GEN-Run3Summer22EEDRPremix-00448_1 | CMSSW_12_4_16 | el8_amd64_gcc10 | yes | 124X_mcRun3_2022_realistic_postEE_v1 |
| 4 | GEN-Run3Summer22EEMiniAODv4-00441_0 | CMSSW_13_0_13 | el8_amd64_gcc11 | yes | 130X_mcRun3_2022_realistic_postEE_v6 |
| 5 | GEN-Run3Summer22EENanoAODv12-00478_0 | CMSSW_13_0_13 | el8_amd64_gcc11 | yes | 130X_mcRun3_2022_realistic_postEE_v6 |

**Output datasets**:
```
/DYto2L-4Jets_MLL-4to50_HT-2500_TuneCP5_13p6TeV_madgraphMLM-pythia8/Run3Summer22EEDRPremix-124X_mcRun3_2022_realistic_postEE_v1-v2/AODSIM
/DYto2L-4Jets_MLL-4to50_HT-2500_TuneCP5_13p6TeV_madgraphMLM-pythia8/Run3Summer22EEMiniAODv4-130X_mcRun3_2022_realistic_postEE_v6-v2/MINIAODSIM
/DYto2L-4Jets_MLL-4to50_HT-2500_TuneCP5_13p6TeV_madgraphMLM-pythia8/Run3Summer22EENanoAODv12-130X_mcRun3_2022_realistic_postEE_v6-v2/NANOAODSIM
```

**What it exercises**: wmLHEGS workflow with LHE input and ~10% filter efficiency. Lower multicore (4 cores, 8 GB) than the other workflows. 18k CPU-days due to filter amplification. Multiple CMSSW versions (12_4_19, 12_4_16, 13_0_13). Extensively tested for adaptive execution and memory scaling.

**Tested modes**: cached (WF 301.0), adaptive step-1 split (WF 351.x), adaptive job split (WF 391.x), memory scaling (WF 392.x)

---

## BPH

**Request name**: `cmsunified_task_BPH-RunIISummer20UL18GEN-00292__v1_T_250801_104414_1441`

- **Type**: StepChain, 7-step (GEN)
- **Multicore**: 16
- **Memory**: 32,000 MB
- **TimePerEvent**: 0.06 s
- **SizePerEvent**: 99.5 KB
- **FilterEfficiency**: 0.000338198
- **RequestNumEvents**: 5,000,000
- **CPU-days**: 171,637.3
- **Output tiers**: MINIAODSIM, NANOAODSIM, AODSIM

| Step | Name | CMSSW | Arch | Keep | GlobalTag |
|------|------|-------|------|------|-----------|
| 1 | BPH-RunIISummer20UL18GEN-00292_0 | CMSSW_10_6_47 | slc7_amd64_gcc700 | no | 106X_upgrade2018_realistic_v4 |
| 2 | BPH-RunIISummer20UL18SIM-00537_0 | CMSSW_10_6_47 | slc7_amd64_gcc700 | no | 106X_upgrade2018_realistic_v11_L1v1 |
| 3 | BPH-RunIISummer20UL18DIGIPremix-00476_0 | CMSSW_10_6_26 | slc7_amd64_gcc700 | no | 106X_upgrade2018_realistic_v16_L1v1 |
| 4 | BPH-RunIISummer20UL18HLT-00536_0 | CMSSW_10_2_16_UL2 | slc7_amd64_gcc700 | no | 102X_upgrade2018_realistic_v15 |
| 5 | BPH-RunIISummer20UL18RECO-00536_0 | CMSSW_10_6_26 | slc7_amd64_gcc700 | yes | 106X_upgrade2018_realistic_v16_L1v1 |
| 6 | BPH-RunIISummer20UL18MiniAODv2-00573_0 | CMSSW_10_6_26 | slc7_amd64_gcc700 | yes | 106X_upgrade2018_realistic_v16_L1v1 |
| 7 | BPH-RunIISummer20UL18NanoAODv9-00545_0 | CMSSW_10_6_47_patch1 | slc7_amd64_gcc700 | yes | 106X_upgrade2018_realistic_v16_L1v1 |

**Output datasets**:
```
/BdToKstarTauTau_TuneCP5_13TeV_pythia8-evtgen/RunIISummer20UL18RECO-Custom_RDStarPU_BParking_106X_upgrade2018_realistic_v16_L1v1-v2/AODSIM
/BdToKstarTauTau_TuneCP5_13TeV_pythia8-evtgen/RunIISummer20UL18MiniAODv2-Custom_RDStarPU_BParking_106X_upgrade2018_realistic_v16_L1v1-v2/MINIAODSIM
/BdToKstarTauTau_TuneCP5_13TeV_pythia8-evtgen/RunIISummer20UL18NanoAODv9-Custom_RDStarPU_BParking_106X_upgrade2018_realistic_v16_L1v1-v2/NANOAODSIM
```

**What it exercises**: Run 2 UltraLegacy reprocessing — 7-step chain with separate GEN, SIM, DIGIPremix, HLT, RECO, MiniAOD, and NanoAOD steps. Very low filter efficiency (0.034%) makes it the largest workflow (172k CPU-days). Uses `slc7_amd64_gcc700` (SLC7) architecture throughout, requiring an older container. Four different CMSSW versions (10_2, 10_6_26, 10_6_47, 10_6_47_patch1). Highest multicore (16 cores, 32 GB) and most steps of any test workflow. Includes an explicit HLT step not present in Run 3 workflows.

**Tested modes**: not yet tested

---

## Choosing a Workflow

**Quick smoke test**: Use **B2G** — smallest event count (25k), fast per-event, single CMSSW version, completes quickly.

**Full pipeline test**: Use **NPS** — well-characterized, two CMSSW versions, the first workflow tested with WMS2.

**High-volume / splitting tests**: Use **GEN-QCD** — 30M events with 0.11 s/event means many fast jobs. Good for testing DAG splitting, work unit management, and monitoring at scale.

**Adaptive execution / memory tests**: Use **DY2L** — 10% filter efficiency (18k CPU-days) stresses adaptive splitting. Lower multicore (4 cores) tests non-standard resource profiles. Extensively characterized for memory scaling.

**Latest CMSSW releases**: Use **GEN-QCD** — uses CMSSW 14.x and 15.x (gcc12 arch), exercises the newest software stack.

**Run 2 / SLC7 / long chains**: Use **BPH** — 7-step Run 2 UltraLegacy workflow with slc7 architecture, explicit HLT step, and 16-core jobs. Tests container compatibility and multi-version CMSSW chains.
