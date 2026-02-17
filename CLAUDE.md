# CLAUDE.md — WMS2 Project Guide

## What is this project?

WMS2 is the next-generation CMS Workload Management System, replacing WMCore/WMAgent. It is a thin orchestration layer that delegates job-level execution to HTCondor DAGMan. Currently in the specification/design phase — no application code yet, only the spec document.

## Project structure

```
docs/spec.md                    — The specification document (the primary artifact)
IMPLEMENTATION_LOG.md           — Development log (what was built, decisions, verification)
scripts/check-env.sh            — Environment verification script
README.md                       — Project readme
LICENSE                         — License file
.claude/environment.md          — Required services, tools, packages (committed)
.claude/environment.local.md    — This machine's setup (gitignored)
.claude/settings.json           — Project-specific tool permissions (committed)
```

## Development environment

**At the start of every session**, do this before any other work:

1. Read `.claude/environment.local.md` — it tells you what's installed, where services are, connection details, and known limitations. Do NOT run discovery commands if this file exists and is recent.
2. If the file is **missing** (fresh clone), run `scripts/check-env.sh` to see what's available, then generate `.claude/environment.local.md` from the template in `.claude/environment.md`.
3. If the file is **stale** (>30 days or after known changes), re-run `scripts/check-env.sh` and update it.

**Key paths on current dev VM:**
- Python venv: `.venv/` (activate with `source .venv/bin/activate`)
- Always use the venv Python, never system Python 3.9
- HTCondor Python bindings: `import htcondor2` (v2 API), not `import htcondor`
- PostgreSQL: local, db=`wms2`, user=`wms2`, password in environment.local.md

See `.claude/environment.md` for full project requirements.

## The spec document (`docs/spec.md`)

This is the main deliverable. It is a comprehensive design spec (~3100 lines) covering:

- **Sections 1–2**: Overview, architecture, execution model
- **Section 3**: Data models (Request, Workflow, DAG, OutputDataset, Site) and PostgreSQL schema
- **Section 4**: Core components (Lifecycle Manager, Workflow Manager, Admission Controller, DAG Planner, DAG Monitor, Output Manager, Error Handler, Site Manager, Metrics Collector)
- **Section 5**: Pilot execution model
- **Section 6**: Recovery model (multi-level recovery, clean stop, catastrophic failure)
- **Section 7**: API design
- **Sections 8–9**: Auth and tech stack
- **Section 10**: Implementation plan (4 phases)
- **Sections 11–13**: Migration, HTCondor feature requirements, success criteria
- **Section 14**: Risks and mitigations
- **Section 15**: Open questions
- **Section 16**: Design decisions (rationale for key choices, rejected alternatives)
- **Appendices A–C**: WMCore mapping, ReqMgr2 schema, DAGMan file format

## Key architectural concepts

- **Request Lifecycle Manager**: Single owner of every request's state machine. Replaces distributed polling loops. All other components are workers called by it.
- **Merge groups as SUBDAG EXTERNAL**: Each merge group (processing + merge + cleanup nodes) is a self-contained sub-DAG within one top-level DAG. A landing node lets HTCondor pick the site; remaining nodes are pinned to that site via PRE/POST scripts.
- **Clean stop flow**: ACTIVE → STOPPING → RESUBMITTING → QUEUED → ACTIVE (rescue DAG)
- **Catastrophic recovery**: Dataset versioning — increment `processing_version`, resubmit from scratch, invalidate old outputs based on cleanup policy.
- **Delegation to HTCondor**: WMS2 does NOT track individual jobs. DAGMan owns per-node retry, dependency ordering, and scheduling. WMS2 operates at workflow/DAG level only.

## Design invariants (do not violate)

- **Spec is the source of truth — always rebuildable.** The spec must capture not just *what* the design is, but *why* each decision was made. Every significant design choice should have its rationale documented in the spec (in the relevant section, in Section 16 Design Decisions, or in Open Questions). Code is derived from the spec, not the other way around. If the spec changes, everything can be rebuilt from it.
- **No per-job tracking in WMS2.** WMS2 tracks requests, workflows, and DAGs. Individual job state is owned entirely by HTCondor/DAGMan. Never suggest adding job-level tables, job status polling, or per-job retry logic in WMS2.
- **HTCondor is the engine, WMS2 is the orchestrator.** Where HTCondor lacks a needed capability, the long-term answer is a feature request to the HTCondor team (Section 12). However, WMS2 is in prototyping stage — pragmatic workarounds using existing DAGMan features (SUBDAG EXTERNAL, PRE/POST scripts, landing nodes, etc.) are expected and encouraged. Log the proper solution as a feature request, but build something that works today.
- **Payload agnosticism.** WMS2 does not know or care what runs inside a job (single-step, multi-step, StepChain). The sandbox/wrapper handles payload execution. Never add payload-specific logic to WMS2 core.
- **Single state machine owner.** The Request Lifecycle Manager is the only component that transitions request status. Other components are stateless workers called by it. Never add independent polling loops to components.
- **Site selection belongs to HTCondor.** WMS2 does not do load balancing or site assignment. The landing node mechanism lets HTCondor pick sites via normal negotiation. Never add site-selection logic to the DAG Planner.
- **One DAG per workflow from WMS2's perspective.** WMS2 submits and monitors one top-level DAG. Merge group sub-DAGs are DAGMan's internal concern.

## When committing changes

- **Update `IMPLEMENTATION_LOG.md`** with every significant commit. Add a new section or append to the current phase with: what was built/changed, design decisions, verification steps, and known issues.

## When editing the spec

- Bump the spec version in the metadata table when making substantial changes
- **Document the "why"**: When adding or changing a design decision, include a brief rationale — either inline (a sentence explaining the reasoning), in the relevant component section, or as an entry in Section 16 (Design Decisions). Someone reading only the spec should understand not just what the system does but why it does it that way.
- When a design choice was considered and rejected, note the alternative and why it was rejected (e.g., "WMS2 does not do site selection because that would duplicate HTCondor's negotiator")
- Update cross-references when renumbering sections
- Section 4 subsections are numbered 4.1–4.10; the component overview (4.1) lists all 9 components
- Enums (RequestStatus, WorkflowStatus, DAGStatus, OutputStatus, CleanupPolicy) must stay in sync between the data models (Section 3), the database schema (Section 3.2), and the Lifecycle Manager handlers (Section 4.2)
- Appendix C shows the DAGMan file format — keep it in sync with the DAG Planner code in Section 4.5
- Open questions are numbered sequentially in Section 15; currently 1–13
- Design decisions are numbered sequentially in Section 16

## Related repositories

- [WMCore](https://github.com/dmwm/WMCore) — The system WMS2 replaces. Reference for understanding current workflow management, WMAgent components, and ReqMgr2 schemas.
- [CRABServer](https://github.com/dmwm/CRABServer) — CMS Remote Analysis Builder. User-facing analysis submission system that sits on top of WMCore.
- [CMSRucio](https://github.com/dmwm/CMSRucio) — CMS-specific Rucio integration. Reference for data management patterns, rule creation, and transfer policies.
- [DBS](https://github.com/dmwm/DBS) — Data Bookkeeping System. WMS2's Output Manager registers datasets here. Reference for the DBS API (dataset injection, file registration, invalidation).
- [wmcoredb](https://github.com/dmwm/wmcoredb) — WMCore database schemas. Reference for understanding the existing data model that WMS2's PostgreSQL schema replaces.

## Domain terminology

- **DAGMan**: HTCondor's DAG manager — owns job-level orchestration
- **Schedd**: HTCondor scheduler daemon — manages job queues
- **SUBDAG EXTERNAL**: DAGMan feature where a node in one DAG is itself another DAG
- **Pilot**: A test job that measures performance before the full production DAG
- **Work unit**: The atomic unit of progress in WMS2 — a self-contained set of processing jobs whose outputs feed a single merge job. Implemented as a merge group SUBDAG. Used for progress tracking, partial production fractions, and clean stop granularity
- **Merge group**: The SUBDAG EXTERNAL implementation of a work unit — a set of processing nodes whose outputs feed one merge job, executed as a self-contained sub-DAG at a single site
- **Landing node**: A trivial `/bin/true` job per work unit that lets HTCondor pick the site via normal negotiation
- **AAA (Any data, Anytime, Anywhere)**: CMS xrootd federation for remote data access
- **GLIDEIN_CMSSite**: HTCondor classad identifying which CMS site a worker node belongs to
- **DBS**: CMS Data Bookkeeping System — catalog of datasets and files
- **Rucio**: Data management system for replica placement and transfer rules
- **ReqMgr2**: CMS Request Manager — source of workflow requests
- **CRIC**: CMS Resource Information Catalog — site configuration database
