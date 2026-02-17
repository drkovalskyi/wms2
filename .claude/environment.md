# WMS2 Development Environment Requirements

This file defines what the WMS2 project needs. It is committed to the repo.
Each developer creates `.claude/environment.local.md` (gitignored) to describe
what is actually available on their machine.

## Required Services

| Service | Min Version | Purpose | Notes |
|---|---|---|---|
| PostgreSQL | 15 | Primary database | ACID, JSONB, task queue |
| HTCondor | 23+ | Job orchestration engine | DAGMan, schedd, negotiator |

## Required Tools

| Tool | Min Version | Purpose |
|---|---|---|
| Python | 3.11 | Application language |
| pip | latest | Package management |
| git | 2.x | Version control |

## Required Python Packages (development)

| Package | Min Version | Purpose |
|---|---|---|
| FastAPI | 0.100+ | API framework |
| SQLAlchemy | 2.0 | ORM / async DB access |
| Pydantic | 2.0 | Data validation |
| uvicorn | latest | ASGI server |
| pytest | latest | Testing |
| alembic | latest | DB migrations |
| httpx | latest | HTTP client / test client |
| htcondor | latest | HTCondor Python bindings |

## External CMS Services (network access)

| Service | Protocol | Required for |
|---|---|---|
| ReqMgr2 | REST API | Importing workflow requests |
| DBS | REST API | Output dataset registration |
| Rucio | REST API | Replica lookup, rule management |
| CRIC | REST API | Site configuration |

## Optional / Nice-to-Have

| Tool | Purpose |
|---|---|
| Docker / Podman | Containerized testing |
| pgAdmin or DBeaver | Database GUI |
| condor_q, condor_status | HTCondor CLI inspection |

---

## Local Environment Template

Copy the block below to `.claude/environment.local.md` and fill in your
machine-specific details:

```markdown
# WMS2 Local Environment — [machine name]

## Machine
- **Hostname**:
- **OS**:
- **CPUs**:
- **Memory**:
- **Disk**:

## Deployment Style
<!-- One of: dev-local, dev-remote-services, production-like, ci -->

## Services

### PostgreSQL
- **Location**: local | remote
- **Host**:
- **Port**:
- **Version**:
- **Status**: installed | not-installed | available-remote
- **Credentials**: (reference, not literal — e.g., "see .env" or "peer auth")

### HTCondor
- **Location**: local | remote | not-available
- **Version**:
- **Components available**: schedd, negotiator, startd, dagman
- **Status**: installed | not-installed | available-remote
- **Notes**:

## Tools

### Python
- **Path**:
- **Version**:
- **Virtual env**: (path or "none")

### Other tools
- **Docker/Podman**: installed | not-installed
- **pg cli (psql)**: installed | not-installed

## Network Access
- **CMS network (ReqMgr2, DBS, Rucio, CRIC)**: reachable | not-reachable | via-proxy
- **Notes**:

## Known Limitations
<!-- Anything missing or broken on this machine -->
```
