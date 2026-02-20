#!/usr/bin/env bash
# setup-dev-vm.sh — Provision a fresh AlmaLinux 9 VM for WMS2 development
#
# Run as root on a fresh VM image. Creates an 'agent' user that owns
# the repo, submits to HTCondor, and runs all dev tasks. After this
# script completes, all work (including Claude Code) should be done
# as the 'agent' user.
#
# Prerequisites (expected in the base image):
#   - AlmaLinux 9
#   - CVMFS mounted (/cvmfs/cms.cern.ch, /cvmfs/grid.cern.ch, /cvmfs/unpacked.cern.ch)
#   - /mnt/shared available (large working disk)
#   - /mnt/creds/x509up (grid proxy, optional)
#   - Internet access to CERN, HTCondor repos, PyPI
#
# Usage:
#   sudo WMS2_REPO_URL=https://github.com/your-org/wms2.git bash scripts/setup-dev-vm.sh
#
# If the repo is already cloned at /mnt/shared/work/wms2, WMS2_REPO_URL is optional.
#
# After completion:
#   su - agent
#   cd /mnt/shared/work/wms2
#   source .venv/bin/activate

set -euo pipefail

# ── Configuration ─────────────────────────────────────────────
DEV_USER="agent"
REPO_DIR="/mnt/shared/work/wms2"
VENV_DIR="$REPO_DIR/.venv"
PG_DB="wms2"
PG_TEST_DB="wms2test"
PG_USER="wms2"
PG_PASS="wms2dev"
REPO_URL="${WMS2_REPO_URL:-}"   # set this or export WMS2_REPO_URL before running
SITECONF_DIR="/opt/cms/siteconf"
CONDOR_CONFIG="/etc/condor/config.d/50-wms2-dev.conf"

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[0;33m'
NC='\033[0m'

ok()   { echo -e "  ${GREEN}OK${NC}    $1"; }
skip() { echo -e "  ${YELLOW}SKIP${NC}  $1 (already done)"; }
info() { echo -e "  ...   $1"; }
die()  { echo -e "  ${RED}FAIL${NC}  $1"; exit 1; }

echo "============================================"
echo " WMS2 Dev VM Setup"
echo " Target user: $DEV_USER"
echo " Repo:        $REPO_DIR"
echo "============================================"
echo ""

# Must be root
[[ $EUID -eq 0 ]] || die "Run this script as root"

# ── 1. Create agent user ──────────────────────────────────────
echo "--- 1. User: $DEV_USER ---"

if id "$DEV_USER" &>/dev/null; then
    skip "User $DEV_USER exists (uid=$(id -u $DEV_USER))"
else
    useradd -m -s /bin/bash "$DEV_USER"
    ok "Created user $DEV_USER (uid=$(id -u $DEV_USER))"
fi

# ── 2. Sudoers ────────────────────────────────────────────────
echo "--- 2. Sudoers ---"

SUDOERS_FILE="/etc/sudoers.d/$DEV_USER"
if [[ -f "$SUDOERS_FILE" ]]; then
    skip "Sudoers file exists"
else
    cat > "$SUDOERS_FILE" <<'SUDOEOF'
# WMS2 dev user — limited sudo for service management and package install
agent ALL=(ALL) NOPASSWD: /usr/bin/systemctl start *, /usr/bin/systemctl stop *, /usr/bin/systemctl restart *, /usr/bin/systemctl status *
agent ALL=(ALL) NOPASSWD: /usr/bin/systemctl enable *, /usr/bin/systemctl disable *
agent ALL=(ALL) NOPASSWD: /usr/bin/dnf install *, /usr/bin/dnf update *, /usr/bin/dnf remove *
agent ALL=(ALL) NOPASSWD: /usr/bin/chown *, /usr/bin/chmod *
agent ALL=(ALL) NOPASSWD: /usr/sbin/condor_reconfig
agent ALL=(ALL) NOPASSWD: /usr/pgsql-16/bin/psql -U postgres *
SUDOEOF
    chmod 440 "$SUDOERS_FILE"
    ok "Sudoers configured at $SUDOERS_FILE"
fi

# ── 3. System packages ───────────────────────────────────────
echo "--- 3. System packages ---"

PACKAGES=(
    python3.12
    python3.12-devel
    python3.12-pip
    git
    jq
)

# PostgreSQL 16 repo (the base OS only ships 13, project requires 15+)
if ! dnf list installed postgresql16-server &>/dev/null; then
    info "Adding PostgreSQL 16 repo..."
    dnf install -y -q https://download.postgresql.org/pub/repos/yum/reporpms/EL-9-x86_64/pgdg-redhat-repo-latest.noarch.rpm 2>/dev/null
    dnf -qy module disable postgresql 2>/dev/null   # disable built-in module to avoid conflicts
    ok "PostgreSQL 16 repo added"
else
    skip "PostgreSQL 16 already installed"
fi

PACKAGES+=(postgresql16-server postgresql16-contrib)

# HTCondor repo
if [[ ! -f /etc/yum.repos.d/htcondor.repo ]]; then
    info "Adding HTCondor 24.x repo..."
    mkdir -p /etc/pki/rpm-gpg
    curl -fsSL -o /etc/pki/rpm-gpg/HTCondor-24.x-Key \
        https://htcss-downloads.chtc.wisc.edu/repo/keys/HTCondor-24.x-Key
    rpm --import /etc/pki/rpm-gpg/HTCondor-24.x-Key
    cat > /etc/yum.repos.d/htcondor.repo <<'REPOEOF'
[htcondor]
name=HTCondor for Enterprise Linux 9 - Release
baseurl=https://htcss-downloads.chtc.wisc.edu/repo/24.x/el9/$basearch/release
enabled=1
gpgcheck=1
repo_gpgcheck=1
gpgkey=file:///etc/pki/rpm-gpg/HTCondor-24.x-Key
priority=90
REPOEOF
    ok "HTCondor repo added"
else
    skip "HTCondor repo exists"
fi

PACKAGES+=(condor)

info "Installing packages (this may take a minute)..."
dnf install -y -q "${PACKAGES[@]}" 2>/dev/null
ok "System packages installed"

# ── 4. PostgreSQL ─────────────────────────────────────────────
echo "--- 4. PostgreSQL ---"

PG_DATA="/var/lib/pgsql/16/data"
PG_SERVICE="postgresql-16"
if [[ ! -f "$PG_DATA/PG_VERSION" ]]; then
    info "Initializing PostgreSQL 16..."
    /usr/pgsql-16/bin/postgresql-16-setup initdb
    ok "PostgreSQL 16 initialized"
else
    skip "PostgreSQL 16 already initialized"
fi

# Configure pg_hba.conf for password auth on TCP
PG_HBA="$PG_DATA/pg_hba.conf"
# Match only active rules (not comments) that still use ident or peer for host connections
if grep -qE '^host\s+all\s+all\s+.*\s+(ident|peer)' "$PG_HBA" 2>/dev/null; then
    sed -i 's/^host\s\+all\s\+all\s\+127.0.0.1\/32\s\+\(ident\|peer\)/host    all             all             127.0.0.1\/32            scram-sha-256/' "$PG_HBA"
    sed -i 's/^host\s\+all\s\+all\s\+::1\/128\s\+\(ident\|peer\)/host    all             all             ::1\/128                 scram-sha-256/' "$PG_HBA"
    chown postgres:postgres "$PG_HBA"
    ok "pg_hba.conf configured for scram-sha-256"
else
    skip "pg_hba.conf already configured"
fi

# Ensure postgres owns its config files
chown postgres:postgres "$PG_HBA"

# Stop old PostgreSQL (base OS version) if running
systemctl stop postgresql 2>/dev/null
systemctl disable postgresql 2>/dev/null

# Start PostgreSQL 16
systemctl enable --now "$PG_SERVICE"
ok "PostgreSQL 16 running"

# Ensure password_encryption matches pg_hba.conf auth method
su - postgres -c "/usr/pgsql-16/bin/psql -c \"ALTER SYSTEM SET password_encryption = 'scram-sha-256'\"" 2>/dev/null
systemctl reload "$PG_SERVICE" 2>/dev/null

# Create role and databases
PG16_PSQL="/usr/pgsql-16/bin/psql"
info "Creating PostgreSQL role and databases..."
su - postgres -c "$PG16_PSQL -tc \"SELECT 1 FROM pg_roles WHERE rolname='$PG_USER'\"" | grep -q 1 || \
    su - postgres -c "$PG16_PSQL -c \"CREATE ROLE $PG_USER WITH LOGIN PASSWORD '$PG_PASS'\""

su - postgres -c "$PG16_PSQL -tc \"SELECT 1 FROM pg_database WHERE datname='$PG_DB'\"" | grep -q 1 || \
    su - postgres -c "$PG16_PSQL -c \"CREATE DATABASE $PG_DB OWNER $PG_USER\""

su - postgres -c "$PG16_PSQL -tc \"SELECT 1 FROM pg_database WHERE datname='$PG_TEST_DB'\"" | grep -q 1 || \
    su - postgres -c "$PG16_PSQL -c \"CREATE DATABASE $PG_TEST_DB OWNER $PG_USER\""

# pgcrypto extension
su - postgres -c "$PG16_PSQL -d $PG_DB -c 'CREATE EXTENSION IF NOT EXISTS pgcrypto'" 2>/dev/null
su - postgres -c "$PG16_PSQL -d $PG_TEST_DB -c 'CREATE EXTENSION IF NOT EXISTS pgcrypto'" 2>/dev/null

ok "PostgreSQL: role=$PG_USER, databases=$PG_DB + $PG_TEST_DB"

# ── 5. HTCondor ───────────────────────────────────────────────
echo "--- 5. HTCondor ---"

if [[ ! -f "$CONDOR_CONFIG" ]]; then
    cat > "$CONDOR_CONFIG" <<'CONDOREOF'
# WMS2 development personal condor configuration
# Runs a full local pool for DAGMan testing

DAEMON_LIST = MASTER, COLLECTOR, NEGOTIATOR, SCHEDD, STARTD

# Use this machine as both submit and execute host
CONDOR_HOST = $(FULL_HOSTNAME)

# Allow unauthenticated local connections for dev simplicity
SEC_DEFAULT_AUTHENTICATION = OPTIONAL
SEC_DEFAULT_AUTHENTICATION_METHODS = FS, IDTOKENS
ALLOW_READ = *
ALLOW_WRITE = *
ALLOW_NEGOTIATOR = *
ALLOW_DAEMON = *
ALLOW_ADMINISTRATOR = *

# Use all available resources (detect automatically)
NUM_CPUS = $(DETECTED_CPUS)
MEMORY = $(DETECTED_MEMORY)

# Partitionable slot (default) — supports dynamic 8-core sub-slots
SLOT_TYPE_1_PARTITIONABLE = TRUE
SLOT_TYPE_1 = cpus=100%, memory=100%, disk=100%
NUM_SLOTS_TYPE_1 = 1

# DAGMan settings for development
DAGMAN_MAX_JOBS_IDLE = 100
DAGMAN_MAX_JOBS_SUBMITTED = 200

# Reduce negotiation cycle for faster turnaround in dev
NEGOTIATOR_INTERVAL = 20

# Release claimed slots quickly so landing node resources return to the pool
CLAIM_WORKLIFE = 20
CONDOREOF
    chmod 644 "$CONDOR_CONFIG"
    ok "HTCondor config written to $CONDOR_CONFIG"
else
    # Fix ALLOW_ADMINISTRATOR if it's restricted (caused condor_reconfig failures)
    if grep -q 'ALLOW_ADMINISTRATOR = \$(CONDOR_HOST)' "$CONDOR_CONFIG"; then
        sed -i 's/ALLOW_ADMINISTRATOR = \$(CONDOR_HOST)/ALLOW_ADMINISTRATOR = */' "$CONDOR_CONFIG"
        ok "Fixed ALLOW_ADMINISTRATOR = * (was restricted to CONDOR_HOST)"
    else
        skip "HTCondor config exists"
    fi
fi

systemctl enable --now condor
ok "HTCondor running"

# ── 6. CMS Siteconf ──────────────────────────────────────────
echo "--- 6. CMS Siteconf ---"

if [[ ! -f "$SITECONF_DIR/JobConfig/site-local-config.xml" ]]; then
    mkdir -p "$SITECONF_DIR/JobConfig"
    mkdir -p "$SITECONF_DIR/PhEDEx"

    cat > "$SITECONF_DIR/JobConfig/site-local-config.xml" <<'SLCEOF'
<site-local-config>
<site name="T2_LOCAL_DEV">
  <event-data>
    <catalog url="trivialcatalog_file:/opt/cms/siteconf/PhEDEx/storage.xml?protocol=xrootd"/>
  </event-data>
  <data-access>
    <catalog volume="LOCAL_DEV" protocol="XRootD"/>
  </data-access>
  <calib-data>
    <frontier-connect>
      <proxy url="http://cmsbpfrontier.cern.ch:3128"/>
      <proxy url="http://cmsbpfrontier1.cern.ch:3128"/>
      <proxy url="http://cmsbpfrontier2.cern.ch:3128"/>
      <proxy url="http://cmsbproxy.fnal.gov:3128"/>
      <server url="http://cmsfrontier.cern.ch:8000/FrontierInt"/>
      <server url="http://cmsfrontier1.cern.ch:8000/FrontierInt"/>
      <server url="http://cmsfrontier2.cern.ch:8000/FrontierInt"/>
      <server url="http://cmsfrontier3.cern.ch:8000/FrontierInt"/>
      <server url="http://cmsfrontier4.cern.ch:8000/FrontierInt"/>
    </frontier-connect>
  </calib-data>
</site>
</site-local-config>
SLCEOF

    cat > "$SITECONF_DIR/PhEDEx/storage.xml" <<'SXEOF'
<storage-mapping>
  <lfn-to-pfn protocol="xrootd" path-match="/+store/(.*)" result="root://cms-xrd-global.cern.ch//store/$1"/>
  <lfn-to-pfn protocol="direct" path-match="(.*)" result="$1"/>
  <pfn-to-lfn protocol="xrootd" path-match="root://cms-xrd-global.cern.ch//(.*)" result="/$1"/>
  <pfn-to-lfn protocol="direct" path-match="(.*)" result="$1"/>
</storage-mapping>
SXEOF

    cat > "$SITECONF_DIR/storage.json" <<'SJEOF'
[
    {
        "site": "T2_LOCAL_DEV",
        "volume": "LOCAL_DEV",
        "protocols": [
            {
                "protocol": "XRootD",
                "access": "global-ro",
                "prefix": "root://cms-xrd-global.cern.ch/"
            }
        ],
        "type": "DISK",
        "rse": "T2_LOCAL_DEV"
    }
]
SJEOF
    ok "CMS siteconf created at $SITECONF_DIR"
else
    skip "CMS siteconf exists"
fi

# ── 7. Directory ownership ────────────────────────────────────
echo "--- 7. Directory ownership ---"

# /mnt/shared/work
mkdir -p /mnt/shared/work
chown "$DEV_USER:$DEV_USER" /mnt/shared/work

# /mnt/shared/store (output area)
mkdir -p /mnt/shared/store
chown "$DEV_USER:$DEV_USER" /mnt/shared/store

# Repo directory (if exists)
if [[ -d "$REPO_DIR" ]]; then
    chown -R "$DEV_USER:$DEV_USER" "$REPO_DIR"
    ok "Repo ownership set to $DEV_USER"
else
    info "Repo not yet cloned — will be set up in step 8"
fi

ok "Directory ownership configured"

# ── 8. Python venv ────────────────────────────────────────────
echo "--- 8. Python venv ---"

if [[ ! -d "$REPO_DIR" ]]; then
    if [[ -n "$REPO_URL" ]]; then
        info "Cloning repo from $REPO_URL..."
        su - "$DEV_USER" -c "git clone '$REPO_URL' '$REPO_DIR'"
        ok "Repo cloned to $REPO_DIR"
    else
        echo -e "  ${YELLOW}WARN${NC}  Repo not cloned — set WMS2_REPO_URL and re-run, or clone manually:"
        echo "        git clone <repo-url> $REPO_DIR"
    fi
fi

if [[ -d "$REPO_DIR" ]]; then
    if [[ ! -f "$VENV_DIR/bin/activate" ]]; then
        info "Creating Python 3.12 venv..."
        su - "$DEV_USER" -c "python3.12 -m venv $VENV_DIR"
        ok "Venv created at $VENV_DIR"
    else
        skip "Venv exists"
    fi

    info "Installing packages..."
    su - "$DEV_USER" -c "
        source $VENV_DIR/bin/activate
        pip install --quiet --upgrade pip
        pip install --quiet -e '$REPO_DIR[dev]'
        pip install --quiet htcondor
    "
    ok "Python packages installed"
else
    echo "  NOTE: Create venv after cloning repo"
fi

# ── 9. Shell profile for agent ────────────────────────────────
echo "--- 9. Shell profile ---"

PROFILE="/home/$DEV_USER/.bashrc"
MARKER="# WMS2 dev environment"

if ! grep -q "$MARKER" "$PROFILE" 2>/dev/null; then
    cat >> "$PROFILE" <<PROFILEEOF

$MARKER
export PATH=/usr/pgsql-16/bin:\$PATH
cd $REPO_DIR 2>/dev/null
source $VENV_DIR/bin/activate 2>/dev/null

# CMS environment
export SITECONFIG_PATH=$SITECONF_DIR
export CMS_PATH=$SITECONF_DIR
export X509_CERT_DIR=/cvmfs/grid.cern.ch/etc/grid-security/certificates
export X509_USER_PROXY=/mnt/creds/x509up
PROFILEEOF
    chown "$DEV_USER:$DEV_USER" "$PROFILE"
    ok "Shell profile configured (auto-activate venv, CMS env vars)"
else
    skip "Shell profile already configured"
fi

# ── 10. Generate environment.local.md ─────────────────────────
echo "--- 10. Environment profile ---"

LOCAL_ENV="$REPO_DIR/.claude/environment.local.md"
if [[ -d "$REPO_DIR/.claude" ]]; then
    HOSTNAME_VAL=$(hostname)
    OS_VAL=$(source /etc/os-release 2>/dev/null && echo "$NAME $VERSION" || uname -sr)
    CPUS_VAL=$(nproc)
    MEM_VAL=$(free -h | awk '/Mem:/{print $2}')
    DISK_ROOT=$(df -h / | awk 'NR==2{print $2 " total, " $4 " free"}')
    DISK_SHARED=$(df -h /mnt/shared 2>/dev/null | awk 'NR==2{print $2 " total, " $4 " free"}' || echo "N/A")
    PG_VER=$(/usr/pgsql-16/bin/psql --version 2>/dev/null | awk '{print $3}' || echo "unknown")
    CONDOR_VER=$(condor_version 2>/dev/null | head -1 | awk '{print $2}' || echo "unknown")
    PY_VER=$("$VENV_DIR/bin/python3" --version 2>&1 | awk '{print $2}' || echo "unknown")
    GIT_VER=$(git --version 2>/dev/null | awk '{print $3}' || echo "unknown")
    TODAY=$(date +%Y-%m-%d)

    cat > "$LOCAL_ENV" <<ENVEOF
# WMS2 Local Environment — $HOSTNAME_VAL

Last updated: $TODAY (generated by setup-dev-vm.sh)

## Machine
- **Hostname**: $HOSTNAME_VAL
- **OS**: $OS_VAL
- **CPUs**: $CPUS_VAL
- **Memory**: $MEM_VAL
- **Disk**: $DISK_ROOT (root), $DISK_SHARED (/mnt/shared)

## Deployment Style
dev-local (full local stack)

## Services

### PostgreSQL
- **Location**: local
- **Host**: 127.0.0.1
- **Port**: 5432
- **Version**: $PG_VER
- **Status**: running (systemd enabled)
- **Databases**: \`$PG_DB\` (app), \`$PG_TEST_DB\` (integration tests)
- **User**: $PG_USER
- **Credentials**: password \`$PG_PASS\` (TCP via scram-sha-256); peer auth via unix socket for postgres superuser
- **Extensions**: pgcrypto (in both databases)

### HTCondor
- **Location**: local
- **Version**: $CONDOR_VER
- **Components**: master, collector, negotiator, schedd, startd
- **Status**: running (systemd enabled)
- **Config**: $CONDOR_CONFIG
- **Python bindings**: htcondor2 (pip) — import as \`htcondor2\`
- **Job submission user**: $DEV_USER

## Tools

### Python
- **System**: $(python3 --version 2>&1) at $(which python3)
- **Dev**: Python $PY_VER in venv
- **Virtual env**: $VENV_DIR
- **Activate**: \`source $VENV_DIR/bin/activate\`

### Git
- **Version**: $GIT_VER

### Other tools
- **psql**: installed ($PG_VER)
- **jq**: $(jq --version 2>/dev/null || echo "not installed")

## CMS / CMSSW Environment

### Site Configuration
- **Location**: $SITECONF_DIR
- **Site name**: T2_LOCAL_DEV

### Environment Variables (set by .bashrc)
- \`SITECONFIG_PATH=$SITECONF_DIR\`
- \`X509_CERT_DIR=/cvmfs/grid.cern.ch/etc/grid-security/certificates\`
- \`X509_USER_PROXY=/mnt/creds/x509up\`

## Network Access
- Run \`scripts/check-env.sh\` to test CMS endpoint reachability

## Known Limitations
- HTCondor Python bindings from pip use \`htcondor2\` module (v2 API), not \`htcondor\` (v1)
- CMS grid proxy at \`/mnt/creds/x509up\` — must be refreshed periodically
- CMSSW el8 releases need apptainer container on el9 host
ENVEOF
    chown "$DEV_USER:$DEV_USER" "$LOCAL_ENV"
    ok "Generated $LOCAL_ENV"
else
    info "Repo not cloned — skipping environment.local.md"
fi

# ── 11. Verify ────────────────────────────────────────────────
echo ""
echo "--- Verification ---"

# PostgreSQL
if PGPASSWORD="$PG_PASS" /usr/pgsql-16/bin/psql -h 127.0.0.1 -U "$PG_USER" -d "$PG_DB" -c "SELECT 1" &>/dev/null; then
    ok "PostgreSQL: $PG_USER@$PG_DB accessible"
else
    echo -e "  ${RED}FAIL${NC}  PostgreSQL connection failed"
fi

# HTCondor
SLOTS=$(condor_status -total 2>/dev/null | awk '/^ *Total +[0-9]/{print $2}')
if [[ -n "$SLOTS" ]] && [[ "$SLOTS" -gt 0 ]]; then
    CPUS=$(condor_config_val NUM_CPUS 2>/dev/null)
    ok "HTCondor: $CPUS CPUs, $SLOTS slot(s)"
else
    echo -e "  ${YELLOW}WARN${NC}  HTCondor: no slots yet (may need a minute to start)"
fi

# HTCondor submit test
if su - "$DEV_USER" -c "condor_q -total" &>/dev/null; then
    ok "HTCondor: $DEV_USER can query schedd"
else
    echo -e "  ${RED}FAIL${NC}  $DEV_USER cannot query HTCondor schedd"
fi

# Python
if [[ -f "$VENV_DIR/bin/python3" ]]; then
    PY_VER=$("$VENV_DIR/bin/python3" --version 2>&1)
    ok "Python: $PY_VER in venv"
fi

# CVMFS
if [[ -d /cvmfs/cms.cern.ch ]]; then
    ok "CVMFS: /cvmfs/cms.cern.ch mounted"
else
    echo -e "  ${YELLOW}WARN${NC}  CVMFS not mounted (needed for CMSSW)"
fi

echo ""
echo "============================================"
echo " Setup complete!"
echo ""
echo " To start working:"
echo "   su - $DEV_USER"
echo ""
echo " The shell auto-activates the venv and sets"
echo " CMS environment variables. Run tests with:"
echo "   pytest tests/ -v"
echo ""
echo " For Claude Code, launch as $DEV_USER."
echo "============================================"
