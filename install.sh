#!/bin/bash
# WMS2 development environment setup — run as: sudo bash install.sh
# RHEL 9
set -euo pipefail

echo "=== WMS2 Dev Environment Setup ==="

# ── 1. PostgreSQL (uncomment if not already set up) ─────────────
# dnf install -y postgresql-server postgresql
# postgresql-setup --initdb
# systemctl start postgresql && systemctl enable postgresql
# sed -i 's/^\(local.*all.*all.*\)peer/\1md5/' /var/lib/pgsql/data/pg_hba.conf
# sed -i 's/^\(host.*all.*all.*127.0.0.1.*\)ident/\1md5/' /var/lib/pgsql/data/pg_hba.conf
# sed -i 's/^\(host.*all.*all.*::1.*\)ident/\1md5/' /var/lib/pgsql/data/pg_hba.conf
# systemctl restart postgresql
# su -c "psql -c \"CREATE USER wms2 WITH PASSWORD 'wms2pass';\"" - postgres
# su -c "psql -c \"CREATE DATABASE wms2 OWNER wms2;\"" - postgres
# su -c "psql -c \"CREATE USER wms2test WITH PASSWORD 'wms2test';\"" - postgres
# su -c "psql -c \"CREATE DATABASE wms2test OWNER wms2test;\"" - postgres
# su -c "psql -d wms2 -c 'CREATE EXTENSION IF NOT EXISTS pgcrypto;'" - postgres
# su -c "psql -d wms2test -c 'CREATE EXTENSION IF NOT EXISTS pgcrypto;'" - postgres

# ── 2. HTCondor ─────────────────────────────────────────────────
echo "--- Setting up HTCondor ---"
if ! command -v condor_version &>/dev/null; then
    # Prerequisites: EPEL + CodeReady Builder repos
    dnf install -y epel-release
    # RHEL 9: enable CRB (name varies by distro)
    dnf config-manager --set-enabled crb 2>/dev/null \
        || dnf config-manager --set-enabled rhel-9-codeready-builder 2>/dev/null \
        || true  # may already be enabled

    # HTCondor 25.x official repo
    dnf install -y https://htcss-downloads.chtc.wisc.edu/repo/25.x/htcondor-release-current.el9.noarch.rpm

    dnf install -y htcondor
fi

# Configure as personal condor (all daemons on localhost)
cat > /etc/condor/config.d/00-personal.conf <<'CONF'
CONDOR_HOST = 127.0.0.1
USE_SHARED_PORT = TRUE
SHARED_PORT_ARGS = -p 9618
DAEMON_LIST = MASTER, COLLECTOR, NEGOTIATOR, SCHEDD, STARTD

ALLOW_WRITE = *
ALLOW_READ = *
ALLOW_ADMINISTRATOR = $(CONDOR_HOST)
ALLOW_NEGOTIATOR = $(CONDOR_HOST)

NUM_CPUS = 2
MEMORY = 4096
DISK = 10000000

NEGOTIATOR_INTERVAL = 10
NEGOTIATOR_CYCLE_DELAY = 5
SCHEDD_INTERVAL = 10

DAGMAN_USE_DIRECT_SUBMIT = True
DAGMAN_MAX_RESCUE_NUM = 10
CONF

systemctl start condor
systemctl enable condor

echo ""
echo "=== Setup Complete ==="
echo ""
condor_version
echo ""
echo "Verify with:"
echo "  condor_status    # should show slots"
echo "  condor_q         # should show empty queue"
