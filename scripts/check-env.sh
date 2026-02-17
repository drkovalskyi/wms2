#!/usr/bin/env bash
# check-env.sh — Quick environment check for WMS2 development
# Run this to verify your dev environment or to generate initial diagnostics
# on a fresh machine.

set -euo pipefail

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[0;33m'
NC='\033[0m'

ok()   { echo -e "  ${GREEN}OK${NC}    $1"; }
warn() { echo -e "  ${YELLOW}WARN${NC}  $1"; }
fail() { echo -e "  ${RED}MISS${NC}  $1"; }

echo "=== WMS2 Environment Check ==="
echo ""
echo "Machine: $(hostname) | $(uname -r)"
echo "CPUs: $(nproc) | Memory: $(free -h | awk '/Mem:/{print $2}')"
echo ""

# --- Required tools ---
echo "--- Required Tools ---"

if command -v python3.12 &>/dev/null; then
    ok "Python 3.12 ($(python3.12 --version 2>&1 | awk '{print $2}'))"
elif command -v python3.11 &>/dev/null; then
    ok "Python 3.11 ($(python3.11 --version 2>&1 | awk '{print $2}'))"
elif command -v python3 &>/dev/null; then
    PY_VER=$(python3 --version 2>&1 | awk '{print $2}')
    PY_MINOR=$(echo "$PY_VER" | cut -d. -f2)
    if [ "$PY_MINOR" -ge 11 ]; then
        ok "Python ($PY_VER)"
    else
        fail "Python $PY_VER — need 3.11+ (have $PY_VER)"
    fi
else
    fail "Python — not found"
fi

if command -v git &>/dev/null; then
    ok "git ($(git --version | awk '{print $3}'))"
else
    fail "git — not found"
fi

if command -v psql &>/dev/null; then
    ok "psql ($(psql --version | awk '{print $3}'))"
else
    fail "psql — not found"
fi

if command -v condor_version &>/dev/null; then
    ok "HTCondor ($(condor_version | head -1 | awk '{print $2}'))"
else
    fail "HTCondor — not found"
fi

echo ""

# --- Services ---
echo "--- Services ---"

if systemctl is-active postgresql &>/dev/null; then
    ok "PostgreSQL — running"
    if PGPASSWORD=wms2dev psql -h 127.0.0.1 -U wms2 -d wms2 -c "SELECT 1" &>/dev/null; then
        ok "PostgreSQL wms2 database — accessible"
    else
        warn "PostgreSQL running but wms2 database not accessible"
    fi
else
    fail "PostgreSQL — not running"
fi

if systemctl is-active condor &>/dev/null; then
    ok "HTCondor — running"
    SLOTS=$(condor_status -total 2>/dev/null | tail -1 | awk '{print $1}')
    if [ -n "$SLOTS" ] && [ "$SLOTS" -gt 0 ] 2>/dev/null; then
        ok "HTCondor pool — $SLOTS slot(s)"
    else
        warn "HTCondor running but no slots advertised yet"
    fi
else
    fail "HTCondor — not running"
fi

echo ""

# --- Python venv ---
echo "--- Python Virtual Environment ---"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
VENV_DIR="$PROJECT_DIR/.venv"

if [ -f "$VENV_DIR/bin/activate" ]; then
    ok "venv exists at $VENV_DIR"
    VENV_PY="$VENV_DIR/bin/python3"
    if $VENV_PY -c "import fastapi, sqlalchemy, pydantic, uvicorn, pytest, alembic, httpx, asyncpg" 2>/dev/null; then
        ok "Core packages importable"
    else
        warn "Some core packages missing — run: $VENV_DIR/bin/pip install -r requirements-dev.txt"
    fi
    if $VENV_PY -c "import htcondor2" 2>/dev/null; then
        ok "htcondor2 bindings importable"
    else
        warn "htcondor2 bindings not installed"
    fi
else
    fail "No venv at $VENV_DIR — create with: python3.12 -m venv $VENV_DIR"
fi

echo ""

# --- Local environment file ---
echo "--- Environment Profile ---"

LOCAL_ENV="$PROJECT_DIR/.claude/environment.local.md"
if [ -f "$LOCAL_ENV" ]; then
    UPDATED=$(grep -m1 "Last updated" "$LOCAL_ENV" 2>/dev/null || echo "unknown")
    ok "environment.local.md exists ($UPDATED)"
else
    warn "No .claude/environment.local.md — ask Claude to generate it"
fi

echo ""
echo "=== Done ==="
