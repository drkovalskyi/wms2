#!/usr/bin/env bash
# Start WMS2 service with nohup.  Kills any existing uvicorn first.
set -euo pipefail
cd "$(dirname "$0")/.."

# Clear stale bytecode
find src -name "*.pyc" -delete

# Kill old uvicorn
pkill -9 -f "uvicorn wms2" 2>/dev/null && sleep 2 || true

# Activate venv
source .venv/bin/activate

# Start with nohup — logs to wms2.log
nohup env \
  WMS2_CONDOR_HOST="${WMS2_CONDOR_HOST:-localhost:9618}" \
  WMS2_LIFECYCLE_CYCLE_INTERVAL="${WMS2_LIFECYCLE_CYCLE_INTERVAL:-30}" \
  WMS2_EXTRA_COLLECTORS="${WMS2_EXTRA_COLLECTORS:-}" \
  WMS2_SPOOL_MOUNT="${WMS2_SPOOL_MOUNT:-}" \
  WMS2_REMOTE_SPOOL_PREFIX="${WMS2_REMOTE_SPOOL_PREFIX:-}" \
  WMS2_REMOTE_SCHEDD="${WMS2_REMOTE_SCHEDD:-}" \
  WMS2_SEC_TOKEN_DIRECTORY="${WMS2_SEC_TOKEN_DIRECTORY:-}" \
  uvicorn wms2.main:create_app --factory --host 0.0.0.0 --port 8080 \
  >> wms2.log 2>&1 &

echo "WMS2 started (PID $!) — logging to wms2.log"
