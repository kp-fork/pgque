#!/usr/bin/env bash
# End-to-end: provision, then run both workloads and print reports.
# Override any knob via env, e.g.  A_WORKERS=16 B_SLOTS=16 bash run.sh
set -Eeuo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DB="${PGQUE_DB:-pgque_repro}"

PGQUE_DB="$DB" bash "$HERE/setup.sh"

run_driver() {
  sudo -u postgres PGQUE_DSN="dbname=${DB}" python3 "$HERE/driver.py" "$@"
}

echo
echo "=================== TIER A — mutual exclusion (migrations) ==================="
echo "    cooperative consumers + per-key advisory lock; order irrelevant"
run_driver --tier a \
  --tenants "${A_TENANTS:-2000}" --dups "${A_DUPS:-4}" \
  --workers "${A_WORKERS:-8}" --work-ms "${A_WORK_MS:-3}" --chunk "${CHUNK:-500}"

echo
echo "=================== TIER B — ordered per key (lifecycle) ==================="
echo "    N hash-routed slot subscriptions; FIFO within a key, parallel across keys"
run_driver --tier b \
  --tenants "${B_TENANTS:-500}" --events-per-tenant "${B_EPT:-20}" \
  --slots "${B_SLOTS:-8}" --chunk "${CHUNK:-500}"

echo
echo "Done. Tweak scale with A_*/B_* env vars (see README.md)."
