#!/usr/bin/env bash
# Show task states for wm_eod_batch runs.
#   scripts/dag_status.sh                 latest run
#   scripts/dag_status.sh <run_id>        a specific run
#   scripts/dag_status.sh --wait [run_id] poll every 30s until the run finishes
set -euo pipefail
cd "$(dirname "$0")/.."
AF=(env WM_HOME="$PWD" docker compose --project-directory "$HOME/aether_trade_project"
    -f "$HOME/aether_trade_project/docker-compose.yml"
    -f "$HOME/aether_trade_project/docker-compose.bigdata.yml"
    -f docker-compose.airflow-override.yml exec -T airflow-scheduler airflow)

runs_json() { "${AF[@]}" dags list-runs -d wm_eod_batch -o json 2>/dev/null; }

latest_run() {
  runs_json | python3 -c 'import json, sys; r = json.load(sys.stdin); print(r[0]["run_id"] if r else "")'
}

run_state() {
  runs_json | python3 -c '
import json, sys
for r in json.load(sys.stdin):
    if r["run_id"] == sys.argv[1]:
        print(r["state"])' "$1"
}

show() {
  echo "== $1 ($(run_state "$1"))"
  "${AF[@]}" tasks states-for-dag-run wm_eod_batch "$1" -o json 2>/dev/null | python3 -c '
import json, sys
order = ["wait_for_nas_feed", "ensure_catalog", "land_to_hdfs", "bronze_nas_files", "bronze_trades_stream",
         "bronze_crm_jdbc", "bronze_market", "silver_reference", "silver_transactions",
         "gold_book_of_record", "gold_reporting", "middle_office_recon", "publish_snowflake", "batch_complete"]
tasks = {t["task_id"]: t for t in json.load(sys.stdin)}
for tid in order:
    t = tasks.get(tid, {})
    state = t.get("state") or "-"
    ended = (t.get("end_date") or "")[:19]
    print(f"  {tid:24} {state:12} {ended}")'
}

if [[ "${1:-}" == "--wait" ]]; then
  run="${2:-$(latest_run)}"
  while true; do
    state=$(run_state "$run")
    [[ "$state" == "success" || "$state" == "failed" ]] && break
    sleep 30
  done
  show "$run"
else
  show "${1:-$(latest_run)}"
fi
