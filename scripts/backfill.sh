#!/usr/bin/env bash
# Backfill the medallion for a date range, a few days at a time (one Workflow
# instance per day, a small batch of days per round).
#
# Usage:
#   ADMIN_TOKEN=... ./scripts/backfill.sh 2026-01-26 2026-09-21 [worker_url] [sleep_seconds] [batch_size] [attempt] [force]
#
# ADMIN_TOKEN is read from the environment only — never pass it as an
# argument (it would land in shell history / process listings). This script
# never echoes it.
#
# [attempt] (optional, positive integer): redo an already-attempted range —
# `create()` is not idempotent, so re-POSTing the SAME dt without a fresh
# `attempt` just gets a 409 for every day that already has an instance
# (README "Operations"). Pass e.g. `2` the second time you run this
# script over a range you're redoing (after deleting those days' rows —
# see README); bump it again for a third attempt, etc. Leave unset for a
# first pass / for days that have never been run.
#
# [force] (optional, `1` or `true`): passed through as `&force=1`, which
# skips the workflow's "manifest already says done → exit" guard for every
# day in the range. Use this together with a fresh [attempt] to rebuild a
# range whose rows you've deleted from silver/gold but whose `_state`
# manifests you have NOT deleted (avoids deleting 240 manifest files one by
# one). Requires [attempt] to be set too (a positional placeholder) — pass
# it even if you'd otherwise leave it unset.
set -euo pipefail

if [ -z "${ADMIN_TOKEN:-}" ]; then
  echo "ADMIN_TOKEN must be set in the environment (not as an argument)." >&2
  exit 1
fi

START_DATE="${1:?usage: $0 <start YYYY-MM-DD> <end YYYY-MM-DD> [worker_url] [sleep_seconds] [batch_size] [attempt]}"
END_DATE="${2:?usage: $0 <start YYYY-MM-DD> <end YYYY-MM-DD> [worker_url] [sleep_seconds] [batch_size] [attempt]}"
WORKER_URL="${3:-${WORKER_URL:-}}"
if [ -z "$WORKER_URL" ]; then
  echo "Pass the Worker URL as the 3rd argument or set WORKER_URL." >&2
  exit 1
fi
SLEEP_SECONDS="${4:-5}"
BATCH_SIZE="${5:-3}"
ATTEMPT="${6:-}"
FORCE="${7:-}"

if [ -n "$ATTEMPT" ] && ! [[ "$ATTEMPT" =~ ^[1-9][0-9]*$ ]]; then
  echo "attempt must be a positive integer (1, 2, 3, ...), got: $ATTEMPT" >&2
  exit 1
fi

if [ -n "$FORCE" ] && [ "$FORCE" != "1" ] && [ "$FORCE" != "true" ]; then
  echo "force must be 1 or true if given, got: $FORCE" >&2
  exit 1
fi

date_to_epoch() {
  # GNU date and BSD/macOS date have incompatible -d/-j flags; try both.
  date -j -f "%Y-%m-%d" "$1" "+%s" 2>/dev/null || date -d "$1" "+%s"
}

epoch_to_date() {
  date -u -r "$1" "+%Y-%m-%d" 2>/dev/null || date -u -d "@$1" "+%Y-%m-%d"
}

start_epoch=$(date_to_epoch "$START_DATE")
end_epoch=$(date_to_epoch "$END_DATE")

if [ "$start_epoch" -gt "$end_epoch" ]; then
  echo "start date must be <= end date" >&2
  exit 1
fi

day=$((60 * 60 * 24))
current_epoch="$start_epoch"
count_in_batch=0

attempt_suffix=""
[ -n "$ATTEMPT" ] && attempt_suffix=" attempt=$ATTEMPT"
force_suffix=""
[ -n "$FORCE" ] && force_suffix=" force=$FORCE"
echo "Backfilling $START_DATE..$END_DATE against $WORKER_URL (batch=$BATCH_SIZE, sleep=${SLEEP_SECONDS}s between requests$attempt_suffix$force_suffix)"

while [ "$current_epoch" -le "$end_epoch" ]; do
  dt=$(epoch_to_date "$current_epoch")
  run_url="$WORKER_URL/run?dt=$dt"
  [ -n "$ATTEMPT" ] && run_url="$run_url&attempt=$ATTEMPT"
  [ -n "$FORCE" ] && run_url="$run_url&force=$FORCE"
  echo "-> POST ${run_url#"$WORKER_URL"}"
  http_status=$(curl -sS -o /tmp/r2-medallion-backfill-response.json -w '%{http_code}' \
    -X POST "$run_url" \
    -H "Authorization: Bearer $ADMIN_TOKEN")
  echo "   HTTP $http_status: $(cat /tmp/r2-medallion-backfill-response.json)"
  rm -f /tmp/r2-medallion-backfill-response.json

  current_epoch=$((current_epoch + day))
  count_in_batch=$((count_in_batch + 1))

  if [ "$count_in_batch" -ge "$BATCH_SIZE" ] && [ "$current_epoch" -le "$end_epoch" ]; then
    echo "   (batch of $BATCH_SIZE dispatched — sleeping ${SLEEP_SECONDS}s)"
    sleep "$SLEEP_SECONDS"
    count_in_batch=0
  else
    sleep 1
  fi
done

echo "Done. Poll GET $WORKER_URL/status?dt=<date> (same bearer token) to check each day's manifest."
