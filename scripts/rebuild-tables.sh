#!/usr/bin/env bash
# Recreate the silver / gold streams, sinks, pipelines and Iceberg tables.
#
# Required whenever schema/*.json changes (wrangler has no command to update a
# stream schema or to drop a table). Instead of typing the commands by hand:
#   1. delete pipeline → sink → stream (dependency order)
#   2. drop the Iceberg tables (Data Catalog REST API)
#   3. recreate streams from schema/*.json and reconnect sinks and pipelines
#   4. write the new stream ids back into wrangler.toml
#   5. redeploy the Worker
#
# Usage:
#   CATALOG_TOKEN=... ./scripts/rebuild-tables.sh            # asks for confirmation
#   CATALOG_TOKEN=... ./scripts/rebuild-tables.sh --yes      # no confirmation
#   CATALOG_TOKEN=... ./scripts/rebuild-tables.sh --dry-run  # print what would happen
#
# CATALOG_TOKEN is an API token with R2 Data Catalog + R2 Storage permissions.
# It is taken from the environment only (as an argument it would land in shell
# history and process listings). CF_ACCOUNT_ID must be set as well.
# ⚠️ Table contents are dropped. bronze (api-metrics/ in R2) is untouched, so the
#    tables can be refilled with scripts/backfill.sh afterwards.
set -uo pipefail

cd "$(dirname "$0")/.."

ACCOUNT_ID="${CF_ACCOUNT_ID:?set CF_ACCOUNT_ID (wrangler whoami)}"
BUCKET=ppn-datalake-bronze
CATALOG_BASE="https://catalog.cloudflarestorage.com/${ACCOUNT_ID}/${BUCKET}/v1"

YES=0
DRY=0
for arg in "$@"; do
  case "$arg" in
    --yes) YES=1 ;;
    --dry-run) DRY=1 ;;
    *) echo "unknown argument: $arg" >&2; exit 2 ;;
  esac
done

if [ -z "${CATALOG_TOKEN:-}" ]; then
  # fall back to a locally stored token (never printed)
  if [ -r "$HOME/.config/r2-medallion/r2-catalog-token" ]; then
    CATALOG_TOKEN="$(< "$HOME/.config/r2-medallion/r2-catalog-token")"
  else
    echo "Set CATALOG_TOKEN or put the token in ~/.config/r2-medallion/r2-catalog-token." >&2
    exit 1
  fi
fi

run() {
  if [ "$DRY" = 1 ]; then
    # never print the token
    local shown=()
    local mask=0
    for a in "$@"; do
      if [ "$mask" = 1 ]; then shown+=("***"); mask=0; continue; fi
      [ "$a" = "--catalog-token" ] && mask=1
      shown+=("$a")
    done
    echo "  [dry-run] ${shown[*]}"
  else
    "$@"
  fi
}

# The Data Catalog REST API needs a per-warehouse path prefix; look it up each time.
prefix() {
  curl -sS -H "Authorization: Bearer $CATALOG_TOKEN" \
    "${CATALOG_BASE}/config?warehouse=${ACCOUNT_ID}_${BUCKET}" \
    | python3 -c "import json,sys; print(json.load(sys.stdin)['overrides']['prefix'])"
}

echo "▶ will recreate: silver.api_metrics / gold.api_metrics_daily (table contents will be dropped)"
if [ "$YES" != 1 ] && [ "$DRY" != 1 ]; then
  read -r -p "  proceed? [y/N] " ans
  [ "$ans" = "y" ] || { echo "aborted."; exit 0; }
fi

if [ "$DRY" = 1 ]; then
  PFX="<prefix>"   # no network calls in dry-run
else
  PFX="$(prefix)"
  [ -n "$PFX" ] || { echo "could not get the catalog prefix (check the token permissions)"; exit 1; }
fi

echo "▶ 1/5 delete pipelines, sinks, streams"
for n in silver gold; do
  run npx wrangler pipelines delete "ppn_datalake_${n}_pipeline" --force
  run npx wrangler pipelines sinks delete "ppn_datalake_${n}_sink" --force
done
run npx wrangler pipelines streams delete ppn_datalake_silver_api_metrics --force
run npx wrangler pipelines streams delete ppn_datalake_gold_api_metrics_daily --force

echo "▶ 2/5 drop Iceberg tables"
for pair in "silver/api_metrics" "gold/api_metrics_daily"; do
  ns="${pair%%/*}"; tbl="${pair##*/}"
  if [ "$DRY" = 1 ]; then
    echo "  [dry-run] DELETE ${CATALOG_BASE}/${PFX}/namespaces/${ns}/tables/${tbl}"
  else
    code=$(curl -sS -o /dev/null -w '%{http_code}' -X DELETE \
      -H "Authorization: Bearer $CATALOG_TOKEN" \
      "${CATALOG_BASE}/${PFX}/namespaces/${ns}/tables/${tbl}?purgeRequested=true")
    echo "  ${ns}.${tbl}: HTTP ${code}"   # 404 means already gone; safe to continue
  fi
done

echo "▶ 3/5 recreate streams, sinks, pipelines"
run npx wrangler pipelines streams create ppn_datalake_silver_api_metrics \
  --schema-file schema/silver.json --http-enabled false
run npx wrangler pipelines streams create ppn_datalake_gold_api_metrics_daily \
  --schema-file schema/gold.json --http-enabled false
run npx wrangler pipelines sinks create ppn_datalake_silver_sink --type r2-data-catalog \
  --bucket "$BUCKET" --namespace silver --table api_metrics --catalog-token "$CATALOG_TOKEN"
run npx wrangler pipelines sinks create ppn_datalake_gold_sink --type r2-data-catalog \
  --bucket "$BUCKET" --namespace gold --table api_metrics_daily --catalog-token "$CATALOG_TOKEN"
run npx wrangler pipelines create ppn_datalake_silver_pipeline \
  --sql "INSERT INTO ppn_datalake_silver_sink SELECT * FROM ppn_datalake_silver_api_metrics"
run npx wrangler pipelines create ppn_datalake_gold_pipeline \
  --sql "INSERT INTO ppn_datalake_gold_sink SELECT * FROM ppn_datalake_gold_api_metrics_daily"

echo "▶ 4/5 write new stream ids into wrangler.toml"
if [ "$DRY" = 1 ]; then
  echo "  [dry-run] streams list → replace stream = \"...\" in wrangler.toml"
else
  npx wrangler pipelines streams list 2>/dev/null | python3 "$(dirname "$0")/_patch_stream_ids.py"
fi

echo "▶ 5/5 deploy"
run npx wrangler deploy

echo "✅ rebuild complete. To refill the tables:"
echo "   ADMIN_TOKEN=... WORKER_URL=... ./scripts/backfill.sh 2026-01-26 \$(date -u -v-1d +%Y-%m-%d) '' 5 3 <attempt> 1"
