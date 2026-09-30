#!/usr/bin/env bash
# silver / gold の stream・sink・pipeline と Iceberg テーブルを作り直す。
#
# schema/*.json を変えたら必ず要る作業（wrangler に stream のスキーマ更新も
# テーブル削除のコマンドも無いため）。手で 6 コマンド叩く代わりに、これ 1 本で:
#   1. pipeline → sink → stream を消す（依存の順）
#   2. Iceberg テーブルを消す（Data Catalog の REST）
#   3. schema/*.json から stream を作り直し、sink と pipeline をつなぎ直す
#   4. 新しい stream の ID を wrangler.toml に書き戻す
#   5. Worker をデプロイし直す
#
# 使い方:
#   CATALOG_TOKEN=... ./scripts/rebuild-tables.sh            # 確認あり
#   CATALOG_TOKEN=... ./scripts/rebuild-tables.sh --yes      # 確認なし
#   CATALOG_TOKEN=... ./scripts/rebuild-tables.sh --dry-run  # 何をするか出すだけ
#
# CATALOG_TOKEN は R2 Data Catalog + R2 Storage の権限を持つ API トークン。
# 環境変数からのみ受け取る（引数にすると履歴とプロセス一覧に残る）。
# ⚠️ テーブルの中身は消える。bronze（R2 の api-metrics/）は触らないので、
#    消したあとは scripts/backfill.sh で作り直せる。
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
  # 手元に保存してあるならそれを使う（画面には出さない）
  if [ -r "$HOME/.config/r2-medallion/r2-catalog-token" ]; then
    CATALOG_TOKEN="$(< "$HOME/.config/r2-medallion/r2-catalog-token")"
  else
    echo "CATALOG_TOKEN を環境変数で渡すか ~/.config/r2-medallion/r2-catalog-token に置いてください。" >&2
    exit 1
  fi
fi

run() {
  if [ "$DRY" = 1 ]; then
    # トークンは画面に出さない（CLAUDE.md「機密値の取り扱い」）
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

# Data Catalog の REST は prefix 付きのパスを要求する。warehouse ごとに違うので毎回引く。
prefix() {
  curl -sS -H "Authorization: Bearer $CATALOG_TOKEN" \
    "${CATALOG_BASE}/config?warehouse=${ACCOUNT_ID}_${BUCKET}" \
    | python3 -c "import json,sys; print(json.load(sys.stdin)['overrides']['prefix'])"
}

echo "▶ 作り直す対象: silver.api_metrics / gold.api_metrics_daily（テーブルの中身は消える）"
if [ "$YES" != 1 ] && [ "$DRY" != 1 ]; then
  read -r -p "  進めますか? [y/N] " ans
  [ "$ans" = "y" ] || { echo "やめました。"; exit 0; }
fi

if [ "$DRY" = 1 ]; then
  PFX="<prefix>"   # dry-run では通信しない
else
  PFX="$(prefix)"
  [ -n "$PFX" ] || { echo "catalog prefix を取れませんでした（トークンの権限を確認）"; exit 1; }
fi

echo "▶ 1/5 pipeline・sink・stream を消す"
for n in silver gold; do
  run npx wrangler pipelines delete "ppn_datalake_${n}_pipeline" --force
  run npx wrangler pipelines sinks delete "ppn_datalake_${n}_sink" --force
done
run npx wrangler pipelines streams delete ppn_datalake_silver_api_metrics --force
run npx wrangler pipelines streams delete ppn_datalake_gold_api_metrics_daily --force

echo "▶ 2/5 Iceberg テーブルを消す"
for pair in "silver/api_metrics" "gold/api_metrics_daily"; do
  ns="${pair%%/*}"; tbl="${pair##*/}"
  if [ "$DRY" = 1 ]; then
    echo "  [dry-run] DELETE ${CATALOG_BASE}/${PFX}/namespaces/${ns}/tables/${tbl}"
  else
    code=$(curl -sS -o /dev/null -w '%{http_code}' -X DELETE \
      -H "Authorization: Bearer $CATALOG_TOKEN" \
      "${CATALOG_BASE}/${PFX}/namespaces/${ns}/tables/${tbl}?purgeRequested=true")
    echo "  ${ns}.${tbl}: HTTP ${code}"   # 404 は「既に無い」なので続行してよい
  fi
done

echo "▶ 3/5 stream・sink・pipeline を作り直す"
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

echo "▶ 4/5 新しい stream の ID を wrangler.toml に書き戻す"
if [ "$DRY" = 1 ]; then
  echo "  [dry-run] streams list → wrangler.toml の stream = \"...\" を置き換え"
else
  npx wrangler pipelines streams list 2>/dev/null | python3 "$(dirname "$0")/_patch_stream_ids.py"
fi

echo "▶ 5/5 デプロイ"
run npx wrangler deploy

echo "✅ 作り直し完了。240日を入れ直すなら:"
echo "   ADMIN_TOKEN=... WORKER_URL=... ./scripts/backfill.sh 2026-01-26 \$(date -u -v-1d +%Y-%m-%d) '' 5 3 <attempt> 1"
