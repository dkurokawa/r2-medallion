# r2-medallion

A daily Cloudflare Workflow that turns per-request API metrics (JSONL in R2) into
Apache Iceberg tables (silver / gold), built only on **Workflows, Pipelines,
R2 Data Catalog and R2 SQL** — no external compute, no separate warehouse.

日本語版: [README.ja.md](README.ja.md)

- Scale: 6,878–32,557 bronze objects per day (as of 2026-09); 240 days backfilled.
- Focus: correctness under retries. Each day reconciles its own counts, then re-counts
  what actually landed in the tables.

## Flow

```
R2 bronze (JSONL) ─ cron 01:00 UTC ─▶ MedallionDayWorkflow
  list → chunk-<n> (typing; bad lines to quarantine) → gold (aggregate) → manifest (reconcile)
  → verify (re-count with R2 SQL) → alert (Discord)
        │ Pipelines                          │ Pipelines
        ▼                                    ▼
  silver.api_metrics (Iceberg)        gold.api_metrics_daily (Iceberg)

cron 02:00 UTC ─ health check that re-reads yesterday's manifest
```

## Design decisions

**`list` returns chunk descriptors, not keys.** Workflows persists step return values, and
~32k keys could approach the state size limit (I could not find the exact limit in the docs).
The step returns only `(service, prefix, cursor, count)` per ~2,000 objects; each chunk
re-lists its own slice.

**`latency_ms = 0` is excluded from latency stats.** `Date.now()` in Workers does not advance
until I/O happens, so I/O-free responses (404s, early auth rejections) record 0 ms. Mixed in,
they pinned p50 to 0 and hid the real latency (found on 2026-09-21 data).

**404s are folded into `(unmatched)` in gold.** Scanner traffic (`/.env` and friends) made most
of the 1,070 gold rows on 2026-09-21 one-off paths. silver keeps the raw path.

**Exact percentiles.** ~32k rows a day is cheap to sort.

**Three-way count reconciliation.** bronze lines = silver + quarantine; silver = Σ gold
requests; objects listed = objects read. Without the third, a vanished object passes silently
as "0 lines".

**Duplicates are assumed possible, so they are made detectable and removable.** After the
240-day backfill, silver had 27,282 more rows than the manifests (one chunk on 12 of 240 days).
A step had finished `send()`, then failed with `WorkflowInternalError` before its result was
persisted, and the retry re-sent the chunk. Retried steps may run more than once, so steps
need to be idempotent: an R2 `put` to a fixed key is, but Pipelines `send()` is append-only and
is not. So this is handled on this side:

1. A progress marker in R2 every 2,000 rows lets a retry resume (narrows the window; not zero).
2. Every row carries a `row_uid` (source key + line number), so duplicates are identifiable.
3. `verify` compares `count(*)` with `count(DISTINCT row_uid)` daily. It never throws — a
   throwing step is retried, which is how duplicates happen in the first place.

Flagged days are cleaned with `scripts/dedupe_day.py` (PyIceberg), since R2 SQL is read-only.

**Two alert paths.** The in-workflow `alert` step, plus a 02:00 cron that catches days where
no manifest was written at all.

## Platform observations (as of 2026-09)

Recorded with dates since these products are evolving. Corrections welcome.

- Files written by the Pipelines sink were split by ingest time, not by the table's `dt`
  partition spec.
- After changing the partition spec, every R2 SQL query failed with
  `Query spans multiple partition specifications`; resetting the default spec to 0 fixed it.
- A PyIceberg write reported an error but had been committed (2026-09-24).
- The R2 SQL REST response shape is not shown in the docs; I used
  [cloudflare/skills](https://github.com/cloudflare/skills/blob/main/skills/cloudflare/references/r2-sql/api.md).

## Setup

Needs a bronze bucket laid out as `api-metrics/<dt>/<service>/<hour>/*.jsonl` and an API token
with R2 Data Catalog, R2 SQL, R2 Storage and Pipelines permissions.

```bash
npx wrangler r2 bucket catalog enable <BUCKET>

npx wrangler pipelines streams create ppn_datalake_silver_api_metrics --schema-file schema/silver.json --http-enabled false
npx wrangler pipelines streams create ppn_datalake_gold_api_metrics_daily --schema-file schema/gold.json --http-enabled false

npx wrangler pipelines sinks create ppn_datalake_silver_sink --type r2-data-catalog \
  --bucket <BUCKET> --namespace silver --table api_metrics --catalog-token <CATALOG_TOKEN>
npx wrangler pipelines sinks create ppn_datalake_gold_sink --type r2-data-catalog \
  --bucket <BUCKET> --namespace gold --table api_metrics_daily --catalog-token <CATALOG_TOKEN>

npx wrangler pipelines create ppn_datalake_silver_pipeline \
  --sql "INSERT INTO ppn_datalake_silver_sink SELECT * FROM ppn_datalake_silver_api_metrics"
npx wrangler pipelines create ppn_datalake_gold_pipeline \
  --sql "INSERT INTO ppn_datalake_gold_sink SELECT * FROM ppn_datalake_gold_api_metrics_daily"

npx wrangler r2 bucket catalog compaction enable <BUCKET> --target-size 128
npx wrangler r2 bucket catalog snapshot-expiration enable <BUCKET> --older-than-days 7 --retain-last 10

# fill the placeholders in wrangler.toml, then
npx wrangler secret put ADMIN_TOKEN           # required for /run and /status
npx wrangler secret put R2_SQL_TOKEN          # enables verify
npx wrangler secret put DISCORD_WEBHOOK_URL   # optional
npx wrangler deploy
```

`scripts/rebuild-tables.sh` recreates all of the above when a schema changes.

## Operations

- Run or inspect a day: `POST /run?dt=YYYY-MM-DD`, `GET /status?dt=YYYY-MM-DD`
  (Bearer `ADMIN_TOKEN`). Backfill: `scripts/backfill.sh`.
- Redo a day: delete its rows with Spark or PyIceberg, then
  `POST /run?dt=<dt>&attempt=2&force=1`. Workflow `create()` is not idempotent for the same
  instance id (a second call errors), so `attempt` yields a fresh one.
- Duplicates: `scripts/dedupe_day.py scan-all | dedupe <dt> | verify-all`
  (always `verify-all` afterwards).

## Development

```bash
pnpm install && pnpm typecheck && pnpm test
```

## License

MIT
