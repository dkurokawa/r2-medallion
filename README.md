# r2-medallion

A daily Cloudflare Workflow that turns raw per-request API metrics (JSONL objects in R2)
into queryable **Apache Iceberg** tables — a bronze → silver → gold medallion built
entirely on Cloudflare: **Workflows**, **Pipelines**, **R2 Data Catalog** and **R2 SQL**.
No external compute, no separate warehouse.

日本語版: [README.ja.md](README.ja.md)

The emphasis of this repository is on **correctness under retries**: every day's run
reconciles its own row counts, then re-counts what actually landed in the tables and
flags duplicates, and the design choices below are driven by numbers observed on real
data rather than guesses.

- Scale it runs at: 6,878–32,557 bronze objects per day (as of 2026-09); a 240-day
  backfill was run through it.
- TypeScript (Workers runtime), 144 unit tests with Vitest, plus a PyIceberg helper
  for repair tasks.

---

## Architecture

```
R2 bucket (bronze)
  api-metrics/<dt>/<service>/<hour>/<file>.jsonl     ← written upstream, one object ≈ one request
        │
        │ cron 01:00 UTC → MEDALLION_WORKFLOW.create({ id: "day-<dt>", params: { dt } })
        ▼                  (or POST /run?dt=... for a manual run)
┌──────────────────────────── MedallionDayWorkflow ────────────────────────────┐
│ guard     _state/medallion/dt=<dt>.json already "done"? → exit                │
│ list      enumerate api-metrics/<dt>/ per service → lightweight               │
│           "chunk descriptors" (service, prefix, resume cursor, count)         │
│ chunk-<n> re-list from the descriptor → get → parse & type each JSONL line    │
│             ├ invalid lines → quarantine/api-metrics/dt=<dt>/chunk-<n>.jsonl  │
│             └ valid rows    → SILVER_STREAM.send()  (resumable, see below)    │
│           also returns partial aggregates per (service, endpoint, method)     │
│ gold      merge partials → exact percentiles → GOLD_STREAM.send()             │
│ manifest  three-way count reconciliation → _state/medallion/dt=<dt>.json      │
│ sleep     6 min (let the Pipelines sink roll its files)                       │
│ verify    R2 SQL: count(*) vs count(DISTINCT row_uid) on silver and gold      │
│ alert     anything wrong → Discord webhook                                    │
└──────────────────────────────────────────────────────────────────────────────┘
        │ SILVER_STREAM                         │ GOLD_STREAM
        ▼ Pipelines → R2 Data Catalog sink      ▼ Pipelines → R2 Data Catalog sink
  silver.api_metrics (Iceberg)            gold.api_metrics_daily (Iceberg)
        └──────────────── queried with R2 SQL (read-only) ───────────────┘

cron 02:00 UTC → independent health check: re-reads yesterday's manifest and alerts
                 if the workflow never wrote one (crash, never started, …)
```

This Worker is a **read-only consumer** of the bronze layer: it never modifies what the
upstream writers produce.

## Design decisions

### 1. The `list` step returns chunk descriptors, not keys

Workflows persists each `step.do` return value as instance state. Returning ~32k R2 keys
from one step risks hitting the persisted-state size limit (I could not find the exact
byte limit in the docs). So `list` returns only a few dozen descriptors —
`(service, prefix, resume cursor, count)`, closed at ~2,000 objects — and each
`chunk-<n>` step re-lists its own slice from the cursor. The extra `list()` calls are a
handful of Class A operations per day, negligible next to the ~32k `get()`s.
→ `src/lib/r2-list.ts`

### 2. Exact percentiles, not histograms

A day is at most ~32k rows, so sorting the whole day once is cheap. gold's
`latency_p50/p95/p99` are exact (nearest-rank), computed after merging every chunk's
partial aggregate. → `src/lib/percentile.ts`, `src/lib/aggregate.ts`

### 3. `latency_ms = 0` is excluded from latency statistics

Found on real data (2026-09-21): in Workers, `Date.now()` does not advance until the
isolate performs I/O, so responses returned without I/O (static replies, 404s, early
auth rejections) are recorded as 0 ms. That is not a measurement bug, but mixing those
rows into percentiles dragged p50 to 0 and hid how slow the real work was. gold now
computes latency stats from non-zero rows only and keeps the split explicitly:
`requests_measured` vs `requests_zero_latency` (latency columns are `null`, not `0`,
when nothing was measured).

### 4. 404s are folded into `(unmatched)` in gold

Vulnerability scanners hit `/.env`, `/wp-login.php`, `/phpinfo.php` and friends; on
2026-09-21 most of the 1,070 gold rows were one-off scanner paths. A 404 path is by
definition not an endpoint of the service, so gold groups them as `(unmatched)`.
silver keeps the raw path, so the scanner traffic is still investigable there.

### 5. Every day reconciles its own counts (three assertions)

The `manifest` step checks:

| assertion | catches |
|---|---|
| bronze JSONL lines = silver rows + quarantined rows | rows lost in parsing |
| silver rows = Σ gold `requests` | aggregation drift |
| objects listed = objects actually read (and 0 `get()` → `null`) | bronze objects that vanished between list and get |

The third one matters: without it, a missing object silently counts as "an object with
0 lines" and the other two equations still hold. On mismatch the manifest is written
with `status: "mismatch"` *before* the step throws, so the evidence survives.
→ `src/lib/manifest.ts`

### 6. Retries are at-least-once, so duplicates are made detectable and removable

What happened: after the 240-day backfill passed every manifest assertion, R2 SQL showed
**27,282 more silver rows than the manifests recorded** — one whole chunk duplicated on
12 of 240 days. `wrangler workflows instances describe` showed why: a `chunk-<n>` step
had completed `SILVER_STREAM.send()`, then failed with
`WorkflowInternalError: Attempt failed due to internal workflows error` before the step
result was persisted, and the automatic retry re-sent the whole chunk. Pipelines is
append-only, so the rows stayed.

This is the normal contract of a retried step (it may run more than once), so the fix
is on this side, in three layers:

1. **Narrow the window.** Each chunk sends in batches of 2,000 rows and writes a
   progress marker to R2 after every batch, keyed by the Workflow instance id. A retry
   reads the marker and resumes after the last sent batch. The duplicate window shrinks
   from "an entire step (up to 15 min)" to "between one `send()` succeeding and one small
   R2 `put()` completing". It is **not zero**. → `src/lib/chunk-marker.ts`
2. **Make every row identifiable.** silver rows carry
   `row_uid = <source R2 key>#<line number>`; gold rows carry
   `row_uid = <dt>|<service>|<endpoint>|<method>`. Re-sending the same source line always
   yields the same `row_uid`.
3. **Check the tables every day.** After the sink rolls, the `verify` step runs
   `SELECT count(*), count(DISTINCT row_uid)` on both tables for that day and records
   the result in the manifest: `mismatch` if the distinct count disagrees with the run's
   own bookkeeping, `duplicates` if `count(*) > distinct`. `verify` never throws —
   a throwing step would be retried, which is exactly how duplicates are produced.

Duplicates, when flagged, are removed with `scripts/dedupe_day.py` (PyIceberg), since
R2 SQL is read-only. Until then, readers can deduplicate with
`QUALIFY row_number() OVER (PARTITION BY row_uid ORDER BY __ingest_ts) = 1`.

### 7. Two independent alert paths

- The workflow's own `alert` step catches `duplicates` and `verify` errors.
- A second cron at 02:00 UTC re-reads yesterday's manifest and catches the case where
  the workflow never wrote one at all.

Both evaluate the same `manifestProblems()`; a bad day can produce two notifications,
which is intentional (missing an alert is worse). `notifyDiscord` never throws, so a
broken webhook cannot fail the pipeline. → `src/lib/alert.ts`, `src/index.ts`

### 8. Admin endpoints fail closed

`POST /run` and `GET /status` require `Authorization: Bearer <ADMIN_TOKEN>`. If the
secret is unset, every request is rejected with 401 rather than let through. The
comparison is constant-time (both sides SHA-256 hashed first).
→ `src/lib/auth.ts`

## Platform observations (as of 2026-09)

Behaviour I observed while building this, recorded with dates because these products
are evolving. Corrections welcome.

- **Sink files follow ingest time, not the table's partition spec.** I added an identity
  partition on `dt` (2026-09-25); files written by the Pipelines sink were still split by
  ingest time (`__ingest_ts`).
- **Changing the partition spec made the table unqueryable from R2 SQL.** With the
  default spec and existing manifests' specs disagreeing, every query returned
  `Query spans multiple partition specifications`. Resetting the default spec to 0
  restored it.
- **A PyIceberg write can report an error and still be committed** (seen 2026-09-24).
  `dedupe_day.py count <dt>` is the first thing to run after any error.
- **R2 SQL REST response shape**: the endpoint is documented in
  [Query data](https://developers.cloudflare.com/r2-sql/query-data/); the response field
  names (`result.rows` / `schema` / `metrics`, `success`, `errors`) I took from
  [cloudflare/skills](https://github.com/cloudflare/skills/blob/main/skills/cloudflare/references/r2-sql/api.md),
  since the docs page does not show a response example. `src/lib/r2sql.ts` follows it.

## Tables

**`silver.api_metrics`** — one row per request:
`ts`, `dt`, `service`, `endpoint`, `method`, `status`, `status_class`, `latency_ms`,
`request_size`, `response_size`, `user_id`, `cf_ray`, `cf_colo`, `error_message`,
`source_key`, `row_uid`. `service` comes from the R2 key path, not the payload — the
physical layout of bronze is trusted over payload contents.

**`gold.api_metrics_daily`** — one row per `dt × service × endpoint × method`:
`requests`, `errors_4xx`, `errors_5xx`, `error_rate`, `requests_measured`,
`requests_zero_latency`, `latency_p50/p95/p99/avg/max`, `colos`, `row_uid`.
`colos` is a sorted, comma-separated string (I could not confirm array support in
Pipelines schema files, so it uses a type that certainly works).

Schemas: [`schema/silver.json`](schema/silver.json), [`schema/gold.json`](schema/gold.json).

## Setup

Prerequisites: a bronze bucket with the layout above, `wrangler` logged in, and an API
token with R2 Data Catalog + R2 SQL + R2 Storage + Pipelines permissions.

```bash
pnpm install

# 1. Enable R2 Data Catalog on the bucket
npx wrangler r2 bucket catalog enable <BUCKET>

# 2. Streams (schemas from this repo)
npx wrangler pipelines streams create ppn_datalake_silver_api_metrics --schema-file schema/silver.json
npx wrangler pipelines streams create ppn_datalake_gold_api_metrics_daily --schema-file schema/gold.json

# 3. R2 Data Catalog sinks
npx wrangler pipelines sinks create ppn_datalake_silver_sink --type r2-data-catalog \
  --bucket <BUCKET> --namespace silver --table api_metrics --catalog-token <CATALOG_TOKEN>
npx wrangler pipelines sinks create ppn_datalake_gold_sink --type r2-data-catalog \
  --bucket <BUCKET> --namespace gold --table api_metrics_daily --catalog-token <CATALOG_TOKEN>

# 4. Connect stream → sink
npx wrangler pipelines create silver-pipeline \
  --sql "INSERT INTO ppn_datalake_silver_sink SELECT * FROM ppn_datalake_silver_api_metrics"
npx wrangler pipelines create gold-pipeline \
  --sql "INSERT INTO ppn_datalake_gold_sink SELECT * FROM ppn_datalake_gold_api_metrics_daily"

# 5. Table maintenance
npx wrangler r2 bucket catalog compaction enable <BUCKET> --target-size 128
npx wrangler r2 bucket catalog snapshot-expiration enable <BUCKET> --older-than-days 7 --retain-last 10

# 6. Fill in wrangler.toml: bucket name, <SILVER_STREAM_ID>, <GOLD_STREAM_ID>,
#    <CLOUDFLARE_ACCOUNT_ID>. `scripts/_patch_stream_ids.py` can write the stream ids
#    from `wrangler pipelines streams list`.

# 7. Secrets and deploy
npx wrangler secret put ADMIN_TOKEN            # required for /run and /status
npx wrangler secret put R2_SQL_TOKEN           # enables the verify step (skipped if unset)
npx wrangler secret put DISCORD_WEBHOOK_URL    # optional; otherwise alerts go to logs only
npx wrangler deploy
```

`scripts/rebuild-tables.sh` automates tearing down and recreating streams, sinks,
pipelines and tables (needed when a stream schema changes, since stream schemas are
fixed at creation).

## Operations

```bash
# Run / inspect a day
curl -X POST -H "Authorization: Bearer $ADMIN_TOKEN" "$WORKER_URL/run?dt=2026-09-21"
curl       -H "Authorization: Bearer $ADMIN_TOKEN" "$WORKER_URL/status?dt=2026-09-21"

# Backfill a range, a few days per round
ADMIN_TOKEN=... ./scripts/backfill.sh 2026-01-26 2026-09-21 "$WORKER_URL"

# Query
export WRANGLER_R2_SQL_AUTH_TOKEN=...
npx wrangler r2 sql query "<warehouse>" "
SELECT service, endpoint, requests, error_rate, latency_p95
FROM gold.api_metrics_daily WHERE dt = '2026-09-21'
ORDER BY requests DESC LIMIT 20"
```

**Redoing a day.** Pipelines is append-only and R2 SQL is read-only, so:

1. Delete that day's rows from both tables with Spark or PyIceberg
   ([Deleting data](https://developers.cloudflare.com/r2-data-catalog/deleting-data/)).
2. `POST /run?dt=<dt>&attempt=2&force=1`. Workflow `create()` is not idempotent for the
   same instance id, so `attempt=N` yields a fresh id (`day-<dt>-r<N>`) and a fresh set of
   progress markers; `force=1` skips the "already done" guard. The only duplicate guard is
   the `_state` record, not the instance id.

**Removing duplicates** (when `verify` reports `duplicates`):

```bash
python3 -m venv .venv && .venv/bin/pip install "pyiceberg[pyarrow,pyiceberg-core]"
export CF_ACCOUNT_ID=... WORKER_URL=...
.venv/bin/python scripts/dedupe_day.py scan-all [silver|gold]   # find affected days
.venv/bin/python scripts/dedupe_day.py dedupe <dt> [silver|gold]
.venv/bin/python scripts/dedupe_day.py verify-all               # always run afterwards
```

`verify-all` compares every day in both tables against its manifest and exits 1 on any
difference.

## Repository layout

```
src/
  index.ts              fetch (/run, /status) + scheduled (daily run, health check)
  workflow.ts           MedallionDayWorkflow — the steps above
  lib/
    r2-list.ts          chunk descriptors and resumable listing
    parse.ts            JSONL → typed silver rows, row_uid
    aggregate.ts        partial aggregates, merge, gold rows
    percentile.ts       exact nearest-rank percentiles
    manifest.ts         reconciliation and manifestProblems()
    chunk-marker.ts     per-batch send progress markers
    r2sql.ts            R2 SQL REST client for verify
    alert.ts            Discord notification (never throws)
    auth.ts             bearer-token check (fails closed)
    keys.ts             every R2 key shape in one place
    concurrency.ts      bounded-concurrency map for get()
schema/                 Pipelines stream schemas
scripts/                backfill, table rebuild, dedupe (PyIceberg)
```

## Development

```bash
pnpm install
pnpm typecheck
pnpm test        # 144 tests; `cloudflare:workers` is stubbed for Node
```

## Documented vs. not verified

Implementation choices follow these Cloudflare docs:
[Pipelines binding `stream` field](https://developers.cloudflare.com/changelog/post/2026-05-27-pipeline-binding-stream-field/) ·
[Writing to streams](https://developers.cloudflare.com/pipelines/streams/writing-to-streams/) ·
[Workflows: sleeping and retrying](https://developers.cloudflare.com/workflows/build/sleeping-and-retrying/) ·
[R2 SQL reference](https://developers.cloudflare.com/r2-sql/sql-reference/) (`QUALIFY`, window functions, `COUNT(DISTINCT)`) ·
[R2 Data Catalog: deleting data](https://developers.cloudflare.com/r2-data-catalog/deleting-data/)

Not confirmed in the docs, handled conservatively:

- the per-call payload limit of Pipelines `send()` → capped by row count (2,000 per batch)
- the persisted-state size limit of a Workflow step/instance → chunk descriptors
- array types in Pipelines schema files → `colos` is a string

## Cost notes

Per day: R2 Class A `list()` in the dozens; R2 Class B `get()` equal to the object count;
Pipelines ingestion roughly equal to the row count; one Workflow instance with
`services × chunks + ~4` steps. Actual prices depend on plan and usage — check the
Billing page rather than trusting estimates.

## License

MIT
