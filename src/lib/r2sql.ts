/**
 * Minimal R2 SQL REST client — just enough to run the two duplicate-
 * detection count queries the `verify` workflow step needs (see
 * `workflow.ts` `runVerify` and README "Design decisions §6"). Not a general
 * SQL client: no pagination, no schema introspection.
 *
 * Request/response shape verified 2026-09-23 against:
 *  - https://developers.cloudflare.com/r2-sql/query-data/ (endpoint, method,
 *    headers, `{"query": "<SQL>"}` request body — confirmed via fetched
 *    page content).
 *  - https://github.com/cloudflare/skills/blob/main/skills/cloudflare/references/r2-sql/api.md
 *    (Cloudflare's own agent-skill reference for R2 SQL), which additionally
 *    documents the exact response envelope used below:
 *    `{"result": {"request_id", "schema", "rows", "metrics"}, "success": true, "errors": []}`
 *    on success, `{"result": null, "success": false, "errors": [{"code","message"}]}`
 *    on failure. Cloudflare's own product docs pages did not show a response
 *    example, so this second source is what the shape
 *    below is actually verified against.
 *  - QUALIFY and window functions (`ROW_NUMBER() OVER (...)`) are documented
 *    as supported in the same reference (and in
 *    https://developers.cloudflare.com/r2-sql/sql-reference/) — used by the
 *    README's canonical dedupe query, not by this module.
 */

export interface R2SqlRow {
  [column: string]: unknown;
}

export interface R2SqlSuccessResponse {
  success: true;
  result: {
    request_id?: string;
    schema?: unknown[];
    rows: R2SqlRow[];
    metrics?: Record<string, unknown>;
  };
  errors: unknown[];
}

export interface R2SqlErrorResponse {
  success: false;
  result: null;
  errors: { code?: number; message?: string }[];
}

export type R2SqlResponse = R2SqlSuccessResponse | R2SqlErrorResponse;

export interface R2SqlQueryOptions {
  /** Cloudflare account id (not secret — see `Env.R2_SQL_ACCOUNT_ID`). */
  accountId: string;
  /** R2 bucket name (`ppn-datalake-bronze` — matches wrangler.toml `[[r2_buckets]] bucket_name`). */
  bucket: string;
  /** Bearer token (`Env.R2_SQL_TOKEN`). */
  token: string;
  /** Test-only fetch override. Defaults to the global `fetch`. */
  fetchImpl?: typeof fetch;
}

/** The R2 bucket this app's tables live in — see README "Architecture". */
export const R2_SQL_BUCKET = 'ppn-datalake-bronze';

export function r2SqlQueryUrl(accountId: string, bucket: string): string {
  return `https://api.sql.cloudflarestorage.com/api/v1/accounts/${accountId}/r2-sql/query/${bucket}`;
}

/**
 * POSTs one read-only SQL statement to the R2 SQL REST endpoint. Throws on
 * any failure (HTTP-level, or a `{"success": false, ...}` response body) —
 * callers decide how to degrade (the `verify` workflow step catches this and
 * records `verify: { error }` rather than failing the day; see workflow.ts).
 */
export async function queryR2Sql(options: R2SqlQueryOptions, sql: string): Promise<R2SqlResponse> {
  const doFetch = options.fetchImpl ?? fetch;
  const res = await doFetch(r2SqlQueryUrl(options.accountId, options.bucket), {
    method: 'POST',
    headers: {
      Authorization: `Bearer ${options.token}`,
      'Content-Type': 'application/json',
    },
    body: JSON.stringify({ query: sql }),
  });
  let body: R2SqlResponse;
  try {
    body = (await res.json()) as R2SqlResponse;
  } catch (err) {
    throw new Error(
      `R2 SQL request: HTTP ${res.status}, response body was not valid JSON (${String(err)})`,
    );
  }
  if (!res.ok && body?.success !== false) {
    // Defensive: an HTTP-level failure that didn't come back in the
    // documented {success:false,...} envelope (e.g. a gateway/edge error
    // page that happens to be valid JSON).
    throw new Error(`R2 SQL request failed: HTTP ${res.status}`);
  }
  return body;
}

const DT_RE = /^\d{4}-\d{2}-\d{2}$/;

export interface RowCounts {
  /** `count(*)` — every row sent for this `dt`, including duplicates. */
  total: number;
  /** `count(DISTINCT row_uid)` — the true number of unique bronze lines/groups. */
  distinct: number;
}

/**
 * `SELECT count(*) AS n, count(DISTINCT row_uid) AS d FROM <table> WHERE dt = '<dt>'`
 * — the canonical duplicate-detection query (README "Design decisions §6").
 * `dt` is re-validated here (belt-and-suspenders — every caller already
 * validated it as `YYYY-MM-DD` upstream: `DATE_RE` in index.ts for a manual
 * `/run`, or `yesterdayUTC()` for cron) before interpolating it into SQL,
 * since R2 SQL has no parameterized-query API to bind it safely instead.
 */
export async function queryRowCounts(
  options: R2SqlQueryOptions,
  table: string,
  dt: string,
): Promise<RowCounts> {
  if (!DT_RE.test(dt)) {
    throw new Error(`queryRowCounts: dt must be YYYY-MM-DD, got ${JSON.stringify(dt)}`);
  }
  const sql = `SELECT count(*) AS n, count(DISTINCT row_uid) AS d FROM ${table} WHERE dt = '${dt}'`;
  const resp = await queryR2Sql(options, sql);
  if (!resp.success) {
    const detail = resp.errors.map((e) => e.message ?? JSON.stringify(e)).join('; ');
    throw new Error(`R2 SQL query failed for ${table}: ${detail}`);
  }
  const row = resp.result.rows[0];
  if (!row) {
    throw new Error(`R2 SQL query for ${table} returned no rows`);
  }
  const total = Number(row.n);
  const distinct = Number(row.d);
  if (!Number.isFinite(total) || !Number.isFinite(distinct)) {
    throw new Error(
      `R2 SQL query for ${table} returned non-numeric counts: n=${JSON.stringify(row.n)} d=${JSON.stringify(row.d)}`,
    );
  }
  return { total, distinct };
}
