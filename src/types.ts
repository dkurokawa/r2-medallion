/**
 * r2-medallion — shared types.
 *
 * Note on binding types: `WorkflowBinding<T>` and `PipelineBinding<T>` below
 * are OUR OWN minimal interfaces, not the ambient Cloudflare types. This is a
 * deliberate choice: the Pipelines product is newer than the Workflows one,
 * and we could not confirm that the pinned `@cloudflare/workers-types`
 * version ships an ambient `Pipeline<T>` global (the docs describe the
 * runtime shape but not a TS type export). Declaring our own narrow
 * interfaces means `Env` typechecks regardless, and callers only rely on the
 * one method (`send`) the docs confirm exists. `WorkflowEntrypoint` itself
 * (in workflow.ts) DOES use the real ambient class from `cloudflare:workers`,
 * since that one must match the runtime exactly for the class to work as a
 * Workflow entrypoint.
 */

export interface MedallionDayParams {
  /** YYYY-MM-DD, UTC. */
  dt: string;
  /**
   * `POST /run?force=1` sets this. Skips the `guard` step's "manifest
   * already says done → exit" check, so a rebuild (rows deleted from
   * silver/gold via PySpark, but the `_state` manifest left in place) can
   * be re-run without deleting 240 `_state` files first. Does NOT bypass
   * `create()`'s own non-idempotency — a second `create()` with the same
   * instance id still errors regardless of `force`; a real redo still needs
   * a fresh `?attempt=N`. The scheduled (cron) handler never sets this.
   */
  force?: boolean;
}

export interface SilverRow {
  ts: string;
  dt: string;
  service: string;
  endpoint: string;
  method: string;
  status: number;
  status_class: string;
  latency_ms: number;
  request_size: number | null;
  response_size: number | null;
  user_id: string | null;
  cf_ray: string | null;
  cf_colo: string | null;
  error_message: string | null;
  source_key: string;
  /**
   * Deterministic id for the bronze *line* this row came from:
   * `<source_key>#<0-based line index within that object>`. Same bronze
   * line always produces the same row_uid, no matter how many times it is
   * (re-)sent — see `lib/parse.ts` `silverRowUid` and README "Design decisions §6". This is what makes a duplicate send (send() succeeds, the
   * step's completion isn't durably recorded, Workflows retries and resends
   * the same rows — the incident this app hit in production 2026-09-23)
   * exactly identifiable and removable at read time, since Pipelines itself
   * has no dedup.
   */
  row_uid: string;
}

export interface GoldRow {
  dt: string;
  service: string;
  endpoint: string;
  method: string;
  requests: number;
  errors_4xx: number;
  errors_5xx: number;
  error_rate: number;
  /** Requests whose latency_ms was > 0 — the ones the latency_* columns describe. */
  requests_measured: number;
  /** Requests that measured 0 ms because the Worker did no I/O (see GroupAgg.zeroLatency). */
  requests_zero_latency: number;
  latency_p50: number | null;
  latency_p95: number | null;
  latency_p99: number | null;
  latency_avg: number | null;
  latency_max: number | null;
  /** Sorted, comma-joined, unique CF colo codes seen for this group (e.g. "ICN,KIX,NRT"). */
  colos: string;
  /**
   * Deterministic id for the (dt, service, endpoint, method) group this gold
   * row summarizes: `<dt>|<service>|<endpoint>|<method>`. Same group always
   * produces the same row_uid — see `lib/aggregate.ts` `goldRowUid`. Same
   * purpose as `SilverRow.row_uid` (identify/remove duplicates at read
   * time), applied to gold instead of silver.
   */
  row_uid: string;
}

export interface PipelineBinding<T> {
  send(records: T[]): Promise<void>;
}

export interface WorkflowInstanceHandle {
  id: string;
  status(): Promise<{ status: string; [key: string]: unknown }>;
}

export interface WorkflowBinding<T> {
  create(options: { id: string; params: T }): Promise<WorkflowInstanceHandle>;
  get(id: string): Promise<WorkflowInstanceHandle>;
}

export interface Env {
  DATALAKE_R2: R2Bucket;
  MEDALLION_WORKFLOW: WorkflowBinding<MedallionDayParams>;
  SILVER_STREAM: PipelineBinding<SilverRow>;
  GOLD_STREAM: PipelineBinding<GoldRow>;
  /** Bearer secret for POST /run and GET /status. Set via `wrangler secret put ADMIN_TOKEN`. */
  ADMIN_TOKEN: string;
  ENVIRONMENT: string;
  /**
   * Cloudflare account id, used only to build the R2 SQL REST endpoint URL
   * (`.../accounts/<id>/r2-sql/query/<bucket>`). Not a secret — an account
   * id alone grants no access — so it's a `[vars]` entry in wrangler.toml,
   * not a Worker secret. See `lib/r2sql.ts`.
   */
  R2_SQL_ACCOUNT_ID: string;
  /**
   * Bearer token for the R2 SQL REST API (`wrangler secret put
   * R2_SQL_TOKEN` — see README "Setup"). Optional at the type level on
   * purpose: the `verify` workflow step must degrade to a recorded skip,
   * not fail the day, when this secret hasn't been set yet — see
   * `workflow.ts` `runVerify`.
   */
  R2_SQL_TOKEN?: string;
  /**
   * Discord webhook URL for the failure-alert path (`wrangler secret put
   * DISCORD_WEBHOOK_URL` — see README "Design decisions §7"). Optional: when
   * unset, alerts still happen but only reach `console.error` (Workers
   * logs), never Discord — see `lib/alert.ts` `notifyDiscord`.
   */
  DISCORD_WEBHOOK_URL?: string;
}
