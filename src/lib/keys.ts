/**
 * R2 key layout helpers.
 *
 * Bronze layout (written upstream by each Worker's metrics writer):
 *   api-metrics/<dt>/<service>/<hour>/<timestamp>-<random>.jsonl
 *
 * This module owns every other key shape this app reads or writes.
 */

const BRONZE_PREFIX = 'api-metrics';

export function dayPrefix(dt: string): string {
  return `${BRONZE_PREFIX}/${dt}/`;
}

export function quarantineKeyFor(dt: string, chunkKey: string): string {
  return `quarantine/api-metrics/dt=${dt}/${chunkKey}.jsonl`;
}

export function manifestKey(dt: string): string {
  return `_state/medallion/dt=${dt}.json`;
}

/**
 * Per-(workflow instance, chunk) send-progress marker key. Distinct from
 * `manifestKey` — that's a single object `dt=<dt>.json`; this nests one
 * level deeper per `runId` + chunk (`dt=<dt>/<runId>/<chunkKey>.json`), so
 * the two never collide as R2 object keys despite sharing the
 * `_state/medallion/` namespace.
 *
 * Keyed by `runId` (the Workflow instance id, e.g. `day-<dt>` or
 * `day-<dt>-r<N>` — see `instanceIdFor`) so that:
 *  - a RETRY of the same instance (Workflows re-running a failed step)
 *    reads back the same marker and resumes sending from where the last
 *    successful batch left off;
 *  - a deliberate REDO (`?attempt=N`, a different instance id, or a fresh
 *    `POST /run` for a day that was never attempted) starts with no marker
 *    at all — it never sees a stale chunk's progress from a previous
 *    instance.
 * See `lib/chunk-marker.ts` and README "Design decisions §6".
 */
export function chunkMarkerKey(dt: string, runId: string, chunkKey: string): string {
  return `_state/medallion/dt=${dt}/${runId}/${chunkKey}.json`;
}

/** UTC calendar date, `days` away from `base` (negative = past), as YYYY-MM-DD. */
export function isoDateOffsetDays(base: Date, days: number): string {
  const d = new Date(Date.UTC(base.getUTCFullYear(), base.getUTCMonth(), base.getUTCDate()));
  d.setUTCDate(d.getUTCDate() + days);
  return d.toISOString().slice(0, 10);
}

/** Yesterday, UTC calendar date, as YYYY-MM-DD. Used by the cron handler. */
export function yesterdayUTC(now: Date = new Date()): string {
  return isoDateOffsetDays(now, -1);
}

const DT_RE = /^\d{4}-\d{2}-\d{2}$/;

/**
 * True if `dt` (a raw `?dt=` query-string value, or null when absent) is a
 * real UTC calendar date in YYYY-MM-DD form. The shape alone isn't enough:
 * `2026-02-30` or `2026-13-45` would otherwise create a real Workflow
 * instance (`day-2026-02-30`) that processes an empty prefix and writes a
 * manifest for a day that doesn't exist. Round-tripping through `Date`
 * rejects those, since JS rolls an out-of-range day into the next month.
 */
export function isValidDt(dt: string | null): dt is string {
  if (dt === null || !DT_RE.test(dt)) return false;
  const d = new Date(`${dt}T00:00:00Z`);
  return !Number.isNaN(d.getTime()) && d.toISOString().slice(0, 10) === dt;
}

const ATTEMPT_RE = /^[1-9]\d*$/;

/**
 * Workflow instance id for a day, optionally suffixed with a redo attempt
 * number: `day-<dt>` (no attempt — what cron always uses, and a day's first
 * `POST /run`) or `day-<dt>-r<attempt>` (a manual redo after deleting that
 * day's silver/gold rows and its `_state` manifest — see README "Operations"). `create()` is not idempotent, so re-running the SAME day
 * needs a fresh id each time; the `_state` manifest (checked by the
 * workflow's own `guard` step) remains the actual dedupe / completion check,
 * independent of which id ran it.
 */
export function instanceIdFor(dt: string, attempt?: string): string {
  return attempt ? `day-${dt}-r${attempt}` : `day-${dt}`;
}

/** True if `attempt` (a raw query-string value, or null when absent) is a valid attempt number. */
export function isValidAttempt(attempt: string | null): boolean {
  return attempt === null || ATTEMPT_RE.test(attempt);
}

const FORCE_TRUE_VALUES = new Set(['1', 'true']);

/**
 * True if `force` (a raw `?force=` query-string value, or null when absent)
 * is a value `POST /run` accepts — absent, `1`, or `true`. Anything else
 * (`0`, `false`, `yes`, ...) is rejected as a 400 rather than silently
 * treated as false, so a typo doesn't accidentally skip a redo the caller
 * asked for.
 */
export function isValidForce(force: string | null): boolean {
  return force === null || FORCE_TRUE_VALUES.has(force);
}

/** Parses an already-`isValidForce`-checked `?force=` value into a boolean. */
export function parseForce(force: string | null): boolean {
  return force !== null && FORCE_TRUE_VALUES.has(force);
}

/**
 * True if a Workflow instance id (`event.instanceId`) is a usable,
 * non-empty string. `WorkflowEvent<T>.instanceId` is typed as `string` in
 * `@cloudflare/workers-types`, but that's a compile-time guarantee only —
 * Workflows is a young product and this value has never been independently
 * verified at runtime. It matters here because chunk/gold send-progress
 * markers (`chunkMarkerKey`) are keyed by it: a falsy or shared runId would
 * make every attempt collide on the same marker path, so a deliberate redo
 * could silently short-circuit on a PRIOR attempt's `complete` markers and
 * send nothing — see `workflow.ts`'s `run()` for where this is enforced.
 */
export function isValidRunId(runId: unknown): runId is string {
  return typeof runId === 'string' && runId.length > 0;
}
