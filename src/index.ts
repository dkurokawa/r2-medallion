/**
 * r2-medallion — Worker entry point.
 *
 * Endpoints (both require `Authorization: Bearer $ADMIN_TOKEN`; nothing else
 * is public — no /health, per the frozen design):
 *   POST /run?dt=YYYY-MM-DD[&attempt=N][&force=1]  → create (trigger) a Workflow instance for that day
 *   GET  /status?dt=YYYY-MM-DD                       → return that day's manifest from R2, if any
 *
 * `force=1` (or `force=true`) sets `params.force`, which makes the
 * workflow's `guard` step skip its "manifest already says done → exit"
 * check — see README "Operations" / "Design decisions §6". Combine
 * with a fresh `?attempt=N` for a real redo (a bare `force=1` retry of an
 * already-created instance id still just 409s, per `create()`'s
 * non-idempotency below).
 *
 * Scheduled handler: daily at 01:00 UTC (wrangler.toml), creates an instance
 * for *yesterday* (UTC) — always the bare `day-<dt>` id (no `attempt`,
 * no `force`).
 *
 * A second cron, 02:00 UTC (`HEALTH_CHECK_CRON`), catches the case the
 * workflow's own `alert` step (workflow.ts) cannot: a day whose Workflow
 * instance never produced a manifest at all (crashed, never invoked, stuck
 * mid-run). It re-reads the SAME `dt` the 01:00 run just processed and
 * alerts if that manifest is missing or unhealthy — see README "Design decisions §7".
 */
import type { Env } from './types';
import { verifyBearerToken } from './lib/auth';
import { formatAlert, manifestProblems, notifyDiscord } from './lib/alert';
import {
  instanceIdFor,
  isValidAttempt,
  isValidForce,
  manifestKey,
  parseForce,
  yesterdayUTC,
} from './lib/keys';
import type { Manifest } from './lib/manifest';

/** The 02:00 UTC health-check cron — see module doc comment above. */
export const HEALTH_CHECK_CRON = '0 2 * * *';

export { MedallionDayWorkflow } from './workflow';

const DATE_RE = /^\d{4}-\d{2}-\d{2}$/;

function json(body: unknown, status = 200): Response {
  return new Response(JSON.stringify(body), {
    status,
    headers: { 'content-type': 'application/json' },
  });
}

async function handleRun(request: Request, env: Env): Promise<Response> {
  if (!(await verifyBearerToken(request, env.ADMIN_TOKEN))) {
    return json({ ok: false, error: 'unauthorized' }, 401);
  }
  const url = new URL(request.url);
  const dt = url.searchParams.get('dt');
  if (!dt || !DATE_RE.test(dt)) {
    return json({ ok: false, error: 'query ?dt=YYYY-MM-DD required' }, 400);
  }
  const attempt = url.searchParams.get('attempt');
  if (!isValidAttempt(attempt)) {
    return json(
      { ok: false, error: 'query ?attempt= must be a positive integer (1, 2, 3, ...)' },
      400,
    );
  }
  const forceRaw = url.searchParams.get('force');
  if (!isValidForce(forceRaw)) {
    return json({ ok: false, error: 'query ?force= must be 1 or true if present' }, 400);
  }
  const force = parseForce(forceRaw);

  try {
    // `create()` is NOT idempotent — it throws if an instance with this id
    // already exists (verified against current Cloudflare docs; see
    // README). Without `attempt`, a duplicate-id failure is exactly the
    // "already running / already done for this day" case, which is a safe,
    // expected outcome of re-POSTing the same dt (e.g. re-running
    // backfill.sh over a range that partially succeeded already) — reported
    // as 409, not a 5xx. To genuinely redo a day (after deleting its rows
    // from silver/gold — see README), pass a fresh `?attempt=N` each time,
    // since the dt alone can no longer produce a fresh instance id. The
    // `_state` manifest guard (checked by the workflow's own `guard` step)
    // is what actually prevents redoing an already-`done` day by mistake,
    // independent of the instance id used to run it — add `&force=1` to
    // skip that guard instead of also deleting the `_state` manifest first
    // (useful for a full-range rebuild across many days at once).
    const instance = await env.MEDALLION_WORKFLOW.create({
      id: instanceIdFor(dt, attempt ?? undefined),
      params: { dt, force },
    });
    return json({ ok: true, dt, instanceId: instance.id, force, status: 'created' }, 202);
  } catch (err) {
    return json(
      { ok: false, dt, error: 'create_failed_or_already_exists', detail: String(err) },
      409,
    );
  }
}

async function handleStatus(request: Request, env: Env): Promise<Response> {
  if (!(await verifyBearerToken(request, env.ADMIN_TOKEN))) {
    return json({ ok: false, error: 'unauthorized' }, 401);
  }
  const url = new URL(request.url);
  const dt = url.searchParams.get('dt');
  if (!dt || !DATE_RE.test(dt)) {
    return json({ ok: false, error: 'query ?dt=YYYY-MM-DD required' }, 400);
  }
  const obj = await env.DATALAKE_R2.get(manifestKey(dt));
  if (!obj) {
    return json({ ok: false, dt, error: 'no manifest for this dt yet' }, 404);
  }
  const manifest = (await obj.json()) as Manifest;
  return json({ ok: true, dt, manifest });
}

/**
 * The 02:00 UTC health check: re-reads the manifest for the SAME `dt` the
 * 01:00 run just processed (`yesterdayUTC` of this cron's own
 * `scheduledTime`, which falls on the same UTC calendar day as the 01:00
 * run an hour earlier) and alerts if it's missing or unhealthy.
 *
 * Deliberately re-alerts even when the workflow's own `alert` step
 * (workflow.ts) already posted for this same day — see the design note in
 * README "Design decisions §7". Never throws — errors are caught and only
 * logged, since a health check that itself crashes must not take the
 * scheduled handler down with it.
 */
async function runHealthCheck(env: Env, event: ScheduledEvent): Promise<void> {
  try {
    const dt = yesterdayUTC(new Date(event.scheduledTime));
    const obj = await env.DATALAKE_R2.get(manifestKey(dt));
    const manifest = obj ? ((await obj.json()) as Manifest) : null;
    const problems = manifestProblems(manifest);
    if (problems.length) {
      await notifyDiscord(env.DISCORD_WEBHOOK_URL, formatAlert(dt, 'health-check', problems));
    }
  } catch (err) {
    console.error(`[r2-medallion] health-check failed: ${String(err)}`);
  }
}

export default {
  async fetch(request: Request, env: Env): Promise<Response> {
    const url = new URL(request.url);
    if (request.method === 'POST' && url.pathname === '/run') {
      return handleRun(request, env);
    }
    if (request.method === 'GET' && url.pathname === '/status') {
      return handleStatus(request, env);
    }
    return json({ ok: false, error: 'not found' }, 404);
  },

  async scheduled(event: ScheduledEvent, env: Env, ctx: ExecutionContext): Promise<void> {
    if (event.cron === HEALTH_CHECK_CRON) {
      ctx.waitUntil(runHealthCheck(env, event));
      return;
    }

    const dt = yesterdayUTC(new Date(event.scheduledTime));
    ctx.waitUntil(
      (async () => {
        try {
          const instance = await env.MEDALLION_WORKFLOW.create({
            id: instanceIdFor(dt),
            params: { dt },
          });
          console.log(`[r2-medallion] created workflow instance ${instance.id} for dt=${dt}`);
        } catch (err) {
          // Expected on any cron re-run within the same UTC day (instance id
          // already exists) — treated as a no-op skip, per the design.
          console.log(`[r2-medallion] create(day-${dt}) skipped or failed: ${String(err)}`);
        }
      })(),
    );
  },
};
