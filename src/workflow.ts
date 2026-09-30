import { WorkflowEntrypoint, type WorkflowEvent, type WorkflowStep } from 'cloudflare:workers';
import type { Env, GoldRow, MedallionDayParams, SilverRow } from './types';
import { chunkMarkerKey, dayPrefix, isValidRunId, manifestKey, quarantineKeyFor } from './lib/keys';
import {
  computeChunkDescriptors,
  listChunkKeys,
  listServicePrefixes,
  type ChunkDescriptor,
} from './lib/r2-list';
import {
  readChunkMarker,
  writeChunkMarker,
  type ChunkMarker,
  type R2MarkerStore,
} from './lib/chunk-marker';
import { parseBronzeLine, splitJsonLines } from './lib/parse';
import {
  addRowToGroup,
  groupKey,
  mergeGroups,
  goldEndpoint,
  newGroup,
  toGoldRow,
  type GroupAgg,
} from './lib/aggregate';
import {
  applyVerifyCounts,
  applyVerifyError,
  applyVerifySkipped,
  buildManifest,
  type Manifest,
} from './lib/manifest';
import { formatAlert, manifestProblems, notifyDiscord } from './lib/alert';
import { mapWithConcurrency } from './lib/concurrency';
import { queryRowCounts, R2_SQL_BUCKET, type R2SqlQueryOptions } from './lib/r2sql';

// Pipelines `send()` takes an array of JSON-serializable records. The task
// brief for this build specifies a 5MB payload ceiling per send() call; we
// could not independently confirm an exact number in the fetched docs (see
// README "Documented vs. not verified"), so we batch conservatively by row count.
// At ~300 bytes/row (silver) this cap stays far under 5MB even in the
// worst case; it's a size *ceiling* choice, not a tuned throughput number.
const STREAM_BATCH_ROWS = 2000;
// R2 get() concurrency within a single chunk step. Bounded so a chunk of
// ~2000 objects doesn't fire 2000 simultaneous fetches, while still being
// fast enough to fit a step's timeout comfortably.
const GET_CONCURRENCY = 20;

/**
 * Sends `rows` to `stream` in STREAM_BATCH_ROWS batches, starting at batch
 * index `startBatch` (from a marker read by the caller — 0 means nothing
 * sent yet for this instance+step), writing an updated marker to
 * `markerKey` after EACH successful batch send. The batch that empties
 * `rows` writes the marker with `complete: true` and the full `result`, so
 * a later read of the same marker (this instance's retry, or the caller's
 * own already-complete short-circuit) can skip re-sending — and, for a
 * caller that checks the marker before doing any upstream work (as both
 * `runChunk` and the `gold` step below do), skip that upstream work too.
 *
 * This shrinks a step's duplicate-send window from "the whole step, end to
 * end" down to "between one send() returning and the marker put that
 * immediately follows it" — see `lib/chunk-marker.ts` and README "Design decisions §6" for why that narrower window is a bound, not a full
 * elimination.
 *
 * Shared by the `chunk-<n>` steps' SILVER_STREAM sends and the `gold`
 * step's GOLD_STREAM send: the incident this guards against
 * (`WorkflowInternalError` after a step's `send()` succeeded but before
 * Workflows durably recorded the step complete — see README) is a generic
 * Workflows failure mode tied to *any* step whose last action is a
 * non-idempotent `send()`, not something specific to chunk steps. A
 * duplicated gold row is arguably worse than a duplicated silver row, since
 * gold is what a dashboard reads directly.
 *
 * Relies on `rows` being reproduced identically (same content, same order)
 * on every attempt of the calling step — true for chunk steps because
 * `listChunkKeys` re-lists the SAME already-closed UTC day's bronze objects
 * deterministically (see r2-list.ts), and true for the gold step because it
 * only re-merges the (already-computed, already-memoized-by-Workflows)
 * `chunkResults` — so the batch boundaries a marker was written against
 * always line up with a later attempt's batches.
 */
async function sendRowsResumable<T, R>(
  stream: { send(records: T[]): Promise<void> },
  bucket: R2MarkerStore,
  markerKey: string,
  rows: T[],
  startBatch: number,
  result: R,
): Promise<void> {
  if (rows.length === 0) {
    // Nothing to send, but completion still needs to be durably recorded so
    // a retry short-circuits via the marker instead of redoing the
    // (harmless but wasteful) upstream work above.
    if (startBatch === 0) {
      await writeChunkMarker<R>(bucket, markerKey, { sentBatches: 0, complete: true, result });
    }
    return;
  }
  const batchCount = Math.ceil(rows.length / STREAM_BATCH_ROWS);
  for (let b = startBatch; b < batchCount; b++) {
    const batch = rows.slice(b * STREAM_BATCH_ROWS, (b + 1) * STREAM_BATCH_ROWS);
    await stream.send(batch);
    const sentBatches = b + 1;
    const marker: ChunkMarker<R> =
      sentBatches === batchCount
        ? { sentBatches, complete: true, result }
        : { sentBatches, complete: false };
    await writeChunkMarker(bucket, markerKey, marker);
  }
}

/** The `gold` step's send-progress marker result — see `sendRowsResumable`. */
export interface GoldStepResult {
  goldRows: number;
  goldRequestsSum: number;
}

export interface ChunkStepResult {
  chunkKey: string;
  service: string;
  /** Objects this chunk's descriptor listed (== descriptor.expectedCount, enforced by listChunkKeys). */
  objectCount: number;
  /** Of `objectCount`, how many `get()` returned `null` for (object vanished between list and get). */
  objectsMissing: number;
  linesParsed: number;
  validRows: number;
  quarantinedRows: number;
  quarantineKey?: string;
  groups: Record<string, GroupAgg>;
}

/**
 * One (service, dt) chunk of ≤2000 bronze objects: check its send-progress
 * marker (short-circuit if already complete) -> list -> get -> parse ->
 * validate -> (quarantine invalid lines) -> aggregate -> send valid rows to
 * the silver stream, resuming from the marker's last successful batch.
 * Extracted as a free function (not a class method) so it has no `this`
 * binding and nothing but its arguments and locals are live across the
 * `await`s — matters less for correctness than for keeping this the obvious
 * place to look when tuning chunk-level behavior. Exported for direct unit
 * testing of the resume logic (see `__tests__/workflow.test.ts`) with a fake
 * bucket + fake stream, without needing a real Workflow instance/miniflare.
 */
export async function runChunk(
  env: Env,
  dt: string,
  chunkKey: string,
  descriptor: ChunkDescriptor,
  runId: string,
): Promise<ChunkStepResult> {
  const markerKey = chunkMarkerKey(dt, runId, chunkKey);
  const marker = await readChunkMarker<ChunkStepResult>(env.DATALAKE_R2, markerKey);
  if (marker.complete && marker.result) {
    // Every batch for this chunk was already sent by an earlier attempt of
    // THIS SAME workflow instance (marker is keyed by runId — see
    // chunkMarkerKey). Return the stored result verbatim: no R2 list/get,
    // no re-parsing, and — the point of this marker — no re-sending.
    return marker.result;
  }

  const keys = await listChunkKeys(env.DATALAKE_R2, descriptor);

  // `R2Bucket.get()` returns `null` for a key that doesn't exist. Silently
  // treating that as an empty body (no lines) would make a vanished bronze
  // object indistinguishable from an object that legitimately had zero
  // lines — i.e. bronze data loss that nothing downstream would ever
  // notice, since it wouldn't violate the lines == silver + quarantine
  // arithmetic either. Track it explicitly instead so the manifest's
  // `objectsListedEqualsRead` assertion can catch it.
  const fetched = await mapWithConcurrency(
    keys,
    GET_CONCURRENCY,
    async (key): Promise<{ text: string; missing: boolean }> => {
      const obj = await env.DATALAKE_R2.get(key);
      return obj ? { text: await obj.text(), missing: false } : { text: '', missing: true };
    },
  );

  const groups = new Map<string, GroupAgg>();
  const silverRows: SilverRow[] = [];
  const quarantineLines: string[] = [];
  let linesParsed = 0;
  let objectsMissing = 0;

  for (let i = 0; i < keys.length; i++) {
    const key = keys[i];
    if (fetched[i].missing) {
      objectsMissing += 1;
      continue;
    }
    // lineIdx resets to 0 for EACH object (it's the index within THIS
    // object's splitJsonLines output) — see `silverRowUid`'s doc comment.
    // `linesParsed` below is a different, deliberately non-resetting
    // counter (this chunk's running total across every object) and must
    // NOT be used as the row_uid line index.
    const lines = splitJsonLines(fetched[i].text);
    for (let lineIdx = 0; lineIdx < lines.length; lineIdx++) {
      linesParsed += 1;
      const result = parseBronzeLine(lines[lineIdx], dt, descriptor.service, key, lineIdx);
      if (!result.ok) {
        quarantineLines.push(
          JSON.stringify({ reason: result.reason, source_key: key, raw: result.raw }),
        );
        continue;
      }
      silverRows.push(result.row);
      // Gold groups 404s under one label; silver keeps the raw path (see goldEndpoint).
      const endpoint = goldEndpoint(result.row.endpoint, result.row.status);
      const key2 = groupKey(result.row.service, endpoint, result.row.method);
      let group = groups.get(key2);
      if (!group) {
        group = newGroup(result.row.service, endpoint, result.row.method);
        groups.set(key2, group);
      }
      addRowToGroup(group, result.row);
    }
  }

  // Ordering matters (design rule: `send()` is the last action of each step).
  // The quarantine write is an idempotent R2 `put` at a fixed key — a retry
  // overwrites it with identical content, so redoing it on step retry is
  // harmless. The stream `send()` calls inside sendRowsResumable are NOT
  // idempotent (Pipelines has no dedup), so they must be the true last
  // action, and everything that can safely be redone happens before them.
  //
  // Residual risk (documented, bounded but not eliminated — see
  // sendRowsResumable and README "Design decisions §6"): if a `send()` call
  // itself succeeds but the marker put that immediately follows it fails or
  // hasn't landed before this step is retried, the retry re-sends that one
  // batch and the silver table gets duplicates for it. The manifest's
  // assertions in the `manifest` step do NOT catch this, because they
  // reconcile this workflow run's own bookkeeping (rows it attempted to
  // send), not a query-back against the silver table — decision #1 in
  // By design, gold never reads silver back within the same run.
  let quarantineKey: string | undefined;
  if (quarantineLines.length > 0) {
    quarantineKey = quarantineKeyFor(dt, chunkKey);
    await env.DATALAKE_R2.put(quarantineKey, quarantineLines.join('\n'), {
      httpMetadata: { contentType: 'application/x-ndjson' },
    });
  }

  const result: ChunkStepResult = {
    chunkKey,
    service: descriptor.service,
    objectCount: keys.length,
    objectsMissing,
    linesParsed,
    validRows: silverRows.length,
    quarantinedRows: quarantineLines.length,
    quarantineKey,
    groups: Object.fromEntries(groups),
  };

  await sendRowsResumable(
    env.SILVER_STREAM,
    env.DATALAKE_R2,
    markerKey,
    silverRows,
    marker.sentBatches,
    result,
  );

  return result;
}

/**
 * The `gold` step's work: merge every chunk's partial
 * (service,endpoint,method) aggregates into per-group totals, compute exact
 * percentiles, and send the resulting gold rows — resuming from a
 * send-progress marker exactly like `runChunk` does for silver (see
 * `sendRowsResumable`'s doc comment for why the gold step needs this too).
 * Extracted as a free function for the same reason `runChunk` is (no `this`
 * binding, obvious place to look), and exported for the same kind of direct
 * unit testing — see `__tests__/workflow.test.ts`.
 */
export async function runGold(
  env: Env,
  dt: string,
  chunkResults: ChunkStepResult[],
  runId: string,
): Promise<GoldStepResult> {
  const markerKey = chunkMarkerKey(dt, runId, 'gold');
  const marker = await readChunkMarker<GoldStepResult>(env.DATALAKE_R2, markerKey);
  if (marker.complete && marker.result) {
    return marker.result;
  }

  const merged = new Map<string, GroupAgg>();
  for (const chunk of chunkResults) {
    for (const [key, group] of Object.entries(chunk.groups)) {
      const prev = merged.get(key);
      merged.set(key, prev ? mergeGroups(prev, group) : group);
    }
  }
  const goldRows: GoldRow[] = Array.from(merged.values()).map((g) => toGoldRow(dt, g));
  const result: GoldStepResult = {
    goldRows: goldRows.length,
    goldRequestsSum: goldRows.reduce((sum, r) => sum + r.requests, 0),
  };
  await sendRowsResumable(
    env.GOLD_STREAM,
    env.DATALAKE_R2,
    markerKey,
    goldRows,
    marker.sentBatches,
    result,
  );
  return result;
}

/**
 * The `verify` step's work: query R2 SQL for this `dt`'s actual row counts
 * in silver and gold, decide whether the day has duplicates and/or a real
 * mismatch (see `applyVerifyCounts`), persist the updated manifest, and
 * return it. Runs strictly after the `manifest` step already wrote a
 * `status: 'done'` manifest and after `step.sleep('verify-sleep', ...)` has
 * given the Pipelines sink time to commit (see `run()` below) — this
 * function itself does no sleeping.
 *
 * Deliberately never throws: a network/API failure, or the secret being
 * unset, is recorded ON the manifest (`verify: { error }` / `{ skipped }`)
 * rather than failing the day — see README "Design decisions §6". Exported (like
 * `runChunk`/`runGold`) for direct unit testing with a fake bucket and fake
 * fetch, without needing a real Workflow instance.
 */
export async function runVerify(
  env: Env,
  dt: string,
  bucket: R2MarkerStore,
  manifest: Manifest,
  fetchImpl?: typeof fetch,
): Promise<Manifest> {
  const token = env.R2_SQL_TOKEN;
  let updated: Manifest;
  if (!token) {
    updated = applyVerifySkipped(manifest, 'no R2_SQL_TOKEN');
  } else {
    const options: R2SqlQueryOptions = {
      accountId: env.R2_SQL_ACCOUNT_ID,
      bucket: R2_SQL_BUCKET,
      token,
      fetchImpl,
    };
    try {
      const [silver, gold] = await Promise.all([
        queryRowCounts(options, 'silver.api_metrics', dt),
        queryRowCounts(options, 'gold.api_metrics_daily', dt),
      ]);
      updated = applyVerifyCounts(manifest, {
        silverRows: silver.total,
        silverDistinct: silver.distinct,
        goldRows: gold.total,
        goldDistinct: gold.distinct,
        checkedAt: new Date().toISOString(),
      });
    } catch (err) {
      updated = applyVerifyError(manifest, String(err));
    }
  }
  await bucket.put(manifestKey(dt), JSON.stringify(updated, null, 2), {
    httpMetadata: { contentType: 'application/json' },
  });
  return updated;
}

export class MedallionDayWorkflow extends WorkflowEntrypoint<Env, MedallionDayParams> {
  async run(event: WorkflowEvent<MedallionDayParams>, step: WorkflowStep) {
    const { dt, force } = event.payload;
    const startedAt = new Date().toISOString();
    const bucket = this.env.DATALAKE_R2;
    // The Workflows runtime exposes the instance id directly on the event
    // (confirmed against @cloudflare/workers-types' WorkflowEvent<T> and
    // https://developers.cloudflare.com/workflows/build/workers-api/ — no
    // need to thread it through `params` separately). Used to key chunk
    // send-progress markers so a RETRY of this instance resumes, while a
    // REDO (a different instance id via `?attempt=N`) starts clean.
    //
    // The type says `instanceId: string`, but that's unverified at runtime
    // (Workflows is a young product) and the failure mode if it's ever
    // falsy/empty is severe: every marker key would collapse to the same
    // literal `.../undefined/...` (or `.../.../...`) path, so a deliberate
    // redo (`?attempt=N`) would find a PRIOR attempt's `complete` markers
    // and silently short-circuit every chunk send — sending nothing at all
    // while still writing a manifest that claims success. That is worse
    // than the duplicate-row bug this marker scheme fixes, so fail loudly
    // here rather than let it happen quietly downstream.
    const runId = event.instanceId;
    if (!isValidRunId(runId)) {
      throw new Error(
        `MedallionDayWorkflow: event.instanceId must be a non-empty string, got ${JSON.stringify(runId)}. ` +
          `Refusing to run — chunk/gold send-progress markers are keyed by this id (see lib/chunk-marker.ts), ` +
          `and a falsy/shared id would make markers collide across attempts.`,
      );
    }

    // ---- guard ------------------------------------------------------
    const guard = await step.do('guard', async () => {
      if (force) {
        // `POST /run?force=1`: skip the "already done" exit so a rebuild
        // (rows deleted from silver/gold, but the `_state` manifest left in
        // place) doesn't require deleting that manifest first. This does
        // NOT bypass `create()`'s own non-idempotency — re-entering with the
        // SAME instance id still isn't possible; a real redo still needs a
        // fresh `?attempt=N`, which is what makes runId (and therefore the
        // chunk markers) fresh too. See README "Operations".
        return { alreadyDone: false as const, manifest: undefined };
      }
      const existing = await bucket.get(manifestKey(dt));
      if (!existing) return { alreadyDone: false as const, manifest: undefined };
      const manifest = JSON.parse(await existing.text()) as Manifest;
      return { alreadyDone: manifest.status === 'done', manifest };
    });
    if (guard.alreadyDone) {
      return { skipped: true, dt, manifest: guard.manifest };
    }

    // ---- list ---------------------------------------------------------
    // Returns only compact chunk *descriptors*, never the (up to ~35k)
    // bronze keys themselves — see lib/r2-list.ts doc comment for why.
    const { descriptors, services } = await step.do('list', async () => {
      const prefix = dayPrefix(dt);
      const servicePrefixes = await listServicePrefixes(bucket, prefix);
      const allDescriptors: ChunkDescriptor[] = [];
      const serviceNames: string[] = [];
      for (const svcPrefix of servicePrefixes) {
        const service = svcPrefix.slice(prefix.length, -1); // strip day prefix + trailing '/'
        serviceNames.push(service);
        const { descriptors: svcDescriptors } = await computeChunkDescriptors(
          bucket,
          service,
          svcPrefix,
        );
        allDescriptors.push(...svcDescriptors);
      }
      return { descriptors: allDescriptors, services: serviceNames };
    });

    const bronzeObjectCount = descriptors.reduce((sum, d) => sum + d.expectedCount, 0);

    // ---- chunk-<n> ------------------------------------------------------
    const chunkResults: ChunkStepResult[] = [];
    for (let i = 0; i < descriptors.length; i++) {
      const descriptor = descriptors[i];
      const chunkKey = `chunk-${i}`;
      const result = await step.do(
        chunkKey,
        {
          retries: { limit: 3, delay: '30 seconds', backoff: 'exponential' },
          timeout: '15 minutes',
        },
        () => runChunk(this.env, dt, chunkKey, descriptor, runId),
      );
      chunkResults.push(result);
    }

    // ---- gold -----------------------------------------------------------
    // Built from THIS run's typed rows only (decision #1: never reads silver
    // back — R2 SQL is read-only anyway). Cross-chunk merge is needed
    // because a service spanning >2000 objects has multiple chunks
    // contributing to the same (service, endpoint, method) group. Same
    // send-progress marker treatment as `runChunk` — see `runGold` and
    // `sendRowsResumable`'s doc comment for why this step needs it too, not
    // just the chunk steps.
    const goldSummary = await step.do('gold', () => runGold(this.env, dt, chunkResults, runId));

    // ---- manifest ---------------------------------------------------
    const manifest = await step.do('manifest', async () => {
      const bronzeObjectsMissing = chunkResults.reduce((sum, c) => sum + c.objectsMissing, 0);
      const bronzeObjectsRead = chunkResults.reduce(
        (sum, c) => sum + (c.objectCount - c.objectsMissing),
        0,
      );
      const bronzeLinesParsed = chunkResults.reduce((sum, c) => sum + c.linesParsed, 0);
      const silverRows = chunkResults.reduce((sum, c) => sum + c.validRows, 0);
      const quarantinedRows = chunkResults.reduce((sum, c) => sum + c.quarantinedRows, 0);
      const built = buildManifest({
        dt,
        startedAt,
        bronzeObjectCount,
        bronzeObjectsRead,
        bronzeObjectsMissing,
        bronzeLinesParsed,
        silverRows,
        quarantinedRows,
        goldRows: goldSummary.goldRows,
        goldRequestsSum: goldSummary.goldRequestsSum,
        services,
      });
      await bucket.put(manifestKey(dt), JSON.stringify(built, null, 2), {
        httpMetadata: { contentType: 'application/json' },
      });
      if (built.status === 'mismatch') {
        throw new Error(
          `medallion manifest mismatch for dt=${dt}: ${JSON.stringify(built.assertions)}`,
        );
      }
      return built;
    });

    // ---- verify ---------------------------------------------------------
    // Only reached when `manifest` didn't throw (status 'done'). Read-only
    // and best-effort — see `runVerify` and README "Design decisions §6".
    let verified: Manifest;
    if (!this.env.R2_SQL_TOKEN) {
      // No point sleeping 6 minutes for a query we already know we'll skip.
      verified = await step.do('verify', () => runVerify(this.env, dt, bucket, manifest));
    } else {
      // Cloudflare Pipelines sinks roll on a 300-second (5 minute) interval
      // before buffered rows actually land in the R2 Data Catalog table (see
      // README "Documented vs. not verified" / the sink's roll-interval config). Querying R2
      // SQL immediately after `manifest` would systematically under-count
      // THIS run's own just-sent rows and misreport a perfectly fine day as
      // 'mismatch'. Sleep past that interval with a minute of margin.
      await step.sleep('verify-sleep', '6 minutes');
      verified = await step.do('verify', () => runVerify(this.env, dt, bucket, manifest));
    }

    // ---- alert ------------------------------------------------------
    // Best-effort, like `verify` itself: `notifyDiscord` never throws, so
    // this step never retries into anything harmful. Shared by both
    // branches above (skip-sleep and slept) rather than duplicated inside
    // each, since the only thing that differs between them is how `verified`
    // was produced, not what happens once it exists.
    await step.do('alert', async () => {
      const problems = manifestProblems(verified);
      if (problems.length) {
        await notifyDiscord(this.env.DISCORD_WEBHOOK_URL, formatAlert(dt, 'workflow', problems));
      }
      return problems;
    });

    return { dt, manifest: verified };
  }
}
