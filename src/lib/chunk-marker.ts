/**
 * Per-chunk send-progress marker — makes a `chunk-<n>` Workflow step's
 * `SILVER_STREAM.send()` calls resumable across a step retry, instead of a
 * retry blindly redoing the whole step (which resends every row).
 *
 * Root cause this bounds (measured in production, 2026-09-23, R2 SQL count
 * vs manifest sums for a 240-day backfill): a `chunk-<n>` step's
 * `SILVER_STREAM.send()` succeeded, then the step failed with
 * `WorkflowInternalError: Attempt failed due to internal workflows error`
 * BEFORE Workflows durably recorded the step as complete. The automatic
 * retry re-ran the entire step and re-sent the same rows. Cloudflare
 * Pipelines is append-only with no dedup, so the duplicate rows stayed in
 * silver: 12 of 240 backfilled days (~5%) got exactly one chunk duplicated,
 * 27,282 extra rows total. See README "Design decisions §6" for the residual
 * window this does NOT close.
 */

export interface ChunkMarker<TResult> {
  /** Number of batches already sent successfully for this chunk (0 = none yet). */
  sentBatches: number;
  /** True once every batch has been sent — `result` is then the chunk's final, authoritative result. */
  complete: boolean;
  /** Present iff `complete` — the value to return without redoing any work (including without re-sending). */
  result?: TResult;
}

/**
 * The subset of R2Bucket this module needs — narrowed (same pattern as
 * `lib/r2-list.ts`'s `R2Listable`) so the marker read/write logic is
 * unit-testable with a plain in-memory fake, without miniflare.
 */
export interface R2MarkerStore {
  get(key: string): Promise<{ text(): Promise<string> } | null>;
  put(
    key: string,
    value: string,
    options?: { httpMetadata?: { contentType?: string } },
  ): Promise<unknown>;
}

/** No marker has ever been written for this chunk+instance — nothing sent yet. */
function emptyMarker<TResult>(): ChunkMarker<TResult> {
  return { sentBatches: 0, complete: false };
}

/** Reads a chunk's marker, or the "nothing sent yet" marker if none has been written. */
export async function readChunkMarker<TResult>(
  bucket: R2MarkerStore,
  key: string,
): Promise<ChunkMarker<TResult>> {
  const obj = await bucket.get(key);
  if (!obj) return emptyMarker<TResult>();
  return JSON.parse(await obj.text()) as ChunkMarker<TResult>;
}

/** Writes (overwrites) a chunk's marker. Idempotent by construction — the same marker written twice is harmless. */
export async function writeChunkMarker<TResult>(
  bucket: R2MarkerStore,
  key: string,
  marker: ChunkMarker<TResult>,
): Promise<void> {
  await bucket.put(key, JSON.stringify(marker), {
    httpMetadata: { contentType: 'application/json' },
  });
}
