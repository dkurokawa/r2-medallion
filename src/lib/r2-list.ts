/**
 * Chunked R2 listing without ever holding (or returning) the full day's key
 * list in memory / Workflow step state.
 *
 * Why: a day can have up to ~32,557 bronze objects (as of 2026-09). Cloudflare
 * Workflows durably persists every `step.do` return value as part of the
 * instance's history; returning tens of thousands of R2 keys from the `list`
 * step (repeated as the workflow accumulates more steps) risks the
 * per-step/instance state size limits documented for Workflows. We could not
 * find a documented exact byte ceiling to size against with confidence (see
 * README "Documented vs. not verified"), so — choosing
 * the robust option — we deliberately avoid the question: the `list` step
 * returns only compact chunk *descriptors* (service, prefix, a resume
 * cursor, and an expected count), and each `chunk-<n>` step re-lists its own
 * slice from that cursor. R2 `list()` calls are cheap Class A operations
 * (~35 of them for a 32k-object day), so the extra listing work is
 * negligible next to the ~32k `get()` calls that dominate either way.
 *
 * Chunk boundaries are cut ONLY at page boundaries, never mid-page: a chunk
 * is closed as soon as its accumulated (whole) pages total >= CHUNK_SIZE,
 * so a chunk can be up to one page larger than CHUNK_SIZE — that's fine, the
 * cap is a soft "roughly 2000" batching target, not a hard limit anything
 * depends on. This is deliberately robust against R2 returning fewer than
 * the requested `limit` on a still-truncated page (observed/assumed
 * possible in review; a fixed-size-page assumption previously here was
 * wrong and caused a real bug: closing a chunk mid-page and resuming the
 * next chunk from `page.cursor` — which points past the *whole* page, not
 * past the point actually reached — skipped or double-counted keys). Cutting
 * only at whole-page boundaries removes the need for any assumption about
 * page sizes at all: `chunkStartCursor` is only ever set to a cursor that
 * was itself returned to *begin* the next page, so resuming from it always
 * lines up exactly. Exercised by r2-list.test.ts with a fake bucket that
 * returns deliberately ragged page sizes.
 */

export const CHUNK_SIZE = 2000;
export const LIST_PAGE_SIZE = 1000;

export interface ChunkDescriptor {
  service: string;
  /** R2 prefix this descriptor's keys live under (that service's directory for `dt`). */
  prefix: string;
  /** Cursor to resume `list()` from for this chunk's first object; undefined = start of `prefix`. */
  startCursor?: string;
  expectedCount: number;
}

export interface R2ListPage {
  objects: { key: string }[];
  truncated: boolean;
  cursor?: string;
  delimitedPrefixes?: string[];
}

/** The subset of R2Bucket.list() this module needs — narrowed so pure logic can be unit-tested with a fake, without miniflare. */
export interface R2Listable {
  list(options: {
    prefix?: string;
    delimiter?: string;
    cursor?: string;
    limit?: number;
  }): Promise<R2ListPage>;
}

/** List the immediate "directories" (service names) under `dayPrefixValue`, via delimiter listing. */
export async function listServicePrefixes(
  bucket: R2Listable,
  dayPrefixValue: string,
): Promise<string[]> {
  const prefixes: string[] = [];
  let cursor: string | undefined;
  do {
    const page = await bucket.list({
      prefix: dayPrefixValue,
      delimiter: '/',
      cursor,
      limit: LIST_PAGE_SIZE,
    });
    prefixes.push(...(page.delimitedPrefixes ?? []));
    cursor = page.truncated ? page.cursor : undefined;
  } while (cursor);
  return prefixes;
}

/**
 * Walk every object under `prefix`, recording a resumable chunk boundary
 * once accumulated whole pages reach CHUNK_SIZE. Returns descriptors only —
 * never the keys themselves (see module doc comment above).
 */
export async function computeChunkDescriptors(
  bucket: R2Listable,
  service: string,
  prefix: string,
): Promise<{ descriptors: ChunkDescriptor[]; totalCount: number }> {
  const descriptors: ChunkDescriptor[] = [];
  // Cursor to fetch the NEXT page (undefined = start of `prefix`).
  let cursor: string | undefined;
  // Cursor that begins the CURRENT (still-open) chunk — always either
  // undefined (chunk starts at the top of `prefix`) or a value that was
  // itself handed out by R2 as the cursor to *begin* some page.
  let chunkStartCursor: string | undefined;
  let countInChunk = 0;
  let totalCount = 0;
  let truncated = true;

  while (truncated) {
    const page = await bucket.list({ prefix, cursor, limit: LIST_PAGE_SIZE });
    countInChunk += page.objects.length;
    totalCount += page.objects.length;
    truncated = page.truncated;
    // Cursor to begin the page AFTER this one (or undefined if this was the
    // last page). Computed now, before any chunk-close logic below, so it's
    // available either as "where the next chunk should start" or as "where
    // the next page of the still-open chunk should start".
    cursor = page.truncated ? page.cursor : undefined;

    if (countInChunk >= CHUNK_SIZE) {
      descriptors.push({
        service,
        prefix,
        startCursor: chunkStartCursor,
        expectedCount: countInChunk,
      });
      countInChunk = 0;
      chunkStartCursor = cursor;
    }
  }
  if (countInChunk > 0) {
    descriptors.push({
      service,
      prefix,
      startCursor: chunkStartCursor,
      expectedCount: countInChunk,
    });
  }
  return { descriptors, totalCount };
}

/**
 * Re-list exactly the keys belonging to one chunk descriptor, resuming from
 * its cursor and requesting the SAME page size (`LIST_PAGE_SIZE`) that was
 * used to build the descriptor in the first place, consuming whole pages
 * (never truncating a page short to avoid "overshooting" expectedCount —
 * by construction, chunk boundaries always land on page boundaries, so a
 * matching re-list should never overshoot). Throws if the reconstructed key
 * count doesn't match `expectedCount` exactly: that means the bucket's
 * contents or pagination behavior changed between the `list` step and this
 * `chunk-<n>` step, which should never happen for an already-closed UTC day
 * and is worth failing loudly on rather than silently processing a
 * different set of objects than the one the day's totals were computed from.
 */
export async function listChunkKeys(
  bucket: R2Listable,
  descriptor: ChunkDescriptor,
): Promise<string[]> {
  const keys: string[] = [];
  let cursor = descriptor.startCursor;
  let truncated = true;
  while (truncated && keys.length < descriptor.expectedCount) {
    const page = await bucket.list({ prefix: descriptor.prefix, cursor, limit: LIST_PAGE_SIZE });
    for (const obj of page.objects) keys.push(obj.key);
    truncated = page.truncated;
    cursor = page.truncated ? page.cursor : undefined;
  }
  if (keys.length !== descriptor.expectedCount) {
    throw new Error(
      `listChunkKeys: expected ${descriptor.expectedCount} keys for service=${descriptor.service} ` +
        `prefix=${descriptor.prefix} startCursor=${descriptor.startCursor ?? '(start)'}, got ${keys.length}`,
    );
  }
  return keys;
}
