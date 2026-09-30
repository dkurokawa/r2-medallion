import { describe, expect, it, vi } from 'vitest';
import {
  runChunk,
  runGold,
  runVerify,
  type ChunkStepResult,
  type GoldStepResult,
} from '../workflow';
import { chunkMarkerKey, manifestKey } from '../lib/keys';
import { writeChunkMarker } from '../lib/chunk-marker';
import type { ChunkDescriptor } from '../lib/r2-list';
import type { GroupAgg } from '../lib/aggregate';
import type { Manifest } from '../lib/manifest';
import type { Env, GoldRow, SilverRow } from '../types';

/**
 * Minimal in-memory R2 fake covering exactly what `runChunk` exercises:
 * `list()` (via `listChunkKeys` — single page only, since this test never
 * has more than one page of bronze objects), `get()` (bronze object bodies
 * from a fixed map, OR a previously-`put()` value — chunk markers and
 * quarantine files are read back the same way real R2 would), and `put()`
 * (chunk marker + quarantine writes, tracked separately from bronze content
 * so a marker write can never accidentally shadow a bronze object).
 */
class FakeR2 {
  private readonly written = new Map<string, string>();

  constructor(private readonly bronze: Map<string, string>) {}

  async list(options: {
    prefix?: string;
    cursor?: string;
    limit?: number;
  }): Promise<{ objects: { key: string }[]; truncated: boolean; cursor?: string }> {
    const prefix = options.prefix ?? '';
    const keys = Array.from(this.bronze.keys())
      .filter((k) => k.startsWith(prefix))
      .sort();
    return { objects: keys.map((key) => ({ key })), truncated: false, cursor: undefined };
  }

  async get(key: string): Promise<{ text(): Promise<string> } | null> {
    if (this.written.has(key)) {
      const value = this.written.get(key)!;
      return { text: async () => value };
    }
    const body = this.bronze.get(key);
    return body === undefined ? null : { text: async () => body };
  }

  async put(key: string, value: string): Promise<null> {
    this.written.set(key, value);
    return null;
  }

  writtenAt(key: string): string | undefined {
    return this.written.get(key);
  }
}

const DT = '2026-09-21';
const SERVICE = 'svc';
const PREFIX = `api-metrics/${DT}/${SERVICE}/`;
const OBJECT_KEY = `${PREFIX}00/obj.jsonl`;
// STREAM_BATCH_ROWS in workflow.ts is 2000 — this must stay > that so a
// single chunk (this test always uses exactly one bronze object) spans two
// send() batches (2000 + 500), which is what the resume tests below exist
// to exercise. If STREAM_BATCH_ROWS ever changes, the batch-size
// assertions below need updating to match.
const ROW_COUNT = 2500;

function bronzeLine(i: number): string {
  return JSON.stringify({
    timestamp: new Date(Date.UTC(2026, 8, 21, 0, 0, i)).toISOString(),
    endpoint: '/x',
    method: 'GET',
    status: 200,
    latency_ms: 5,
  });
}

function makeBucket(): FakeR2 {
  const lines = Array.from({ length: ROW_COUNT }, (_, i) => bronzeLine(i)).join('\n');
  return new FakeR2(new Map([[OBJECT_KEY, lines]]));
}

function makeDescriptor(): ChunkDescriptor {
  return { service: SERVICE, prefix: PREFIX, startCursor: undefined, expectedCount: 1 };
}

function makeEnv(bucket: FakeR2, send: (rows: SilverRow[]) => Promise<void>): Env {
  return {
    DATALAKE_R2: bucket as unknown as Env['DATALAKE_R2'],
    MEDALLION_WORKFLOW: {} as Env['MEDALLION_WORKFLOW'],
    SILVER_STREAM: { send },
    GOLD_STREAM: { send: async () => {} },
    ADMIN_TOKEN: 'test',
    ENVIRONMENT: 'test',
    R2_SQL_ACCOUNT_ID: 'acct123',
  };
}

function makeGoldEnv(bucket: FakeR2, send: (rows: GoldRow[]) => Promise<void>): Env {
  return {
    DATALAKE_R2: bucket as unknown as Env['DATALAKE_R2'],
    MEDALLION_WORKFLOW: {} as Env['MEDALLION_WORKFLOW'],
    SILVER_STREAM: { send: async () => {} },
    GOLD_STREAM: { send },
    ADMIN_TOKEN: 'test',
    ENVIRONMENT: 'test',
    R2_SQL_ACCOUNT_ID: 'acct123',
  };
}

/** A minimal, valid single-row GroupAgg for one (service, endpoint, method) group. */
function makeGroupAgg(endpoint: string): GroupAgg {
  return {
    service: SERVICE,
    endpoint,
    method: 'GET',
    count: 1,
    errors4xx: 0,
    errors5xx: 0,
    sumLatency: 5,
    maxLatency: 5,
    colos: ['NRT'],
    zeroLatency: 0,
    latencies: [5],
  };
}

// > STREAM_BATCH_ROWS (2000) so a single `gold` step spans two send()
// batches (2000 + 500) — same reasoning as ROW_COUNT above, applied to
// distinct (service,endpoint,method) groups instead of bronze rows.
const GROUP_COUNT = 2500;

function makeChunkResultWithGroups(groupCount: number): ChunkStepResult {
  const groups: Record<string, GroupAgg> = {};
  for (let i = 0; i < groupCount; i++) {
    groups[`g${i}`] = makeGroupAgg(`/ep${i}`);
  }
  return {
    chunkKey: 'chunk-0',
    service: SERVICE,
    objectCount: 1,
    objectsMissing: 0,
    linesParsed: groupCount,
    validRows: groupCount,
    quarantinedRows: 0,
    groups,
  };
}

describe('runChunk resumability (chunk send-progress marker)', () => {
  it('a fresh run sends all batches and writes a final complete marker matching the returned result', async () => {
    const bucket = makeBucket();
    const sent: SilverRow[][] = [];
    const env = makeEnv(bucket, async (rows) => {
      sent.push(rows);
    });
    const runId = 'day-2026-09-21';

    const result = await runChunk(env, DT, 'chunk-0', makeDescriptor(), runId);

    expect(result.validRows).toBe(ROW_COUNT);
    expect(result.quarantinedRows).toBe(0);
    expect(sent.map((b) => b.length)).toEqual([2000, 500]);

    const markerKey = chunkMarkerKey(DT, runId, 'chunk-0');
    const marker = JSON.parse(bucket.writtenAt(markerKey)!);
    expect(marker.complete).toBe(true);
    expect(marker.sentBatches).toBe(2);
    expect(marker.result).toEqual(result);
  });

  it('a retry that resumes from a marker at sentBatches: 1 sends only the remaining batch', async () => {
    const bucket = makeBucket();
    const runId = 'day-2026-09-21';
    const markerKey = chunkMarkerKey(DT, runId, 'chunk-0');
    // Simulate: an earlier attempt of THIS SAME instance already sent batch
    // 0 (2000 rows) and durably recorded that, but crashed before sending —
    // or before recording — batch 1.
    await writeChunkMarker(bucket, markerKey, { sentBatches: 1, complete: false });

    const sent: SilverRow[][] = [];
    const env = makeEnv(bucket, async (rows) => {
      sent.push(rows);
    });

    const result = await runChunk(env, DT, 'chunk-0', makeDescriptor(), runId);

    // Only the remaining 500-row batch is sent — batch 0 is NOT resent.
    expect(sent.map((b) => b.length)).toEqual([500]);
    expect(result.validRows).toBe(ROW_COUNT);

    const marker = JSON.parse(bucket.writtenAt(markerKey)!);
    expect(marker.complete).toBe(true);
    expect(marker.sentBatches).toBe(2);
    expect(marker.result).toEqual(result);
  });

  it('a complete marker short-circuits entirely: no re-list, no re-send, returns the stored result verbatim', async () => {
    const bucket = makeBucket();
    const runId = 'day-2026-09-21';
    const markerKey = chunkMarkerKey(DT, runId, 'chunk-0');
    const storedResult: ChunkStepResult = {
      chunkKey: 'chunk-0',
      service: SERVICE,
      objectCount: 1,
      objectsMissing: 0,
      linesParsed: ROW_COUNT,
      validRows: ROW_COUNT,
      quarantinedRows: 0,
      groups: {},
    };
    await writeChunkMarker(bucket, markerKey, {
      sentBatches: 2,
      complete: true,
      result: storedResult,
    });

    const listSpy = vi.spyOn(bucket, 'list');
    const sent: SilverRow[][] = [];
    const env = makeEnv(bucket, async (rows) => {
      sent.push(rows);
    });

    const result = await runChunk(env, DT, 'chunk-0', makeDescriptor(), runId);

    expect(result).toEqual(storedResult);
    expect(sent).toEqual([]);
    expect(listSpy).not.toHaveBeenCalled();
  });

  it('a different runId (a fresh attempt/redo) ignores another instance’s marker and starts clean', async () => {
    const bucket = makeBucket();
    const staleRunId = 'day-2026-09-21';
    const freshRunId = 'day-2026-09-21-r2';
    await writeChunkMarker(bucket, chunkMarkerKey(DT, staleRunId, 'chunk-0'), {
      sentBatches: 2,
      complete: true,
      result: { chunkKey: 'chunk-0' } as unknown as ChunkStepResult,
    });

    const sent: SilverRow[][] = [];
    const env = makeEnv(bucket, async (rows) => {
      sent.push(rows);
    });

    const result = await runChunk(env, DT, 'chunk-0', makeDescriptor(), freshRunId);

    expect(sent.map((b) => b.length)).toEqual([2000, 500]);
    expect(result.validRows).toBe(ROW_COUNT);
  });

  it('a chunk with zero valid rows still writes a complete marker (nothing to send, but completion is still recorded)', async () => {
    const bucket = new FakeR2(new Map([[OBJECT_KEY, '']])); // empty object body -> zero lines
    const runId = 'day-2026-09-21';
    const sent: SilverRow[][] = [];
    const env = makeEnv(bucket, async (rows) => {
      sent.push(rows);
    });

    const result = await runChunk(env, DT, 'chunk-0', makeDescriptor(), runId);

    expect(result.validRows).toBe(0);
    expect(sent).toEqual([]);

    const markerKey = chunkMarkerKey(DT, runId, 'chunk-0');
    const marker = JSON.parse(bucket.writtenAt(markerKey)!);
    expect(marker.complete).toBe(true);
    expect(marker.sentBatches).toBe(0);

    // A subsequent call for the same instance short-circuits too.
    const listSpy = vi.spyOn(bucket, 'list');
    const second = await runChunk(env, DT, 'chunk-0', makeDescriptor(), runId);
    expect(second).toEqual(result);
    expect(listSpy).not.toHaveBeenCalled();
  });
});

describe('runGold resumability (gold send-progress marker)', () => {
  it('a fresh run sends all batches and writes a final complete marker matching the returned result', async () => {
    const bucket = new FakeR2(new Map());
    const sent: GoldRow[][] = [];
    const env = makeGoldEnv(bucket, async (rows) => {
      sent.push(rows);
    });
    const runId = 'day-2026-09-21';
    const chunkResults = [makeChunkResultWithGroups(GROUP_COUNT)];

    const result = await runGold(env, DT, chunkResults, runId);

    expect(result.goldRows).toBe(GROUP_COUNT);
    expect(result.goldRequestsSum).toBe(GROUP_COUNT); // 1 request per group
    expect(sent.map((b) => b.length)).toEqual([2000, 500]);

    const markerKey = chunkMarkerKey(DT, runId, 'gold');
    const marker = JSON.parse(bucket.writtenAt(markerKey)!);
    expect(marker.complete).toBe(true);
    expect(marker.sentBatches).toBe(2);
    expect(marker.result).toEqual(result);
  });

  it('a retry that resumes from a marker at sentBatches: 1 sends only the remaining batch', async () => {
    const bucket = new FakeR2(new Map());
    const runId = 'day-2026-09-21';
    const markerKey = chunkMarkerKey(DT, runId, 'gold');
    // Simulate: an earlier attempt of THIS SAME instance already sent batch
    // 0 (2000 gold rows) and durably recorded that, but crashed before
    // sending — or before recording — batch 1.
    await writeChunkMarker<GoldStepResult>(bucket, markerKey, { sentBatches: 1, complete: false });

    const sent: GoldRow[][] = [];
    const env = makeGoldEnv(bucket, async (rows) => {
      sent.push(rows);
    });
    const chunkResults = [makeChunkResultWithGroups(GROUP_COUNT)];

    const result = await runGold(env, DT, chunkResults, runId);

    // Only the remaining 500-row batch is sent — batch 0 is NOT resent.
    expect(sent.map((b) => b.length)).toEqual([500]);
    expect(result.goldRows).toBe(GROUP_COUNT);

    const marker = JSON.parse(bucket.writtenAt(markerKey)!);
    expect(marker.complete).toBe(true);
    expect(marker.sentBatches).toBe(2);
    expect(marker.result).toEqual(result);
  });

  it('a complete marker short-circuits entirely: no merge/re-send, returns the stored result verbatim', async () => {
    const bucket = new FakeR2(new Map());
    const runId = 'day-2026-09-21';
    const markerKey = chunkMarkerKey(DT, runId, 'gold');
    const storedResult: GoldStepResult = { goldRows: GROUP_COUNT, goldRequestsSum: GROUP_COUNT };
    await writeChunkMarker<GoldStepResult>(bucket, markerKey, {
      sentBatches: 2,
      complete: true,
      result: storedResult,
    });

    const sent: GoldRow[][] = [];
    const env = makeGoldEnv(bucket, async (rows) => {
      sent.push(rows);
    });
    // A chunkResults array that, if merged, would NOT reproduce
    // storedResult — proves the merge never ran.
    const chunkResults = [makeChunkResultWithGroups(3)];

    const result = await runGold(env, DT, chunkResults, runId);

    expect(result).toEqual(storedResult);
    expect(sent).toEqual([]);
  });

  it('a different runId (a fresh attempt/redo) ignores another instance’s marker and starts clean', async () => {
    const bucket = new FakeR2(new Map());
    const staleRunId = 'day-2026-09-21';
    const freshRunId = 'day-2026-09-21-r2';
    await writeChunkMarker<GoldStepResult>(bucket, chunkMarkerKey(DT, staleRunId, 'gold'), {
      sentBatches: 2,
      complete: true,
      result: { goldRows: 999, goldRequestsSum: 999 },
    });

    const sent: GoldRow[][] = [];
    const env = makeGoldEnv(bucket, async (rows) => {
      sent.push(rows);
    });
    const chunkResults = [makeChunkResultWithGroups(GROUP_COUNT)];

    const result = await runGold(env, DT, chunkResults, freshRunId);

    expect(sent.map((b) => b.length)).toEqual([2000, 500]);
    expect(result.goldRows).toBe(GROUP_COUNT);
  });

  it('merges groups for the same (service,endpoint,method) key across multiple chunks before sending', async () => {
    const bucket = new FakeR2(new Map());
    const sent: GoldRow[][] = [];
    const env = makeGoldEnv(bucket, async (rows) => {
      sent.push(rows);
    });
    const runId = 'day-2026-09-21';
    // Two chunks contributing to the SAME group key (as would happen for a
    // service spanning >2000 bronze objects, split across chunk-0/chunk-1).
    const shared = makeGroupAgg('/shared');
    const chunkResults: ChunkStepResult[] = [
      { ...makeChunkResultWithGroups(0), groups: { shared } },
      { ...makeChunkResultWithGroups(0), groups: { shared: makeGroupAgg('/shared') } },
    ];

    const result = await runGold(env, DT, chunkResults, runId);

    expect(result.goldRows).toBe(1); // merged into a single gold row
    expect(result.goldRequestsSum).toBe(2); // 1 request from each chunk's copy
  });
});

describe('runVerify (post-manifest R2 SQL duplicate/mismatch check)', () => {
  function makeManifest(overrides: Partial<Manifest> = {}): Manifest {
    return {
      dt: DT,
      status: 'done',
      startedAt: '2026-09-22T01:00:00.000Z',
      finishedAt: '2026-09-22T01:00:05.000Z',
      bronzeObjectCount: 10,
      bronzeObjectsRead: 10,
      bronzeObjectsMissing: 0,
      bronzeLinesParsed: 10,
      silverRows: 9,
      quarantinedRows: 1,
      goldRows: 3,
      goldRequestsSum: 9,
      services: ['svc'],
      assertions: {
        linesEqualsSilverPlusQuarantine: true,
        silverEqualsGoldRequests: true,
        objectsListedEqualsRead: true,
      },
      ...overrides,
    };
  }

  function makeEnvWithToken(token: string | undefined): Env {
    return {
      DATALAKE_R2: {} as Env['DATALAKE_R2'],
      MEDALLION_WORKFLOW: {} as Env['MEDALLION_WORKFLOW'],
      SILVER_STREAM: { send: async () => {} },
      GOLD_STREAM: { send: async () => {} },
      ADMIN_TOKEN: 'test',
      ENVIRONMENT: 'test',
      R2_SQL_ACCOUNT_ID: 'acct123',
      R2_SQL_TOKEN: token,
    };
  }

  /** Fake `fetch` for the R2 SQL REST endpoint: routes by table name in the SQL text. */
  function fakeR2SqlFetch(
    counts: { silver: { n: number; d: number }; gold: { n: number; d: number } },
    failing?: 'silver' | 'gold' | 'both',
  ): typeof fetch {
    return (async (_url: unknown, init: { body: string }) => {
      const { query } = JSON.parse(init.body) as { query: string };
      const isSilver = query.includes('silver.api_metrics');
      if (failing === 'both' || failing === (isSilver ? 'silver' : 'gold')) {
        throw new Error('simulated network error');
      }
      const c = isSilver ? counts.silver : counts.gold;
      return {
        ok: true,
        status: 200,
        json: async () => ({ success: true, result: { rows: [{ n: c.n, d: c.d }] }, errors: [] }),
      } as unknown as Response;
    }) as unknown as typeof fetch;
  }

  it('skips the query and records verify.skipped when R2_SQL_TOKEN is unset, without changing status', async () => {
    const bucket = new FakeR2(new Map());
    const env = makeEnvWithToken(undefined);
    const manifest = makeManifest();

    const result = await runVerify(env, DT, bucket, manifest);

    expect(result.status).toBe('done');
    expect(result.verify).toEqual({ skipped: 'no R2_SQL_TOKEN' });
    const stored = JSON.parse(bucket.writtenAt(manifestKey(DT))!);
    expect(stored).toEqual(result);
  });

  it('stays status=done and records matching counts when R2 SQL agrees with the manifest exactly', async () => {
    const bucket = new FakeR2(new Map());
    const env = makeEnvWithToken('secret');
    const manifest = makeManifest(); // silverRows: 9, goldRows: 3
    const fetchImpl = fakeR2SqlFetch({ silver: { n: 9, d: 9 }, gold: { n: 3, d: 3 } });

    const result = await runVerify(env, DT, bucket, manifest, fetchImpl);

    expect(result.status).toBe('done');
    expect(result.assertions.silverDistinctEqualsManifest).toBe(true);
    expect(result.assertions.goldDistinctEqualsManifest).toBe(true);
    expect(result.verify).toMatchObject({
      silverRows: 9,
      silverDistinct: 9,
      goldRows: 3,
      goldDistinct: 3,
    });
    const stored = JSON.parse(bucket.writtenAt(manifestKey(DT))!);
    expect(stored).toEqual(result);
  });

  it('sets status=duplicates when silver count(*) exceeds count(DISTINCT row_uid) but the distinct count matches', async () => {
    const bucket = new FakeR2(new Map());
    const env = makeEnvWithToken('secret');
    const manifest = makeManifest();
    // 11 silver rows sent, but only 9 distinct — 2 rows got resent (this
    // app's known residual risk — see README "Design decisions §6").
    const fetchImpl = fakeR2SqlFetch({ silver: { n: 11, d: 9 }, gold: { n: 3, d: 3 } });

    const result = await runVerify(env, DT, bucket, manifest, fetchImpl);

    expect(result.status).toBe('duplicates');
    expect(result.assertions.silverDistinctEqualsManifest).toBe(true);
  });

  it('sets status=mismatch when the distinct silver count does not match manifest.silverRows', async () => {
    const bucket = new FakeR2(new Map());
    const env = makeEnvWithToken('secret');
    const manifest = makeManifest();
    const fetchImpl = fakeR2SqlFetch({ silver: { n: 9, d: 8 }, gold: { n: 3, d: 3 } });

    const result = await runVerify(env, DT, bucket, manifest, fetchImpl);

    expect(result.status).toBe('mismatch');
    expect(result.assertions.silverDistinctEqualsManifest).toBe(false);
  });

  it('records verify.error and leaves status unchanged when the R2 SQL request fails — does not throw', async () => {
    const bucket = new FakeR2(new Map());
    const env = makeEnvWithToken('secret');
    const manifest = makeManifest();
    const fetchImpl = fakeR2SqlFetch({ silver: { n: 9, d: 9 }, gold: { n: 3, d: 3 } }, 'both');

    const result = await runVerify(env, DT, bucket, manifest, fetchImpl);

    expect(result.status).toBe('done'); // unchanged from the input manifest
    expect(result.verify).toMatchObject({
      error: expect.stringContaining('simulated network error'),
    });
    const stored = JSON.parse(bucket.writtenAt(manifestKey(DT))!);
    expect(stored).toEqual(result);
  });
});
