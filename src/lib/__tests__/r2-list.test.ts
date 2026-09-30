import { describe, expect, it } from 'vitest';
import {
  CHUNK_SIZE,
  computeChunkDescriptors,
  listChunkKeys,
  listServicePrefixes,
  type R2ListPage,
  type R2Listable,
} from '../r2-list';

/**
 * In-memory fake mirroring the two R2 `list()` behaviors this module relies
 * on: (1) delimiter listing returns one entry per distinct "directory", and
 * (2) plain prefix listing returns exactly `limit` objects per page whenever
 * that many remain (only the final page is short), with an opaque cursor
 * that resumes correctly. Keys must be pre-sorted ascending, like R2's own
 * lexicographic guarantee.
 */
class FakeBucket implements R2Listable {
  constructor(private readonly keys: string[]) {}

  async list(options: {
    prefix?: string;
    delimiter?: string;
    cursor?: string;
    limit?: number;
  }): Promise<R2ListPage> {
    const prefix = options.prefix ?? '';
    const limit = options.limit ?? 1000;
    const matching = this.keys.filter((k) => k.startsWith(prefix));

    if (options.delimiter) {
      const prefixes = new Set<string>();
      for (const k of matching) {
        const rest = k.slice(prefix.length);
        const idx = rest.indexOf(options.delimiter);
        if (idx !== -1) prefixes.add(prefix + rest.slice(0, idx + 1));
      }
      return { objects: [], delimitedPrefixes: Array.from(prefixes).sort(), truncated: false };
    }

    const start = options.cursor ? Number(options.cursor) : 0;
    const page = matching.slice(start, start + limit);
    const truncated = start + limit < matching.length;
    return {
      objects: page.map((key) => ({ key })),
      truncated,
      cursor: truncated ? String(start + limit) : undefined,
    };
  }
}

function makeKeys(dt: string, service: string, count: number): string[] {
  return Array.from({ length: count }, (_, i) => {
    const n = String(i).padStart(6, '0');
    return `api-metrics/${dt}/${service}/00/${n}-aaaaaa.jsonl`;
  });
}

/**
 * A fake bucket that returns RAGGED page sizes — i.e. it may return fewer
 * than the requested `limit` even while `truncated: true` (more data
 * remains). This is exactly the R2 behavior that a previous version of
 * `computeChunkDescriptors`/`listChunkKeys` silently assumed could not
 * happen, which caused keys to be skipped or double-listed at a chunk
 * boundary that fell mid-page. Pages are pre-split into fixed slices by
 * `pageSizes` (cycling the last size if there are more objects than sizes
 * given), and `cursor` is simply "which pre-split page comes next" — fully
 * deterministic and independent of `limit`/call count, so calling `list()`
 * again from the same cursor (as `listChunkKeys` does, separately from
 * `computeChunkDescriptors`) always replays the identical page sequence.
 */
class RaggedPageBucket implements R2Listable {
  private readonly pages: string[][] = [];

  constructor(keys: string[], pageSizes: number[]) {
    let i = 0;
    let sizeIdx = 0;
    while (i < keys.length) {
      const size = pageSizes[Math.min(sizeIdx, pageSizes.length - 1)];
      this.pages.push(keys.slice(i, i + size));
      i += size;
      sizeIdx += 1;
    }
  }

  async list(options: {
    prefix?: string;
    delimiter?: string;
    cursor?: string;
    limit?: number;
  }): Promise<R2ListPage> {
    if (options.delimiter) throw new Error('RaggedPageBucket does not support delimiter listing');
    const pageIndex = options.cursor ? Number(options.cursor) : 0;
    const page = this.pages[pageIndex] ?? [];
    const truncated = pageIndex + 1 < this.pages.length;
    return {
      objects: page.map((key) => ({ key })),
      truncated,
      cursor: truncated ? String(pageIndex + 1) : undefined,
    };
  }
}

describe('listServicePrefixes', () => {
  it('returns one prefix per distinct service directory', async () => {
    const bucket = new FakeBucket([
      ...makeKeys('2026-09-21', 'weathio', 3),
      ...makeKeys('2026-09-21', 'kanpo-archive', 2),
      ...makeKeys('2026-09-20', 'weathio', 5), // different day — must be excluded
    ]);
    const prefixes = await listServicePrefixes(bucket, 'api-metrics/2026-09-21/');
    expect(prefixes.sort()).toEqual([
      'api-metrics/2026-09-21/kanpo-archive/',
      'api-metrics/2026-09-21/weathio/',
    ]);
  });

  it('returns an empty list for a day with no objects', async () => {
    const bucket = new FakeBucket([]);
    expect(await listServicePrefixes(bucket, 'api-metrics/2026-09-21/')).toEqual([]);
  });
});

describe('computeChunkDescriptors + listChunkKeys round-trip', () => {
  it('splits an exact multiple of CHUNK_SIZE into equal chunks with no remainder chunk', async () => {
    const dt = '2026-09-21';
    const service = 'weathio';
    const prefix = `api-metrics/${dt}/${service}/`;
    const total = CHUNK_SIZE * 2; // exactly 2 chunks
    const keys = makeKeys(dt, service, total);
    const bucket = new FakeBucket(keys);

    const { descriptors, totalCount } = await computeChunkDescriptors(bucket, service, prefix);
    expect(totalCount).toBe(total);
    expect(descriptors).toHaveLength(2);
    expect(descriptors.map((d) => d.expectedCount)).toEqual([CHUNK_SIZE, CHUNK_SIZE]);
    expect(descriptors[0].startCursor).toBeUndefined();
    expect(descriptors[1].startCursor).toBeDefined();

    const reconstructed: string[] = [];
    for (const d of descriptors) {
      reconstructed.push(...(await listChunkKeys(bucket, d)));
    }
    expect(reconstructed).toEqual(keys);
  });

  it('handles a partial final chunk (not a multiple of CHUNK_SIZE)', async () => {
    const dt = '2026-09-21';
    const service = 'onokoro';
    const prefix = `api-metrics/${dt}/${service}/`;
    const total = CHUNK_SIZE + 137;
    const keys = makeKeys(dt, service, total);
    const bucket = new FakeBucket(keys);

    const { descriptors, totalCount } = await computeChunkDescriptors(bucket, service, prefix);
    expect(totalCount).toBe(total);
    expect(descriptors.map((d) => d.expectedCount)).toEqual([CHUNK_SIZE, 137]);

    const reconstructed: string[] = [];
    for (const d of descriptors) {
      reconstructed.push(...(await listChunkKeys(bucket, d)));
    }
    expect(reconstructed).toEqual(keys);
  });

  it('handles fewer than one page of objects', async () => {
    const dt = '2026-09-21';
    const service = 'tiny';
    const prefix = `api-metrics/${dt}/${service}/`;
    const keys = makeKeys(dt, service, 3);
    const bucket = new FakeBucket(keys);

    const { descriptors, totalCount } = await computeChunkDescriptors(bucket, service, prefix);
    expect(totalCount).toBe(3);
    expect(descriptors).toHaveLength(1);
    expect(descriptors[0].expectedCount).toBe(3);
    expect(await listChunkKeys(bucket, descriptors[0])).toEqual(keys);
  });

  it('handles zero objects', async () => {
    const bucket = new FakeBucket([]);
    const { descriptors, totalCount } = await computeChunkDescriptors(
      bucket,
      'ghost',
      'api-metrics/2026-09-21/ghost/',
    );
    expect(totalCount).toBe(0);
    expect(descriptors).toEqual([]);
  });

  it('does not leak another service’s keys into a chunk (prefix isolation)', async () => {
    const dt = '2026-09-21';
    const keys = [...makeKeys(dt, 'a', 50), ...makeKeys(dt, 'bbb', 50)];
    const bucket = new FakeBucket(keys);
    const { descriptors } = await computeChunkDescriptors(bucket, 'a', `api-metrics/${dt}/a/`);
    expect(descriptors).toHaveLength(1);
    const gotKeys = await listChunkKeys(bucket, descriptors[0]);
    expect(gotKeys.every((k) => k.startsWith(`api-metrics/${dt}/a/`))).toBe(true);
    expect(gotKeys).toHaveLength(50);
  });
});

describe('computeChunkDescriptors + listChunkKeys with ragged page sizes', () => {
  it('every key appears exactly once across chunks, in order, with no gaps or overlaps (1000,700,1000,300,1000)', async () => {
    const dt = '2026-09-21';
    const service = 'ragged';
    const prefix = `api-metrics/${dt}/${service}/`;
    const pageSizes = [1000, 700, 1000, 300, 1000];
    const total = pageSizes.reduce((s, n) => s + n, 0); // 4000
    const keys = makeKeys(dt, service, total);
    const bucket = new RaggedPageBucket(keys, pageSizes);

    const { descriptors, totalCount } = await computeChunkDescriptors(bucket, service, prefix);
    expect(totalCount).toBe(total);

    // Every chunk must be >= CHUNK_SIZE (except possibly the last), since
    // chunks only close once accumulated whole pages reach CHUNK_SIZE.
    for (const d of descriptors.slice(0, -1)) {
      expect(d.expectedCount).toBeGreaterThanOrEqual(CHUNK_SIZE);
    }

    const reconstructed: string[] = [];
    for (const d of descriptors) {
      const chunkKeys = await listChunkKeys(bucket, d);
      expect(chunkKeys).toHaveLength(d.expectedCount);
      reconstructed.push(...chunkKeys);
    }
    // Exact order preserved end-to-end, and — the actual bug — no key
    // missing (a gap) and none duplicated (an overlap).
    expect(reconstructed).toEqual(keys);
    expect(new Set(reconstructed).size).toBe(keys.length);
  });

  it('handles a boundary that would fall mid-page under a fixed-page-size assumption (1500,1500,1500)', async () => {
    // With naive "close at exactly CHUNK_SIZE objects seen" logic (the
    // original bug), the first chunk would close 500 objects into the
    // second page and resume the next chunk from that whole page's cursor
    // — skipping its last 1000 objects entirely. This fake makes that
    // failure mode concrete and unmissable if it regresses.
    const dt = '2026-09-21';
    const service = 'midpage';
    const prefix = `api-metrics/${dt}/${service}/`;
    const pageSizes = [1500, 1500, 1500];
    const keys = makeKeys(dt, service, 4500);
    const bucket = new RaggedPageBucket(keys, pageSizes);

    const { descriptors } = await computeChunkDescriptors(bucket, service, prefix);
    const reconstructed: string[] = [];
    for (const d of descriptors) {
      reconstructed.push(...(await listChunkKeys(bucket, d)));
    }
    expect(reconstructed).toEqual(keys);
    expect(new Set(reconstructed).size).toBe(4500);
  });

  it('listChunkKeys throws if the reconstructed count does not match expectedCount', async () => {
    const dt = '2026-09-21';
    const service = 'mismatch';
    const prefix = `api-metrics/${dt}/${service}/`;
    const keys = makeKeys(dt, service, 10);
    const bucket = new RaggedPageBucket(keys, [10]);
    const { descriptors } = await computeChunkDescriptors(bucket, service, prefix);
    expect(descriptors).toHaveLength(1);

    const corrupted = { ...descriptors[0], expectedCount: descriptors[0].expectedCount + 1 };
    await expect(listChunkKeys(bucket, corrupted)).rejects.toThrow(/expected 11 keys/);
  });
});
