import { describe, expect, it } from 'vitest';
import { readChunkMarker, writeChunkMarker, type R2MarkerStore } from '../chunk-marker';

/** In-memory fake of the narrow R2 get/put surface chunk-marker.ts needs. */
class FakeMarkerBucket implements R2MarkerStore {
  private readonly store = new Map<string, string>();

  async get(key: string): Promise<{ text(): Promise<string> } | null> {
    const value = this.store.get(key);
    if (value === undefined) return null;
    return { text: async () => value };
  }

  async put(key: string, value: string): Promise<unknown> {
    this.store.set(key, value);
    return null;
  }

  has(key: string): boolean {
    return this.store.has(key);
  }
}

interface FakeResult {
  chunkKey: string;
  validRows: number;
}

describe('readChunkMarker', () => {
  it('returns the "nothing sent yet" marker when none has been written', async () => {
    const bucket = new FakeMarkerBucket();
    const marker = await readChunkMarker<FakeResult>(bucket, 'some/key.json');
    expect(marker).toEqual({ sentBatches: 0, complete: false });
  });

  it('round-trips a written in-progress marker', async () => {
    const bucket = new FakeMarkerBucket();
    await writeChunkMarker<FakeResult>(bucket, 'k', { sentBatches: 1, complete: false });
    const marker = await readChunkMarker<FakeResult>(bucket, 'k');
    expect(marker).toEqual({ sentBatches: 1, complete: false });
  });

  it('round-trips a written complete marker with its result', async () => {
    const bucket = new FakeMarkerBucket();
    const result: FakeResult = { chunkKey: 'chunk-0', validRows: 42 };
    await writeChunkMarker<FakeResult>(bucket, 'k', { sentBatches: 3, complete: true, result });
    const marker = await readChunkMarker<FakeResult>(bucket, 'k');
    expect(marker).toEqual({ sentBatches: 3, complete: true, result });
  });

  it('a later write overwrites (not merges with) an earlier one at the same key', async () => {
    const bucket = new FakeMarkerBucket();
    await writeChunkMarker<FakeResult>(bucket, 'k', { sentBatches: 1, complete: false });
    await writeChunkMarker<FakeResult>(bucket, 'k', {
      sentBatches: 2,
      complete: true,
      result: { chunkKey: 'chunk-0', validRows: 5 },
    });
    const marker = await readChunkMarker<FakeResult>(bucket, 'k');
    expect(marker.sentBatches).toBe(2);
    expect(marker.complete).toBe(true);
  });

  it('markers at different keys do not interfere with each other', async () => {
    const bucket = new FakeMarkerBucket();
    await writeChunkMarker<FakeResult>(bucket, 'chunk-0', { sentBatches: 1, complete: false });
    await writeChunkMarker<FakeResult>(bucket, 'chunk-1', { sentBatches: 2, complete: false });
    expect((await readChunkMarker<FakeResult>(bucket, 'chunk-0')).sentBatches).toBe(1);
    expect((await readChunkMarker<FakeResult>(bucket, 'chunk-1')).sentBatches).toBe(2);
  });
});
