import { describe, expect, it } from 'vitest';
import { mapWithConcurrency } from '../concurrency';

describe('mapWithConcurrency', () => {
  it('preserves input order in the output regardless of completion order', async () => {
    const items = [30, 10, 20, 5, 25];
    const result = await mapWithConcurrency(items, 3, async (ms) => {
      await new Promise((resolve) => setTimeout(resolve, ms));
      return ms;
    });
    expect(result).toEqual(items);
  });

  it('never runs more than `concurrency` tasks at once', async () => {
    const items = Array.from({ length: 20 }, (_, i) => i);
    let inFlight = 0;
    let maxInFlight = 0;
    const CONCURRENCY = 4;

    await mapWithConcurrency(items, CONCURRENCY, async (i) => {
      inFlight += 1;
      maxInFlight = Math.max(maxInFlight, inFlight);
      await new Promise((resolve) => setTimeout(resolve, 5));
      inFlight -= 1;
      return i * 2;
    });

    expect(maxInFlight).toBeLessThanOrEqual(CONCURRENCY);
  });

  it('handles an empty array', async () => {
    const result = await mapWithConcurrency([], 5, async (x: never) => x);
    expect(result).toEqual([]);
  });

  it('clamps concurrency to the item count', async () => {
    const result = await mapWithConcurrency([1, 2], 100, async (x) => x + 1);
    expect(result).toEqual([2, 3]);
  });

  it('propagates a thrown error', async () => {
    await expect(
      mapWithConcurrency([1, 2, 3], 2, async (x) => {
        if (x === 2) throw new Error('boom');
        return x;
      }),
    ).rejects.toThrow('boom');
  });
});
