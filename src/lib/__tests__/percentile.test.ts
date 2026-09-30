import { describe, expect, it } from 'vitest';
import { percentile } from '../percentile';

describe('percentile', () => {
  it('returns 0 for an empty array', () => {
    expect(percentile([], 50)).toBe(0);
    expect(percentile([], 99)).toBe(0);
  });

  it('returns the single value for a 1-element array at any percentile', () => {
    expect(percentile([42], 1)).toBe(42);
    expect(percentile([42], 99)).toBe(42);
  });

  it('matches nearest-rank expectations for 1..100', () => {
    const sorted = Array.from({ length: 100 }, (_, i) => i + 1); // [1..100]
    expect(percentile(sorted, 50)).toBe(50);
    expect(percentile(sorted, 95)).toBe(95);
    expect(percentile(sorted, 99)).toBe(99);
    expect(percentile(sorted, 100)).toBe(100);
    expect(percentile(sorted, 1)).toBe(1);
  });

  it('is monotonic non-decreasing as p increases', () => {
    const sorted = [3, 5, 5, 8, 13, 21, 34, 55, 89, 144];
    let prev = -Infinity;
    for (let p = 1; p <= 100; p++) {
      const v = percentile(sorted, p);
      expect(v).toBeGreaterThanOrEqual(prev);
      prev = v;
    }
  });
});
