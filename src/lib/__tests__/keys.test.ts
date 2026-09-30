import { describe, expect, it } from 'vitest';
import {
  chunkMarkerKey,
  dayPrefix,
  instanceIdFor,
  isoDateOffsetDays,
  isValidAttempt,
  isValidForce,
  isValidRunId,
  manifestKey,
  parseForce,
  quarantineKeyFor,
  yesterdayUTC,
} from '../keys';

describe('keys', () => {
  it('dayPrefix builds the bronze day prefix', () => {
    expect(dayPrefix('2026-09-21')).toBe('api-metrics/2026-09-21/');
  });

  it('quarantineKeyFor namespaces by dt and chunk', () => {
    expect(quarantineKeyFor('2026-09-21', 'chunk-3')).toBe(
      'quarantine/api-metrics/dt=2026-09-21/chunk-3.jsonl',
    );
  });

  it('manifestKey', () => {
    expect(manifestKey('2026-09-21')).toBe('_state/medallion/dt=2026-09-21.json');
  });

  it('chunkMarkerKey nests under dt=<dt>/<runId>/, distinct from manifestKey', () => {
    expect(chunkMarkerKey('2026-09-21', 'day-2026-09-21', 'chunk-3')).toBe(
      '_state/medallion/dt=2026-09-21/day-2026-09-21/chunk-3.json',
    );
    // Never collides with the manifest object, even though both share the
    // `_state/medallion/` namespace and the same dt.
    expect(chunkMarkerKey('2026-09-21', 'day-2026-09-21', 'chunk-3')).not.toBe(
      manifestKey('2026-09-21'),
    );
  });

  it('chunkMarkerKey differs per runId — a redo (fresh attempt) never collides with a prior attempt', () => {
    const a = chunkMarkerKey('2026-09-21', 'day-2026-09-21', 'chunk-0');
    const b = chunkMarkerKey('2026-09-21', 'day-2026-09-21-r2', 'chunk-0');
    expect(a).not.toBe(b);
  });

  it('isoDateOffsetDays handles month/year boundaries in UTC', () => {
    expect(isoDateOffsetDays(new Date('2026-01-01T00:30:00Z'), -1)).toBe('2025-12-31');
    expect(isoDateOffsetDays(new Date('2026-03-01T00:00:00Z'), -1)).toBe('2026-02-28');
  });

  it('yesterdayUTC ignores local time-of-day and uses UTC calendar date', () => {
    // 00:05 UTC on the 22nd — "yesterday" must be the 21st regardless of
    // wall-clock timezone the process happens to run in.
    expect(yesterdayUTC(new Date('2026-09-22T00:05:00Z'))).toBe('2026-09-21');
    // 23:55 UTC on the 22nd — still the 21st.
    expect(yesterdayUTC(new Date('2026-09-22T23:55:00Z'))).toBe('2026-09-21');
  });
});

describe('instanceIdFor', () => {
  it('is the bare day id with no attempt', () => {
    expect(instanceIdFor('2026-09-21')).toBe('day-2026-09-21');
    expect(instanceIdFor('2026-09-21', undefined)).toBe('day-2026-09-21');
  });

  it('suffixes with -r<attempt> when an attempt is given', () => {
    expect(instanceIdFor('2026-09-21', '2')).toBe('day-2026-09-21-r2');
    expect(instanceIdFor('2026-09-21', '10')).toBe('day-2026-09-21-r10');
  });

  it('a different attempt yields a different, non-colliding id', () => {
    const ids = new Set([
      instanceIdFor('2026-09-21'),
      instanceIdFor('2026-09-21', '1'),
      instanceIdFor('2026-09-21', '2'),
      instanceIdFor('2026-09-21', '3'),
    ]);
    expect(ids.size).toBe(4);
  });
});

describe('isValidAttempt', () => {
  it('accepts null (absent) and positive integers as strings', () => {
    expect(isValidAttempt(null)).toBe(true);
    expect(isValidAttempt('1')).toBe(true);
    expect(isValidAttempt('2')).toBe(true);
    expect(isValidAttempt('42')).toBe(true);
  });

  it('rejects zero, negatives, non-integers, and non-numeric strings', () => {
    expect(isValidAttempt('0')).toBe(false);
    expect(isValidAttempt('-1')).toBe(false);
    expect(isValidAttempt('1.5')).toBe(false);
    expect(isValidAttempt('abc')).toBe(false);
    expect(isValidAttempt('')).toBe(false);
    expect(isValidAttempt('02')).toBe(false); // leading zero
  });
});

describe('isValidForce / parseForce', () => {
  it('accepts null (absent), "1", and "true"', () => {
    expect(isValidForce(null)).toBe(true);
    expect(isValidForce('1')).toBe(true);
    expect(isValidForce('true')).toBe(true);
  });

  it('rejects anything else, including falsy-looking values', () => {
    expect(isValidForce('0')).toBe(false);
    expect(isValidForce('false')).toBe(false);
    expect(isValidForce('yes')).toBe(false);
    expect(isValidForce('')).toBe(false);
  });

  it('parseForce is true only for the accepted truthy values', () => {
    expect(parseForce(null)).toBe(false);
    expect(parseForce('1')).toBe(true);
    expect(parseForce('true')).toBe(true);
  });
});

describe('isValidRunId', () => {
  it('accepts a non-empty string', () => {
    expect(isValidRunId('day-2026-09-21')).toBe(true);
    expect(isValidRunId('day-2026-09-21-r2')).toBe(true);
    expect(isValidRunId('x')).toBe(true);
  });

  it('rejects an empty string', () => {
    expect(isValidRunId('')).toBe(false);
  });

  it('rejects non-string values a runtime might unexpectedly hand back', () => {
    expect(isValidRunId(undefined)).toBe(false);
    expect(isValidRunId(null)).toBe(false);
    expect(isValidRunId(0)).toBe(false);
    expect(isValidRunId({})).toBe(false);
    expect(isValidRunId([])).toBe(false);
  });
});
