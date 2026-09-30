import { describe, expect, it } from 'vitest';
import {
  applyVerifyCounts,
  applyVerifyError,
  applyVerifySkipped,
  buildManifest,
  type Manifest,
  type ManifestVerifyCounts,
} from '../manifest';

const BASE = {
  dt: '2026-09-21',
  startedAt: '2026-09-22T01:00:00.000Z',
  bronzeObjectCount: 10,
  bronzeObjectsRead: 10,
  bronzeObjectsMissing: 0,
  bronzeLinesParsed: 10,
  silverRows: 9,
  quarantinedRows: 1,
  goldRows: 3,
  goldRequestsSum: 9,
  services: ['weathio'],
};

describe('buildManifest', () => {
  it('status=done when all assertions hold', () => {
    const m = buildManifest(BASE);
    expect(m.status).toBe('done');
    expect(m.assertions.linesEqualsSilverPlusQuarantine).toBe(true);
    expect(m.assertions.silverEqualsGoldRequests).toBe(true);
    expect(m.assertions.objectsListedEqualsRead).toBe(true);
    expect(m.dt).toBe('2026-09-21');
    expect(m.bronzeObjectsRead).toBe(10);
    expect(m.bronzeObjectsMissing).toBe(0);
    expect(typeof m.finishedAt).toBe('string');
  });

  it('status=mismatch when lines != silver + quarantine', () => {
    const m = buildManifest({ ...BASE, bronzeLinesParsed: 11 });
    expect(m.status).toBe('mismatch');
    expect(m.assertions.linesEqualsSilverPlusQuarantine).toBe(false);
    expect(m.assertions.silverEqualsGoldRequests).toBe(true);
    expect(m.assertions.objectsListedEqualsRead).toBe(true);
  });

  it('status=mismatch when silver != gold requests sum', () => {
    const m = buildManifest({ ...BASE, goldRequestsSum: 8 });
    expect(m.status).toBe('mismatch');
    expect(m.assertions.linesEqualsSilverPlusQuarantine).toBe(true);
    expect(m.assertions.silverEqualsGoldRequests).toBe(false);
    expect(m.assertions.objectsListedEqualsRead).toBe(true);
  });

  it('status=mismatch when some bronze objects were listed but never successfully read (missing)', () => {
    const m = buildManifest({
      ...BASE,
      bronzeObjectsRead: 9,
      bronzeObjectsMissing: 1,
    });
    expect(m.status).toBe('mismatch');
    expect(m.assertions.objectsListedEqualsRead).toBe(false);
    // The other two assertions can still hold independently — a missing
    // object silently produces zero lines from it, which does NOT by itself
    // break the lines == silver + quarantine arithmetic. That's exactly why
    // this needs to be its own, separate assertion.
    expect(m.assertions.linesEqualsSilverPlusQuarantine).toBe(true);
    expect(m.assertions.silverEqualsGoldRequests).toBe(true);
  });

  it('status=mismatch when bronzeObjectsMissing is nonzero even if the read/listed counts superficially balance', () => {
    // Pathological but defensive: missing > 0 must always fail the
    // assertion, regardless of how bronzeObjectCount vs bronzeObjectsRead
    // happen to compare.
    const m = buildManifest({
      ...BASE,
      bronzeObjectCount: 10,
      bronzeObjectsRead: 10,
      bronzeObjectsMissing: 1,
    });
    expect(m.assertions.objectsListedEqualsRead).toBe(false);
    expect(m.status).toBe('mismatch');
  });

  it('status=done for an all-zero (no bronze objects) day', () => {
    const m = buildManifest({
      ...BASE,
      bronzeObjectCount: 0,
      bronzeObjectsRead: 0,
      bronzeObjectsMissing: 0,
      bronzeLinesParsed: 0,
      silverRows: 0,
      quarantinedRows: 0,
      goldRows: 0,
      goldRequestsSum: 0,
    });
    expect(m.status).toBe('done');
  });
});

describe('applyVerifyCounts / applyVerifySkipped / applyVerifyError (the `verify` step)', () => {
  const doneManifest: Manifest = buildManifest(BASE); // silverRows: 9, goldRows: 3, status: 'done'

  const matchingCounts: ManifestVerifyCounts = {
    silverRows: 9,
    silverDistinct: 9,
    goldRows: 3,
    goldDistinct: 3,
    checkedAt: '2026-09-22T01:06:00.000Z',
  };

  it('stays status=done when R2 SQL counts exactly match what this run sent', () => {
    const m = applyVerifyCounts(doneManifest, matchingCounts);
    expect(m.status).toBe('done');
    expect(m.assertions.silverDistinctEqualsManifest).toBe(true);
    expect(m.assertions.goldDistinctEqualsManifest).toBe(true);
    expect(m.verify).toEqual(matchingCounts);
    // The build-time assertions are untouched.
    expect(m.assertions.linesEqualsSilverPlusQuarantine).toBe(true);
  });

  it('status=duplicates when count(*) > count(DISTINCT row_uid) but the distinct count matches manifest', () => {
    const m = applyVerifyCounts(doneManifest, {
      ...matchingCounts,
      silverRows: 11, // 2 duplicate silver rows; distinct (9) still matches manifest.silverRows
    });
    expect(m.status).toBe('duplicates');
    expect(m.assertions.silverDistinctEqualsManifest).toBe(true);
    expect(m.assertions.goldDistinctEqualsManifest).toBe(true);
  });

  it('status=duplicates when the gold table has the extra rows instead of silver', () => {
    const m = applyVerifyCounts(doneManifest, { ...matchingCounts, goldRows: 5 });
    expect(m.status).toBe('duplicates');
  });

  it('status=mismatch (not duplicates) when silverDistinct does not match manifest.silverRows', () => {
    const m = applyVerifyCounts(doneManifest, { ...matchingCounts, silverDistinct: 8 });
    expect(m.status).toBe('mismatch');
    expect(m.assertions.silverDistinctEqualsManifest).toBe(false);
  });

  it('status=mismatch when goldDistinct does not match manifest.goldRows', () => {
    const m = applyVerifyCounts(doneManifest, { ...matchingCounts, goldDistinct: 4 });
    expect(m.status).toBe('mismatch');
    expect(m.assertions.goldDistinctEqualsManifest).toBe(false);
  });

  it('mismatch takes priority over duplicates when both conditions hold', () => {
    const m = applyVerifyCounts(doneManifest, {
      ...matchingCounts,
      silverDistinct: 8,
      silverRows: 20,
    });
    expect(m.status).toBe('mismatch');
  });

  it('applyVerifySkipped records the skip reason without touching status', () => {
    const m = applyVerifySkipped(doneManifest, 'no R2_SQL_TOKEN');
    expect(m.status).toBe('done');
    expect(m.verify).toEqual({ skipped: 'no R2_SQL_TOKEN' });
    expect(m.assertions.silverDistinctEqualsManifest).toBeUndefined();
  });

  it('applyVerifyError records the error without touching status', () => {
    const m = applyVerifyError(doneManifest, 'fetch failed: network error');
    expect(m.status).toBe('done');
    expect(m.verify).toEqual({ error: 'fetch failed: network error' });
  });
});
