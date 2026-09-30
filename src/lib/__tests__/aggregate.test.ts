import { describe, expect, it } from 'vitest';
import {
  UNMATCHED_ENDPOINT,
  addRowToGroup,
  goldEndpoint,
  goldRowUid,
  groupKey,
  mergeGroups,
  newGroup,
  toGoldRow,
} from '../aggregate';
import type { SilverRow } from '../../types';

function row(overrides: Partial<SilverRow> = {}): SilverRow {
  return {
    ts: '2026-09-21T00:00:00.000Z',
    dt: '2026-09-21',
    service: 'weathio',
    endpoint: '/v1/forecast',
    method: 'GET',
    status: 200,
    status_class: '2xx',
    latency_ms: 100,
    request_size: null,
    response_size: null,
    user_id: null,
    cf_ray: null,
    cf_colo: 'NRT',
    error_message: null,
    source_key: 'k',
    row_uid: 'k#0',
    ...overrides,
  };
}

describe('groupKey', () => {
  it('is distinct per (service, endpoint, method) and stable', () => {
    expect(groupKey('a', '/x', 'GET')).toBe(groupKey('a', '/x', 'GET'));
    expect(groupKey('a', '/x', 'GET')).not.toBe(groupKey('a', '/x', 'POST'));
    expect(groupKey('a', '/x', 'GET')).not.toBe(groupKey('a', '/y', 'GET'));
  });
});

describe('addRowToGroup', () => {
  it('accumulates counts, errors, latency stats and unique colos', () => {
    const g = newGroup('weathio', '/v1/forecast', 'GET');
    addRowToGroup(g, row({ latency_ms: 100, status_class: '2xx', cf_colo: 'NRT' }));
    addRowToGroup(g, row({ latency_ms: 300, status_class: '4xx', cf_colo: 'NRT' }));
    addRowToGroup(g, row({ latency_ms: 200, status_class: '5xx', cf_colo: 'KIX' }));

    expect(g.count).toBe(3);
    expect(g.errors4xx).toBe(1);
    expect(g.errors5xx).toBe(1);
    expect(g.sumLatency).toBe(600);
    expect(g.maxLatency).toBe(300);
    expect(g.colos.sort()).toEqual(['KIX', 'NRT']);
    expect(g.latencies).toEqual([100, 300, 200]);
  });

  it('does not add a colo when cf_colo is null', () => {
    const g = newGroup('a', '/x', 'GET');
    addRowToGroup(g, row({ cf_colo: null }));
    expect(g.colos).toEqual([]);
  });
});

describe('mergeGroups', () => {
  it('sums counts/errors, takes max latency, unions colos, concatenates latencies', () => {
    const a = newGroup('svc', '/e', 'GET');
    addRowToGroup(a, row({ latency_ms: 50, cf_colo: 'NRT', status_class: '2xx' }));
    const b = newGroup('svc', '/e', 'GET');
    addRowToGroup(b, row({ latency_ms: 500, cf_colo: 'KIX', status_class: '5xx' }));

    const merged = mergeGroups(a, b);
    expect(merged.count).toBe(2);
    expect(merged.errors5xx).toBe(1);
    expect(merged.maxLatency).toBe(500);
    expect(merged.colos.sort()).toEqual(['KIX', 'NRT']);
    expect(merged.latencies.sort((x, y) => x - y)).toEqual([50, 500]);
  });
});

describe('toGoldRow', () => {
  it('computes error_rate, avg, max and sorted-unique colos string', () => {
    const g = newGroup('svc', '/e', 'GET');
    addRowToGroup(g, row({ latency_ms: 100, status_class: '2xx', cf_colo: 'NRT' }));
    addRowToGroup(g, row({ latency_ms: 100, status_class: '2xx', cf_colo: 'NRT' }));
    addRowToGroup(g, row({ latency_ms: 300, status_class: '4xx', cf_colo: 'KIX' }));
    addRowToGroup(g, row({ latency_ms: 900, status_class: '5xx', cf_colo: 'ICN' }));

    const gold = toGoldRow('2026-09-21', g);
    expect(gold.dt).toBe('2026-09-21');
    expect(gold.requests).toBe(4);
    expect(gold.errors_4xx).toBe(1);
    expect(gold.errors_5xx).toBe(1);
    expect(gold.error_rate).toBeCloseTo(0.5, 10);
    expect(gold.latency_avg).toBeCloseTo((100 + 100 + 300 + 900) / 4, 10);
    expect(gold.latency_max).toBe(900);
    expect(gold.colos).toBe('ICN,KIX,NRT');
  });

  it('handles a zero-count group without dividing by zero', () => {
    const g = newGroup('svc', '/e', 'GET');
    const gold = toGoldRow('2026-09-21', g);
    expect(gold.requests).toBe(0);
    expect(gold.error_rate).toBe(0);
    expect(gold.requests_measured).toBe(0);
    expect(gold.latency_avg).toBeNull();
    expect(gold.colos).toBe('');
  });
});

describe('zero-latency requests (Workers Date.now() does not advance without I/O)', () => {
  it('counts 0 ms requests separately and keeps them out of the percentiles', () => {
    const g = newGroup('weathio', '/v1/forecast', 'GET');
    for (const latency_ms of [0, 0, 0, 100, 200, 300]) {
      addRowToGroup(g, row({ latency_ms }));
    }
    const gold = toGoldRow('2026-09-21', g);
    expect(gold.requests).toBe(6);
    expect(gold.requests_zero_latency).toBe(3);
    expect(gold.requests_measured).toBe(3);
    // avg over the measured three (200), not over all six (100).
    expect(gold.latency_avg).toBe(200);
    expect(gold.latency_max).toBe(300);
    expect(gold.latency_p50).toBe(200);
  });

  it('still counts a 0 ms request in requests, errors and colos', () => {
    const g = newGroup('weathio', '/v1/forecast', 'GET');
    addRowToGroup(g, row({ latency_ms: 0, status: 500, status_class: '5xx', cf_colo: 'KIX' }));
    const gold = toGoldRow('2026-09-21', g);
    expect(gold.requests).toBe(1);
    expect(gold.errors_5xx).toBe(1);
    expect(gold.error_rate).toBe(1);
    expect(gold.colos).toBe('KIX');
    expect(gold.latency_p50).toBeNull();
    expect(gold.latency_max).toBeNull();
  });

  it('merges zero-latency counts across chunks', () => {
    const a = newGroup('weathio', '/v1/forecast', 'GET');
    addRowToGroup(a, row({ latency_ms: 0 }));
    const b = newGroup('weathio', '/v1/forecast', 'GET');
    addRowToGroup(b, row({ latency_ms: 50 }));
    const gold = toGoldRow('2026-09-21', mergeGroups(a, b));
    expect(gold.requests).toBe(2);
    expect(gold.requests_zero_latency).toBe(1);
    expect(gold.requests_measured).toBe(1);
    expect(gold.latency_avg).toBe(50);
  });
});

describe('goldRowUid / row_uid determinism', () => {
  it('is exactly "<dt>|<service>|<endpoint>|<method>"', () => {
    expect(goldRowUid('2026-09-21', 'weathio', '/v1/forecast', 'GET')).toBe(
      '2026-09-21|weathio|/v1/forecast|GET',
    );
  });

  it('is the same for the same group and different for a different group', () => {
    expect(goldRowUid('2026-09-21', 'a', '/x', 'GET')).toBe(
      goldRowUid('2026-09-21', 'a', '/x', 'GET'),
    );
    expect(goldRowUid('2026-09-21', 'a', '/x', 'GET')).not.toBe(
      goldRowUid('2026-09-21', 'a', '/x', 'POST'),
    );
    expect(goldRowUid('2026-09-21', 'a', '/x', 'GET')).not.toBe(
      goldRowUid('2026-09-22', 'a', '/x', 'GET'),
    );
  });

  it('toGoldRow sets row_uid from (dt, service, endpoint, method), stable across merges', () => {
    const a = newGroup('svc', '/e', 'GET');
    addRowToGroup(a, row({ latency_ms: 50 }));
    const b = newGroup('svc', '/e', 'GET');
    addRowToGroup(b, row({ latency_ms: 60 }));

    const goldA = toGoldRow('2026-09-21', a);
    const goldMerged = toGoldRow('2026-09-21', mergeGroups(a, b));
    expect(goldA.row_uid).toBe('2026-09-21|svc|/e|GET');
    expect(goldMerged.row_uid).toBe(goldA.row_uid);
  });
});

describe('goldEndpoint', () => {
  it('collapses 404 paths so scanner probes do not each become a gold row', () => {
    expect(goldEndpoint('/.env', 404)).toBe(UNMATCHED_ENDPOINT);
    expect(goldEndpoint('/wp-login.php', 404)).toBe(UNMATCHED_ENDPOINT);
  });

  it('keeps the real path for every other status, including other 4xx', () => {
    expect(goldEndpoint('/v1/forecast', 200)).toBe('/v1/forecast');
    expect(goldEndpoint('/v1/forecast', 401)).toBe('/v1/forecast');
    expect(goldEndpoint('/v1/forecast', 500)).toBe('/v1/forecast');
  });
});
