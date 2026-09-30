import { describe, expect, it } from 'vitest';
import { parseBronzeLine, silverRowUid, splitJsonLines, statusClass } from '../parse';

const DT = '2026-09-21';
const SERVICE = 'weathio';
const KEY = 'api-metrics/2026-09-21/weathio/14/1758470400123-ab12cd.jsonl';

function validLine(overrides: Record<string, unknown> = {}): string {
  return JSON.stringify({
    timestamp: '2026-09-21T14:03:00.000Z',
    service: 'weathio',
    endpoint: '/v1/forecast',
    method: 'GET',
    status: 200,
    latency_ms: 42,
    request_size: 128,
    response_size: 4096,
    user_id: 'u_1',
    cf_ray: '8f0000000000-NRT',
    cf_colo: 'NRT',
    ...overrides,
  });
}

describe('statusClass', () => {
  it('buckets by hundreds', () => {
    expect(statusClass(200)).toBe('2xx');
    expect(statusClass(201)).toBe('2xx');
    expect(statusClass(301)).toBe('3xx');
    expect(statusClass(404)).toBe('4xx');
    expect(statusClass(500)).toBe('5xx');
    expect(statusClass(599)).toBe('5xx');
  });

  it('returns unknown outside 1xx-5xx', () => {
    expect(statusClass(0)).toBe('unknown');
    expect(statusClass(700)).toBe('unknown');
  });
});

describe('splitJsonLines', () => {
  it('drops blank lines and trims', () => {
    expect(splitJsonLines('a\n\n b \n\nc\n')).toEqual(['a', 'b', 'c']);
  });

  it('returns empty array for empty body', () => {
    expect(splitJsonLines('')).toEqual([]);
  });
});

describe('parseBronzeLine', () => {
  it('accepts a well-formed line and derives status_class + source_key, and uses the KEY-derived service', () => {
    const result = parseBronzeLine(validLine(), DT, SERVICE, KEY, 0);
    expect(result.ok).toBe(true);
    if (!result.ok) throw new Error('expected ok');
    expect(result.row).toEqual({
      ts: '2026-09-21T14:03:00.000Z',
      dt: DT,
      service: SERVICE,
      endpoint: '/v1/forecast',
      method: 'GET',
      status: 200,
      status_class: '2xx',
      latency_ms: 42,
      request_size: 128,
      response_size: 4096,
      user_id: 'u_1',
      cf_ray: '8f0000000000-NRT',
      cf_colo: 'NRT',
      error_message: null,
      source_key: KEY,
      row_uid: `${KEY}#0`,
    });
  });

  it('fills optional fields with null when absent', () => {
    const line = JSON.stringify({
      timestamp: '2026-09-21T14:03:00.000Z',
      endpoint: '/v1/forecast',
      method: 'GET',
      status: 200,
      latency_ms: 10,
    });
    const result = parseBronzeLine(line, DT, SERVICE, KEY, 0);
    expect(result.ok).toBe(true);
    if (!result.ok) throw new Error('expected ok');
    expect(result.row.request_size).toBeNull();
    expect(result.row.user_id).toBeNull();
    expect(result.row.error_message).toBeNull();
  });

  it('rejects invalid JSON', () => {
    const result = parseBronzeLine('{not json', DT, SERVICE, KEY, 0);
    expect(result).toEqual({ ok: false, reason: 'invalid_json', raw: '{not json' });
  });

  it('rejects a JSON array (not an object)', () => {
    const result = parseBronzeLine('[1,2,3]', DT, SERVICE, KEY, 0);
    expect(result.ok).toBe(false);
    if (result.ok) throw new Error('expected failure');
    expect(result.reason).toBe('not_an_object');
  });

  for (const field of ['timestamp', 'endpoint', 'method'] as const) {
    it(`rejects missing ${field}`, () => {
      const obj = JSON.parse(validLine());
      delete obj[field];
      const result = parseBronzeLine(JSON.stringify(obj), DT, SERVICE, KEY, 0);
      expect(result.ok).toBe(false);
    });
  }

  it('rejects an invalid timestamp string', () => {
    const result = parseBronzeLine(validLine({ timestamp: 'not-a-date' }), DT, SERVICE, KEY, 0);
    expect(result.ok).toBe(false);
    if (result.ok) throw new Error('expected failure');
    expect(result.reason).toBe('missing_or_invalid_timestamp');
  });

  it('rejects a non-integer / out-of-range status', () => {
    expect(parseBronzeLine(validLine({ status: 200.5 }), DT, SERVICE, KEY, 0).ok).toBe(false);
    expect(parseBronzeLine(validLine({ status: 99 }), DT, SERVICE, KEY, 0).ok).toBe(false);
    expect(parseBronzeLine(validLine({ status: 600 }), DT, SERVICE, KEY, 0).ok).toBe(false);
    expect(parseBronzeLine(validLine({ status: '200' }), DT, SERVICE, KEY, 0).ok).toBe(false);
  });

  it('rejects a negative or non-numeric latency_ms', () => {
    expect(parseBronzeLine(validLine({ latency_ms: -1 }), DT, SERVICE, KEY, 0).ok).toBe(false);
    expect(parseBronzeLine(validLine({ latency_ms: 'fast' }), DT, SERVICE, KEY, 0).ok).toBe(false);
  });

  it('rejects non-string optional string fields', () => {
    expect(parseBronzeLine(validLine({ cf_colo: 123 }), DT, SERVICE, KEY, 0).ok).toBe(false);
    expect(parseBronzeLine(validLine({ user_id: {} }), DT, SERVICE, KEY, 0).ok).toBe(false);
  });

  it('rejects non-numeric request_size/response_size but allows null', () => {
    expect(parseBronzeLine(validLine({ request_size: 'big' }), DT, SERVICE, KEY, 0).ok).toBe(false);
    expect(parseBronzeLine(validLine({ request_size: null }), DT, SERVICE, KEY, 0).ok).toBe(true);
  });
});

describe('silverRowUid / row_uid determinism', () => {
  it('is exactly "<source_key>#<line_index>"', () => {
    expect(silverRowUid(KEY, 0)).toBe(`${KEY}#0`);
    expect(silverRowUid(KEY, 7)).toBe(`${KEY}#7`);
  });

  it('re-parsing the same line at the same index always produces the same row_uid', () => {
    const r1 = parseBronzeLine(validLine(), DT, SERVICE, KEY, 3);
    const r2 = parseBronzeLine(validLine(), DT, SERVICE, KEY, 3);
    expect(r1.ok && r2.ok).toBe(true);
    if (!r1.ok || !r2.ok) throw new Error('expected ok');
    expect(r1.row.row_uid).toBe(r2.row.row_uid);
    expect(r1.row.row_uid).toBe(`${KEY}#3`);
  });

  it('differs by line index for the same object, and by source_key for the same index', () => {
    const idx0 = parseBronzeLine(validLine(), DT, SERVICE, KEY, 0);
    const idx1 = parseBronzeLine(validLine(), DT, SERVICE, KEY, 1);
    if (!idx0.ok || !idx1.ok) throw new Error('expected ok');
    expect(idx0.row.row_uid).not.toBe(idx1.row.row_uid);

    const otherKey = 'api-metrics/2026-09-21/weathio/14/other-object.jsonl';
    const sameIdxOtherKey = parseBronzeLine(validLine(), DT, SERVICE, otherKey, 0);
    if (!sameIdxOtherKey.ok) throw new Error('expected ok');
    expect(idx0.row.row_uid).not.toBe(sameIdxOtherKey.row.row_uid);
  });

  it('re-enumerating splitJsonLines over the same multi-line object body reproduces identical row_uids', () => {
    const body = [
      validLine(),
      validLine({ endpoint: '/other' }),
      validLine({ endpoint: '/third' }),
    ].join('\n');
    const parseAll = () =>
      splitJsonLines(body).map((line, i) => {
        const r = parseBronzeLine(line, DT, SERVICE, KEY, i);
        if (!r.ok) throw new Error('expected ok');
        return r.row.row_uid;
      });
    const first = parseAll();
    const second = parseAll();
    expect(first).toEqual(second);
    expect(first).toEqual([`${KEY}#0`, `${KEY}#1`, `${KEY}#2`]);
  });
});
