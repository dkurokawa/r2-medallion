import type { SilverRow } from '../types';

export interface ParseSuccess {
  ok: true;
  row: SilverRow;
}

export interface ParseFailure {
  ok: false;
  reason: string;
  raw: string;
}

export type ParseResult = ParseSuccess | ParseFailure;

function isFiniteNumber(v: unknown): v is number {
  return typeof v === 'number' && Number.isFinite(v);
}

export function statusClass(status: number): string {
  const bucket = Math.floor(status / 100);
  return bucket >= 1 && bucket <= 5 ? `${bucket}xx` : 'unknown';
}

/** Split an R2 object body into non-empty JSONL lines (an object may hold more than one metric — see `@ppn/metrics-r2` batching). */
export function splitJsonLines(body: string): string[] {
  return body
    .split('\n')
    .map((l) => l.trim())
    .filter((l) => l.length > 0);
}

/**
 * Deterministic id for one bronze line: the R2 object key plus its 0-based
 * line index *within that object's `splitJsonLines(...)` output* — e.g.
 * `api-metrics/2026-08-20/ppn-hub-workers/17/1787248753488-ll6jls.jsonl#0`.
 * The same bronze line always produces the same row_uid, no matter how many
 * times (or which workflow attempt) parses/sends it — see
 * `SilverRow.row_uid` and README "Design decisions §6".
 *
 * `lineIndex` MUST come from enumerating the array `splitJsonLines` returns
 * (reset to 0 at the start of each object), not from a running counter that
 * accumulates across multiple objects in a chunk (that counter exists in
 * `workflow.ts` as `linesParsed`, and does NOT reset per object — using it
 * here would silently produce colliding/non-reproducible uids).
 */
export function silverRowUid(sourceKey: string, lineIndex: number): string {
  return `${sourceKey}#${lineIndex}`;
}

/**
 * Validate + type one bronze JSONL line into a silver row.
 *
 * `service` is taken from the R2 key's path segment (the partition the
 * object was actually listed under), not from the payload's own `service`
 * field — the key is authoritative for where the row lives; a mismatching
 * payload field (if it ever happened) would otherwise silently misfile a row
 * into the wrong service's aggregates.
 *
 * `lineIndex` is this line's 0-based position within its object's
 * `splitJsonLines(...)` output — see `silverRowUid`.
 */
export function parseBronzeLine(
  line: string,
  dt: string,
  service: string,
  sourceKey: string,
  lineIndex: number,
): ParseResult {
  let parsed: unknown;
  try {
    parsed = JSON.parse(line);
  } catch {
    return { ok: false, reason: 'invalid_json', raw: line };
  }
  if (typeof parsed !== 'object' || parsed === null || Array.isArray(parsed)) {
    return { ok: false, reason: 'not_an_object', raw: line };
  }
  const m = parsed as Record<string, unknown>;

  if (typeof m.timestamp !== 'string' || Number.isNaN(Date.parse(m.timestamp))) {
    return { ok: false, reason: 'missing_or_invalid_timestamp', raw: line };
  }
  if (typeof m.endpoint !== 'string' || m.endpoint.length === 0) {
    return { ok: false, reason: 'missing_endpoint', raw: line };
  }
  if (typeof m.method !== 'string' || m.method.length === 0) {
    return { ok: false, reason: 'missing_method', raw: line };
  }
  if (
    !isFiniteNumber(m.status) ||
    !Number.isInteger(m.status) ||
    m.status < 100 ||
    m.status > 599
  ) {
    return { ok: false, reason: 'missing_or_invalid_status', raw: line };
  }
  if (!isFiniteNumber(m.latency_ms) || m.latency_ms < 0) {
    return { ok: false, reason: 'missing_or_invalid_latency_ms', raw: line };
  }
  if (m.request_size !== undefined && m.request_size !== null && !isFiniteNumber(m.request_size)) {
    return { ok: false, reason: 'invalid_request_size', raw: line };
  }
  if (
    m.response_size !== undefined &&
    m.response_size !== null &&
    !isFiniteNumber(m.response_size)
  ) {
    return { ok: false, reason: 'invalid_response_size', raw: line };
  }
  for (const field of ['user_id', 'cf_ray', 'cf_colo', 'error_message'] as const) {
    const v = m[field];
    if (v !== undefined && v !== null && typeof v !== 'string') {
      return { ok: false, reason: `invalid_${field}`, raw: line };
    }
  }

  const row: SilverRow = {
    ts: m.timestamp,
    dt,
    service,
    endpoint: m.endpoint,
    method: m.method,
    status: m.status,
    status_class: statusClass(m.status),
    latency_ms: m.latency_ms,
    request_size: isFiniteNumber(m.request_size) ? m.request_size : null,
    response_size: isFiniteNumber(m.response_size) ? m.response_size : null,
    user_id: typeof m.user_id === 'string' ? m.user_id : null,
    cf_ray: typeof m.cf_ray === 'string' ? m.cf_ray : null,
    cf_colo: typeof m.cf_colo === 'string' ? m.cf_colo : null,
    error_message: typeof m.error_message === 'string' ? m.error_message : null,
    source_key: sourceKey,
    row_uid: silverRowUid(sourceKey, lineIndex),
  };
  return { ok: true, row };
}
