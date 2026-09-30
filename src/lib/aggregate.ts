import type { GoldRow, SilverRow } from '../types';
import { percentile } from './percentile';

/** Per (service, endpoint, method) running aggregate, built up one silver row at a time. */
export interface GroupAgg {
  service: string;
  endpoint: string;
  method: string;
  count: number;
  errors4xx: number;
  errors5xx: number;
  sumLatency: number;
  maxLatency: number;
  colos: string[];
  /**
   * Requests whose `latency_ms` was exactly 0. On Workers, `Date.now()` does
   * not advance until the isolate performs I/O, so a response served without
   * any I/O (static payloads, 404s, early auth rejects) measures as 0 ms. That
   * is not a missing measurement — it is "no I/O time" — but mixing those
   * zeros into percentiles drags p50 toward 0 and hides the latency of the
   * requests that actually did work. They are counted here and excluded from
   * `latencies`, so the percentiles describe measured-I/O requests only.
   */
  zeroLatency: number;
  /** Non-zero latencies seen for this group so far — merged and sorted once, in the `gold` step. */
  latencies: number[];
}

/**
 * The endpoint a row is grouped under in gold. Vulnerability scanners probe
 * hundreds of one-off paths (`/.env`, `/phpinfo.php`, `/wp-login.php`), each of
 * which would otherwise become its own gold row — on 2026-09-21 that noise was
 * the bulk of the 1,070 gold rows for a single day. A 404 path is by definition
 * not an endpoint of the service, so gold groups them all under one label.
 * Silver keeps the raw path, so the individual probes stay investigable.
 */
export const UNMATCHED_ENDPOINT = '(unmatched)';

export function goldEndpoint(endpoint: string, status: number): string {
  return status === 404 ? UNMATCHED_ENDPOINT : endpoint;
}

const GROUP_KEY_SEP = '\u0000';

export function groupKey(service: string, endpoint: string, method: string): string {
  return `${service}${GROUP_KEY_SEP}${endpoint}${GROUP_KEY_SEP}${method}`;
}

/**
 * Deterministic id for a gold row's (dt, service, endpoint, method) group —
 * `<dt>|<service>|<endpoint>|<method>`. Unlike `groupKey` (an in-memory map
 * key, U+0000-separated, never persisted), this is written to the gold
 * table itself, so it uses a readable separator. Same group always produces
 * the same row_uid regardless of how many chunks contributed to it or how
 * many times the `gold` step's send is retried — see `GoldRow.row_uid`.
 */
export function goldRowUid(dt: string, service: string, endpoint: string, method: string): string {
  return `${dt}|${service}|${endpoint}|${method}`;
}

export function newGroup(service: string, endpoint: string, method: string): GroupAgg {
  return {
    service,
    endpoint,
    method,
    count: 0,
    errors4xx: 0,
    errors5xx: 0,
    sumLatency: 0,
    maxLatency: 0,
    colos: [],
    zeroLatency: 0,
    latencies: [],
  };
}

export function addRowToGroup(group: GroupAgg, row: SilverRow): void {
  group.count += 1;
  if (row.status_class === '4xx') group.errors4xx += 1;
  if (row.status_class === '5xx') group.errors5xx += 1;
  if (row.cf_colo && !group.colos.includes(row.cf_colo)) group.colos.push(row.cf_colo);
  if (row.latency_ms === 0) {
    group.zeroLatency += 1;
    return;
  }
  group.sumLatency += row.latency_ms;
  if (row.latency_ms > group.maxLatency) group.maxLatency = row.latency_ms;
  group.latencies.push(row.latency_ms);
}

/** Merge two aggregates for the SAME (service, endpoint, method) group — e.g. from two chunks of the same service. */
export function mergeGroups(a: GroupAgg, b: GroupAgg): GroupAgg {
  return {
    service: a.service,
    endpoint: a.endpoint,
    method: a.method,
    count: a.count + b.count,
    errors4xx: a.errors4xx + b.errors4xx,
    errors5xx: a.errors5xx + b.errors5xx,
    sumLatency: a.sumLatency + b.sumLatency,
    maxLatency: Math.max(a.maxLatency, b.maxLatency),
    colos: Array.from(new Set([...a.colos, ...b.colos])),
    zeroLatency: a.zeroLatency + b.zeroLatency,
    latencies: a.latencies.concat(b.latencies),
  };
}

export function toGoldRow(dt: string, group: GroupAgg): GoldRow {
  const sorted = [...group.latencies].sort((x, y) => x - y);
  const errors = group.errors4xx + group.errors5xx;
  // Percentiles/avg/max describe the measured-I/O requests only (see
  // GroupAgg.zeroLatency). With none of them, there is nothing to average:
  // the latency columns are null rather than a misleading 0.
  const measured = sorted.length;
  return {
    dt,
    service: group.service,
    endpoint: group.endpoint,
    method: group.method,
    requests: group.count,
    errors_4xx: group.errors4xx,
    errors_5xx: group.errors5xx,
    error_rate: group.count > 0 ? errors / group.count : 0,
    requests_measured: measured,
    requests_zero_latency: group.zeroLatency,
    latency_p50: measured > 0 ? percentile(sorted, 50) : null,
    latency_p95: measured > 0 ? percentile(sorted, 95) : null,
    latency_p99: measured > 0 ? percentile(sorted, 99) : null,
    latency_avg: measured > 0 ? group.sumLatency / measured : null,
    latency_max: measured > 0 ? group.maxLatency : null,
    colos: [...group.colos].sort().join(','),
    row_uid: goldRowUid(dt, group.service, group.endpoint, group.method),
  };
}
