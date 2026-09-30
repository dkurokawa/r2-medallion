export interface ManifestAssertions {
  /** total bronze JSONL lines == silver rows + quarantined rows. */
  linesEqualsSilverPlusQuarantine: boolean;
  /** silver rows == sum of gold `requests`. */
  silverEqualsGoldRequests: boolean;
  /**
   * bronze objects enumerated by the `list` step == objects actually read with
   * `get()`, with zero missing. If a `get()` returning `null` (object gone) were
   * silently treated as an empty body, its rows would simply not exist, the other
   * two equations would still hold, and part of bronze would be lost without a
   * trace — this assertion is what catches that.
   */
  objectsListedEqualsRead: boolean;
  /**
   * Set only by the `verify` workflow step (after a successful R2 SQL
   * query) — `true` iff `count(DISTINCT row_uid)` in silver for this `dt`
   * equals `silverRows` (the count THIS run's manifest recorded as sent).
   * `false` means the actual distinct row count diverges from what this run
   * believes it sent — a real correctness problem, not just a resend
   * duplicate — so it forces `status: 'mismatch'`. Absent until the
   * `verify` step has run (skipped, or failed before getting a count).
   */
  silverDistinctEqualsManifest?: boolean;
  /** Same as `silverDistinctEqualsManifest`, for the gold table / `goldRows`. */
  goldDistinctEqualsManifest?: boolean;
}

/**
 * `'duplicates'` is set only by the `verify` step: the distinct row count
 * matches what this run sent (so NOT a `'mismatch'`), but R2 SQL's
 * `count(*)` for the day exceeds `count(DISTINCT row_uid)` — i.e. some rows
 * were durably written more than once (the residual risk documented in
 * README "Design decisions §6": a `send()` that succeeded but whose progress
 * marker write then failed/raced a retry). It is intentionally distinct
 * from `'mismatch'` (which means THIS run's own bookkeeping doesn't
 * reconcile) — `'duplicates'` means the bookkeeping is fine, but the table
 * itself needs a dedupe read (see README's canonical query) until it's
 * compacted.
 */
export type ManifestStatus = 'done' | 'mismatch' | 'duplicates';

/**
 * Raw counts from the `verify` step's two R2 SQL queries. `checkedAt` is
 * this step's own timestamp, distinct from `finishedAt` (the `manifest`
 * step's timestamp, written ~6 minutes earlier — see workflow.ts's
 * `verify-sleep`).
 */
export interface ManifestVerifyCounts {
  /** `count(*)` in silver.api_metrics for this dt (includes duplicates, if any). */
  silverRows: number;
  /** `count(DISTINCT row_uid)` in silver.api_metrics for this dt. */
  silverDistinct: number;
  /** `count(*)` in gold.api_metrics_daily for this dt. */
  goldRows: number;
  /** `count(DISTINCT row_uid)` in gold.api_metrics_daily for this dt. */
  goldDistinct: number;
  checkedAt: string;
}

/** The `verify` step never ran a query — no `R2_SQL_TOKEN` secret was configured. The day still completes; see workflow.ts `runVerify`. */
export interface ManifestVerifySkipped {
  skipped: string;
}

/** The `verify` step tried to query R2 SQL but the request/response itself failed (network, auth, bad shape, ...) — NOT the same as finding duplicates or a mismatch. The day still completes; see workflow.ts `runVerify`. */
export interface ManifestVerifyError {
  error: string;
}

export type ManifestVerify = ManifestVerifyCounts | ManifestVerifySkipped | ManifestVerifyError;

export interface Manifest {
  dt: string;
  status: ManifestStatus;
  startedAt: string;
  finishedAt: string;
  bronzeObjectCount: number;
  bronzeObjectsRead: number;
  bronzeObjectsMissing: number;
  bronzeLinesParsed: number;
  silverRows: number;
  quarantinedRows: number;
  goldRows: number;
  goldRequestsSum: number;
  services: string[];
  assertions: ManifestAssertions;
  /** Absent until the `verify` workflow step runs (it runs after `manifest` — see workflow.ts). */
  verify?: ManifestVerify;
}

export interface ManifestInput {
  dt: string;
  startedAt: string;
  bronzeObjectCount: number;
  bronzeObjectsRead: number;
  bronzeObjectsMissing: number;
  bronzeLinesParsed: number;
  silverRows: number;
  quarantinedRows: number;
  goldRows: number;
  goldRequestsSum: number;
  services: string[];
}

/**
 * Builds the manifest and evaluates the cross-checks that define a
 * completed day. `status: 'mismatch'` signals the workflow's
 * `manifest` step should throw after persisting this (see workflow.ts) —
 * the manifest itself is written either way so the failure is visible from
 * R2 SQL / GET /status without needing to dig through Workflow run logs.
 */
export function buildManifest(input: ManifestInput): Manifest {
  const linesEqualsSilverPlusQuarantine =
    input.bronzeLinesParsed === input.silverRows + input.quarantinedRows;
  const silverEqualsGoldRequests = input.silverRows === input.goldRequestsSum;
  const objectsListedEqualsRead =
    input.bronzeObjectCount === input.bronzeObjectsRead && input.bronzeObjectsMissing === 0;
  const ok = linesEqualsSilverPlusQuarantine && silverEqualsGoldRequests && objectsListedEqualsRead;

  return {
    dt: input.dt,
    status: ok ? 'done' : 'mismatch',
    startedAt: input.startedAt,
    finishedAt: new Date().toISOString(),
    bronzeObjectCount: input.bronzeObjectCount,
    bronzeObjectsRead: input.bronzeObjectsRead,
    bronzeObjectsMissing: input.bronzeObjectsMissing,
    bronzeLinesParsed: input.bronzeLinesParsed,
    silverRows: input.silverRows,
    quarantinedRows: input.quarantinedRows,
    goldRows: input.goldRows,
    goldRequestsSum: input.goldRequestsSum,
    services: input.services,
    assertions: {
      linesEqualsSilverPlusQuarantine,
      silverEqualsGoldRequests,
      objectsListedEqualsRead,
    },
  };
}

/**
 * Applies a successful `verify` step's R2 SQL counts to an already-built
 * manifest, deciding the (possibly new) status. Pure — no I/O, no R2 SQL
 * client dependency — so it's unit-testable independent of the network
 * call. See `ManifestStatus` for the priority between 'mismatch' and
 * 'duplicates': a distinct-count mismatch against this run's own bookkeeping
 * always wins, since it's the more serious of the two problems.
 */
export function applyVerifyCounts(manifest: Manifest, counts: ManifestVerifyCounts): Manifest {
  const silverDistinctEqualsManifest = counts.silverDistinct === manifest.silverRows;
  const goldDistinctEqualsManifest = counts.goldDistinct === manifest.goldRows;
  const hasDuplicates =
    counts.silverRows > counts.silverDistinct || counts.goldRows > counts.goldDistinct;

  const status: ManifestStatus =
    !silverDistinctEqualsManifest || !goldDistinctEqualsManifest
      ? 'mismatch'
      : hasDuplicates
        ? 'duplicates'
        : manifest.status;

  return {
    ...manifest,
    status,
    assertions: {
      ...manifest.assertions,
      silverDistinctEqualsManifest,
      goldDistinctEqualsManifest,
    },
    verify: counts,
  };
}

/** The `verify` step found no `R2_SQL_TOKEN` secret configured — records the skip without touching `status`. */
export function applyVerifySkipped(manifest: Manifest, reason: string): Manifest {
  return { ...manifest, verify: { skipped: reason } };
}

/** The `verify` step's R2 SQL request itself failed — records the error without touching `status` (the day still completes; see workflow.ts `runVerify`). */
export function applyVerifyError(manifest: Manifest, message: string): Manifest {
  return { ...manifest, verify: { error: message } };
}
