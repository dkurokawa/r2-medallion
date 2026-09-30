/**
 * Exact percentile over a fully-merged, ascending-sorted array — the "nearest
 * rank" method. Chosen over a histogram/approximation because the day's total
 * row count (6,878–32,557 as of 2026-09) is small enough to sort in one shot
 * (see workflow.ts `gold` step), so there is no reason to trade accuracy for
 * memory here.
 */
export function percentile(sortedAscending: readonly number[], p: number): number {
  const n = sortedAscending.length;
  if (n === 0) return 0;
  const rank = Math.ceil((p / 100) * n);
  const index = Math.min(Math.max(rank - 1, 0), n - 1);
  return sortedAscending[index];
}
