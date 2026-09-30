/**
 * Discord alerts — surface a `MedallionDayWorkflow` day that ended badly
 * (`verify.error` / `status !== 'done'` / no manifest at all). Posts
 * `{ content }` to a Discord webhook.
 *
 * Two callers:
 *  - the `alert` step in `workflow.ts` (right after `verify`, inside that day's run)
 *  - the health-check cron in `index.ts` (catches days where `verify` never ran,
 *    i.e. the workflow did not complete, by the manifest being absent)
 */

import type { Manifest, ManifestVerify } from './manifest';

/**
 * Returns a human-readable list of problems in `manifest`. Pure (no I/O).
 * Empty when there is nothing wrong.
 */
export function manifestProblems(m: Manifest | null): string[] {
  if (m === null) {
    return ['manifest is missing (the workflow may not have completed)'];
  }

  const problems: string[] = [];

  if (m.status !== 'done') {
    problems.push(`status is ${m.status}`);
  }

  const verify: ManifestVerify | undefined = m.verify;
  if (!verify) {
    problems.push('verify has not run');
  } else if ('error' in verify) {
    problems.push(`verify failed: ${verify.error}`);
  } else if ('skipped' in verify) {
    problems.push(`verify was skipped: ${verify.skipped}`);
  }

  return problems;
}

// Discord caps messages at 2000 characters; truncate with some headroom.
const TRUNCATE_TO = 1900;

/** Builds the Discord message body, always truncated to at most `TRUNCATE_TO`. */
export function formatAlert(
  dt: string,
  source: 'workflow' | 'health-check',
  problems: string[],
): string {
  const lines = [
    `:rotating_light: **r2-medallion dt=${dt}** (${source})`,
    ...problems.map((p) => `- ${p}`),
    `check: GET /status?dt=${dt} / scripts/dedupe_day.py verify-all`,
  ];
  const content = lines.join('\n');
  return content.length > TRUNCATE_TO ? content.slice(0, TRUNCATE_TO) : content;
}

/**
 * Sends a notification to the Discord webhook. **Never throws** — a missing
 * webhook or a failed fetch just returns `false`, so the caller (the workflow's
 * `alert` step / the health-check cron) keeps going. `console.error` is always
 * called first regardless, so the problem reaches Workers logs even without a
 * webhook. The webhook URL itself is never logged.
 */
export async function notifyDiscord(
  webhookUrl: string | undefined,
  content: string,
  fetchImpl: typeof fetch = fetch,
): Promise<boolean> {
  console.error(content);
  if (!webhookUrl) {
    return false;
  }
  try {
    const res = await fetchImpl(webhookUrl, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      // The body contains external strings (e.g. R2 SQL error messages), so make
      // sure `@everyone` and friends are never expanded as mentions.
      body: JSON.stringify({ content, allowed_mentions: { parse: [] } }),
    });
    return res.ok;
  } catch {
    return false;
  }
}
