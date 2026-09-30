/**
 * Discord アラート — `MedallionDayWorkflow` の日次処理が「悪い形」で終わった
 * とき（`verify.error` / `status !== 'done'` / manifest そのものが無い）に
 * 気づけるようにする。POST `{ content }` の Discord webhook。
 *
 * 呼び出し元は2つ:
 *  - `workflow.ts` の `alert` ステップ（`verify` ステップ直後、その日の実行内）
 *  - `index.ts` の health-check cron（`verify` ステップ自体が一度も走らなかった
 *    ＝ワークフローが完了しなかった日を、manifest の不在で検知する）
 */

import type { Manifest, ManifestVerify } from './manifest';

/**
 * `manifest` から人が読める問題点の一覧を返す。純粋関数（I/O なし）。
 * 問題が無ければ空配列。
 */
export function manifestProblems(m: Manifest | null): string[] {
  if (m === null) {
    return ['manifest が存在しない（workflow が完了していない可能性がある）'];
  }

  const problems: string[] = [];

  if (m.status !== 'done') {
    problems.push(`status が ${m.status}`);
  }

  const verify: ManifestVerify | undefined = m.verify;
  if (!verify) {
    problems.push('verify が実行されていない');
  } else if ('error' in verify) {
    problems.push(`verify がエラー: ${verify.error}`);
  } else if ('skipped' in verify) {
    problems.push(`verify がスキップされた: ${verify.skipped}`);
  }

  return problems;
}

// Discord の上限は 2000 文字。それに対して余裕を持たせた切り詰め先。
const TRUNCATE_TO = 1900;

/** Discord に投げる本文を組み立てる。長さは常に `TRUNCATE_TO` 以下に切り詰める。 */
export function formatAlert(
  dt: string,
  source: 'workflow' | 'health-check',
  problems: string[],
): string {
  const lines = [
    `:rotating_light: **r2-medallion dt=${dt}** (${source})`,
    ...problems.map((p) => `- ${p}`),
    `確認: GET /status?dt=${dt} / scripts/dedupe_day.py verify-all`,
  ];
  const content = lines.join('\n');
  return content.length > TRUNCATE_TO ? content.slice(0, TRUNCATE_TO) : content;
}

/**
 * Discord webhook へ通知する。**throw しない** — webhook 未設定・fetch 失敗
 * いずれも `false` を返すだけで、呼び出し元（workflow の `alert` ステップ /
 * health-check cron）の処理を止めない。`console.error` は webhook の有無に
 * 関わらず必ず先に呼ぶので、webhook が未設定の環境でも Workers のログには
 * 残る。webhook URL 自体はログに出さない。
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
      // 本文には R2 SQL のエラー文など外から来る文字列が入るので、`@everyone` 等が
      // 混ざってもメンションとして展開させない。
      body: JSON.stringify({ content, allowed_mentions: { parse: [] } }),
    });
    return res.ok;
  } catch {
    return false;
  }
}
