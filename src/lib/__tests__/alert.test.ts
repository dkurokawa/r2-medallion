import { describe, expect, it, vi } from 'vitest';
import { formatAlert, manifestProblems, notifyDiscord } from '../alert';
import { buildManifest, type Manifest } from '../manifest';

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

function doneManifestWithVerify(verify: Manifest['verify']): Manifest {
  return { ...buildManifest(BASE), verify };
}

describe('manifestProblems', () => {
  it('null manifest → one problem saying the workflow did not complete', () => {
    const problems = manifestProblems(null);
    expect(problems).toHaveLength(1);
    expect(problems[0]).toMatch(/manifest/);
  });

  it('status=done with matching verify counts → no problems', () => {
    const m = doneManifestWithVerify({
      silverRows: 9,
      silverDistinct: 9,
      goldRows: 3,
      goldDistinct: 3,
      checkedAt: '2026-09-22T01:06:00.000Z',
    });
    expect(manifestProblems(m)).toEqual([]);
  });

  it('status=duplicates → problem mentions the status', () => {
    const m: Manifest = { ...buildManifest(BASE), status: 'duplicates' };
    const problems = manifestProblems(m);
    expect(problems.some((p) => p.includes('duplicates'))).toBe(true);
  });

  it('status=mismatch → problem mentions the status', () => {
    const m: Manifest = { ...buildManifest(BASE), status: 'mismatch' };
    const problems = manifestProblems(m);
    expect(problems.some((p) => p.includes('mismatch'))).toBe(true);
  });

  it('verify.error → problem includes the error message', () => {
    const m = doneManifestWithVerify({ error: 'fetch failed: network error' });
    const problems = manifestProblems(m);
    expect(problems.some((p) => p.includes('fetch failed: network error'))).toBe(true);
  });

  it('verify.skipped → problem includes the reason', () => {
    const m = doneManifestWithVerify({ skipped: 'no R2_SQL_TOKEN' });
    const problems = manifestProblems(m);
    expect(problems.some((p) => p.includes('no R2_SQL_TOKEN'))).toBe(true);
  });

  it('verify absent → problem says verify not run', () => {
    const m = buildManifest(BASE); // no `verify` set
    const problems = manifestProblems(m);
    expect(problems.some((p) => p.includes('verify'))).toBe(true);
  });
});

describe('formatAlert', () => {
  it('contains dt, source, and each problem', () => {
    const content = formatAlert('2026-09-21', 'workflow', [
      'status が mismatch',
      'verify がエラー: x',
    ]);
    expect(content).toContain('2026-09-21');
    expect(content).toContain('workflow');
    expect(content).toContain('status が mismatch');
    expect(content).toContain('verify がエラー: x');
  });

  it('is truncated to at most 1900 chars for huge input', () => {
    const hugeProblems = Array.from({ length: 500 }, (_, i) => `problem number ${i} `.repeat(5));
    const content = formatAlert('2026-09-21', 'health-check', hugeProblems);
    expect(content.length).toBeLessThanOrEqual(1900);
  });
});

describe('notifyDiscord', () => {
  it('no URL → returns false and fetch is not called', async () => {
    const fetchImpl = vi.fn();
    const result = await notifyDiscord(undefined, 'hello', fetchImpl as unknown as typeof fetch);
    expect(result).toBe(false);
    expect(fetchImpl).not.toHaveBeenCalled();
  });

  it('ok response → returns true and posts the correct body', async () => {
    const fetchImpl = vi.fn(async (_url: string, init: RequestInit) => {
      expect(init.method).toBe('POST');
      expect((init.headers as Record<string, string>)['Content-Type']).toBe('application/json');
      expect(JSON.parse(init.body as string)).toEqual({
        content: 'hello',
        allowed_mentions: { parse: [] },
      });
      return { ok: true } as Response;
    });
    const result = await notifyDiscord(
      'https://discord.example/webhook',
      'hello',
      fetchImpl as unknown as typeof fetch,
    );
    expect(result).toBe(true);
    expect(fetchImpl).toHaveBeenCalledWith(
      'https://discord.example/webhook',
      expect.objectContaining({ method: 'POST' }),
    );
  });

  it('non-ok response → returns false', async () => {
    const fetchImpl = vi.fn(async () => ({ ok: false }) as Response);
    const result = await notifyDiscord(
      'https://discord.example/webhook',
      'hello',
      fetchImpl as unknown as typeof fetch,
    );
    expect(result).toBe(false);
  });

  it('fetch throws → returns false, does not throw', async () => {
    const fetchImpl = vi.fn(async () => {
      throw new Error('network down');
    });
    await expect(
      notifyDiscord(
        'https://discord.example/webhook',
        'hello',
        fetchImpl as unknown as typeof fetch,
      ),
    ).resolves.toBe(false);
  });

  it('always calls console.error with the content first, regardless of webhook outcome', async () => {
    const spy = vi.spyOn(console, 'error').mockImplementation(() => {});
    await notifyDiscord(undefined, 'a problem happened', vi.fn() as unknown as typeof fetch);
    expect(spy).toHaveBeenCalledWith('a problem happened');
    spy.mockRestore();
  });
});
