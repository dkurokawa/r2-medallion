import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import handler, { HEALTH_CHECK_CRON } from '../index';
import { manifestKey } from '../lib/keys';
import { buildManifest, type Manifest } from '../lib/manifest';
import type { Env } from '../types';

const RUN_CRON = '0 1 * * *';
// 2026-09-25T02:00:00Z (health check) and 2026-09-25T01:00:00Z (run) both
// fall on the same UTC calendar day, so `yesterdayUTC` resolves both to the
// same dt — the health check is meant to re-check the day the run just
// processed an hour earlier.
const SCHEDULED_TIME_HEALTH = Date.UTC(2026, 8, 25, 2, 0, 0);
const SCHEDULED_TIME_RUN = Date.UTC(2026, 8, 25, 1, 0, 0);
const DT = '2026-09-24';

class FakeCtx {
  waitUntilPromises: Promise<unknown>[] = [];
  waitUntil(p: Promise<unknown>): void {
    this.waitUntilPromises.push(p);
  }
  passThroughOnException(): void {}
  props = undefined;
}

function fakeR2(manifest: Manifest | null): Env['DATALAKE_R2'] {
  return {
    get: async () => {
      if (manifest === null) return null;
      return { json: async () => manifest } as unknown;
    },
  } as unknown as Env['DATALAKE_R2'];
}

function makeEnv(overrides: Partial<Env> = {}): Env {
  return {
    DATALAKE_R2: fakeR2(null),
    MEDALLION_WORKFLOW: {
      create: vi.fn(async ({ id }: { id: string }) => ({
        id,
        status: async () => ({ status: 'queued' }),
      })),
      get: vi.fn(),
    } as unknown as Env['MEDALLION_WORKFLOW'],
    SILVER_STREAM: { send: async () => {} },
    GOLD_STREAM: { send: async () => {} },
    ADMIN_TOKEN: 'test-admin-token',
    ENVIRONMENT: 'test',
    R2_SQL_ACCOUNT_ID: 'acct123',
    DISCORD_WEBHOOK_URL: 'https://discord.example/webhook',
    ...overrides,
  };
}

const BASE_MANIFEST_INPUT = {
  dt: DT,
  startedAt: '2026-09-25T01:00:00.000Z',
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

describe('scheduled handler — health-check cron', () => {
  let fetchMock: ReturnType<typeof vi.fn>;

  beforeEach(() => {
    fetchMock = vi.fn(async () => ({ ok: true }) as Response);
    vi.stubGlobal('fetch', fetchMock);
  });

  afterEach(() => {
    vi.unstubAllGlobals();
  });

  it('missing manifest for dt → calls the Discord webhook', async () => {
    const env = makeEnv({ DATALAKE_R2: fakeR2(null) });
    const ctx = new FakeCtx();
    const event = {
      cron: HEALTH_CHECK_CRON,
      scheduledTime: SCHEDULED_TIME_HEALTH,
    } as unknown as ScheduledEvent;

    await handler.scheduled(event, env, ctx as unknown as ExecutionContext);
    await Promise.all(ctx.waitUntilPromises);

    expect(fetchMock).toHaveBeenCalledTimes(1);
    expect(fetchMock).toHaveBeenCalledWith(
      'https://discord.example/webhook',
      expect.objectContaining({ method: 'POST' }),
    );
    const body = JSON.parse((fetchMock.mock.calls[0][1] as RequestInit).body as string);
    expect(body.content).toContain(DT);
    expect(body.content).toContain('health-check');
  });

  it('good manifest (status=done, verify matches) → does not call the webhook', async () => {
    const manifest: Manifest = {
      ...buildManifest(BASE_MANIFEST_INPUT),
      verify: {
        silverRows: 9,
        silverDistinct: 9,
        goldRows: 3,
        goldDistinct: 3,
        checkedAt: '2026-09-25T01:06:00.000Z',
      },
    };
    const env = makeEnv({ DATALAKE_R2: fakeR2(manifest) });
    const ctx = new FakeCtx();
    const event = {
      cron: HEALTH_CHECK_CRON,
      scheduledTime: SCHEDULED_TIME_HEALTH,
    } as unknown as ScheduledEvent;

    await handler.scheduled(event, env, ctx as unknown as ExecutionContext);
    await Promise.all(ctx.waitUntilPromises);

    expect(fetchMock).not.toHaveBeenCalled();
  });

  it('reads the manifest key for the SAME dt the 01:00 run would have processed', async () => {
    const getSpy = vi.fn(async () => null);
    const env = makeEnv({ DATALAKE_R2: { get: getSpy } as unknown as Env['DATALAKE_R2'] });
    const ctx = new FakeCtx();
    const event = {
      cron: HEALTH_CHECK_CRON,
      scheduledTime: SCHEDULED_TIME_HEALTH,
    } as unknown as ScheduledEvent;

    await handler.scheduled(event, env, ctx as unknown as ExecutionContext);
    await Promise.all(ctx.waitUntilPromises);

    expect(getSpy).toHaveBeenCalledWith(manifestKey(DT));
  });
});

describe('scheduled handler — 01:00 run cron', () => {
  it('still creates a workflow instance for yesterday (unchanged behavior)', async () => {
    const env = makeEnv();
    const ctx = new FakeCtx();
    const event = {
      cron: RUN_CRON,
      scheduledTime: SCHEDULED_TIME_RUN,
    } as unknown as ScheduledEvent;

    await handler.scheduled(event, env, ctx as unknown as ExecutionContext);
    await Promise.all(ctx.waitUntilPromises);

    expect(env.MEDALLION_WORKFLOW.create).toHaveBeenCalledWith({
      id: `day-${DT}`,
      params: { dt: DT },
    });
  });

  it('does not touch the Discord webhook path', async () => {
    const fetchMock = vi.fn();
    vi.stubGlobal('fetch', fetchMock);
    const env = makeEnv();
    const ctx = new FakeCtx();
    const event = {
      cron: RUN_CRON,
      scheduledTime: SCHEDULED_TIME_RUN,
    } as unknown as ScheduledEvent;

    await handler.scheduled(event, env, ctx as unknown as ExecutionContext);
    await Promise.all(ctx.waitUntilPromises);

    expect(fetchMock).not.toHaveBeenCalled();
    vi.unstubAllGlobals();
  });
});
