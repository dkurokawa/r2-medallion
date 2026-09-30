import { describe, expect, it } from 'vitest';
import { timingSafeEqual, verifyBearerToken } from '../auth';

describe('timingSafeEqual', () => {
  it('true for equal strings', async () => {
    expect(await timingSafeEqual('abc123', 'abc123')).toBe(true);
  });

  it('false for different strings of the same length', async () => {
    expect(await timingSafeEqual('abc123', 'abc124')).toBe(false);
  });

  it('false for different lengths', async () => {
    expect(await timingSafeEqual('short', 'a-lot-longer-string')).toBe(false);
  });

  it('false against an empty string, true for two empty strings', async () => {
    expect(await timingSafeEqual('nonempty', '')).toBe(false);
    expect(await timingSafeEqual('', '')).toBe(true);
  });
});

describe('verifyBearerToken', () => {
  const TOKEN = 'super-secret-token';

  function reqWith(header?: string): Request {
    const headers = new Headers();
    if (header !== undefined) headers.set('Authorization', header);
    return new Request('https://example.com/run?dt=2026-09-21', { headers });
  }

  it('accepts a correct bearer token', async () => {
    expect(await verifyBearerToken(reqWith(`Bearer ${TOKEN}`), TOKEN)).toBe(true);
  });

  it('is case-insensitive on the "Bearer" scheme', async () => {
    expect(await verifyBearerToken(reqWith(`bearer ${TOKEN}`), TOKEN)).toBe(true);
  });

  it('rejects a missing Authorization header', async () => {
    expect(await verifyBearerToken(reqWith(undefined), TOKEN)).toBe(false);
  });

  it('rejects a non-Bearer scheme', async () => {
    expect(await verifyBearerToken(reqWith(`Basic ${TOKEN}`), TOKEN)).toBe(false);
  });

  it('rejects a wrong token', async () => {
    expect(await verifyBearerToken(reqWith('Bearer wrong'), TOKEN)).toBe(false);
  });

  it('rejects when the expected token is empty (misconfiguration must fail closed)', async () => {
    expect(await verifyBearerToken(reqWith('Bearer '), '')).toBe(false);
    expect(await verifyBearerToken(reqWith('Bearer anything'), '')).toBe(false);
  });
});
