import { describe, expect, it, vi } from 'vitest';
import { queryR2Sql, queryRowCounts, r2SqlQueryUrl, type R2SqlResponse } from '../r2sql';

const OPTIONS = { accountId: 'acct123', bucket: 'ppn-datalake-bronze', token: 'test-token' };

function fakeFetch(response: R2SqlResponse, status = 200): typeof fetch {
  return vi.fn(async () => {
    return {
      ok: status >= 200 && status < 300,
      status,
      json: async () => response,
    } as unknown as Response;
  }) as unknown as typeof fetch;
}

describe('r2SqlQueryUrl', () => {
  it('builds the documented endpoint shape', () => {
    expect(r2SqlQueryUrl('acct123', 'my-bucket')).toBe(
      'https://api.sql.cloudflarestorage.com/api/v1/accounts/acct123/r2-sql/query/my-bucket',
    );
  });
});

describe('queryR2Sql', () => {
  it('POSTs {query: sql} with a Bearer auth header to the account/bucket URL', async () => {
    const fetchImpl = fakeFetch({
      success: true,
      result: { rows: [{ n: 1 }] },
      errors: [],
    });
    await queryR2Sql({ ...OPTIONS, fetchImpl }, 'SELECT 1 AS n');

    expect(fetchImpl).toHaveBeenCalledTimes(1);
    const [url, init] = (fetchImpl as ReturnType<typeof vi.fn>).mock.calls[0];
    expect(url).toBe(
      'https://api.sql.cloudflarestorage.com/api/v1/accounts/acct123/r2-sql/query/ppn-datalake-bronze',
    );
    expect(init.method).toBe('POST');
    expect(init.headers.Authorization).toBe('Bearer test-token');
    expect(init.headers['Content-Type']).toBe('application/json');
    expect(JSON.parse(init.body)).toEqual({ query: 'SELECT 1 AS n' });
  });

  it('returns the parsed body on success', async () => {
    const body: R2SqlResponse = { success: true, result: { rows: [{ n: 5 }] }, errors: [] };
    const result = await queryR2Sql({ ...OPTIONS, fetchImpl: fakeFetch(body) }, 'SELECT 1');
    expect(result).toEqual(body);
  });

  it('returns the parsed body (does not throw) on a documented {success:false} error response', async () => {
    const body: R2SqlResponse = {
      success: false,
      result: null,
      errors: [{ code: 40003, message: 'table not found' }],
    };
    const result = await queryR2Sql({ ...OPTIONS, fetchImpl: fakeFetch(body, 400) }, 'SELECT 1');
    expect(result).toEqual(body);
  });

  it('throws on an HTTP failure whose body is not the documented {success:false} shape', async () => {
    const fetchImpl = fakeFetch({} as R2SqlResponse, 502);
    await expect(queryR2Sql({ ...OPTIONS, fetchImpl }, 'SELECT 1')).rejects.toThrow(/HTTP 502/);
  });
});

describe('queryRowCounts', () => {
  it('builds the count(*)/count(DISTINCT row_uid) query with the table and dt interpolated', async () => {
    const fetchImpl = fakeFetch({
      success: true,
      result: { rows: [{ n: 100, d: 98 }] },
      errors: [],
    });
    const counts = await queryRowCounts(
      { ...OPTIONS, fetchImpl },
      'silver.api_metrics',
      '2026-09-21',
    );

    expect(counts).toEqual({ total: 100, distinct: 98 });
    const [, init] = (fetchImpl as ReturnType<typeof vi.fn>).mock.calls[0];
    const { query } = JSON.parse(init.body);
    expect(query).toBe(
      "SELECT count(*) AS n, count(DISTINCT row_uid) AS d FROM silver.api_metrics WHERE dt = '2026-09-21'",
    );
  });

  it('rejects a dt that is not YYYY-MM-DD without making a request', async () => {
    const fetchImpl = fakeFetch({ success: true, result: { rows: [] }, errors: [] });
    await expect(
      queryRowCounts({ ...OPTIONS, fetchImpl }, 'silver.api_metrics', "2026-09-21' OR '1'='1"),
    ).rejects.toThrow(/must be YYYY-MM-DD/);
    expect(fetchImpl).not.toHaveBeenCalled();
  });

  it('throws with the error message(s) when the query itself fails', async () => {
    const fetchImpl = fakeFetch({
      success: false,
      result: null,
      errors: [{ code: 40003, message: 'namespace not found' }],
    });
    await expect(
      queryRowCounts({ ...OPTIONS, fetchImpl }, 'silver.api_metrics', '2026-09-21'),
    ).rejects.toThrow(/namespace not found/);
  });

  it('throws when the response has no rows', async () => {
    const fetchImpl = fakeFetch({ success: true, result: { rows: [] }, errors: [] });
    await expect(
      queryRowCounts({ ...OPTIONS, fetchImpl }, 'silver.api_metrics', '2026-09-21'),
    ).rejects.toThrow(/returned no rows/);
  });

  it('throws when n/d are not numeric', async () => {
    const fetchImpl = fakeFetch({
      success: true,
      result: { rows: [{ n: 'not-a-number', d: 3 }] },
      errors: [],
    });
    await expect(
      queryRowCounts({ ...OPTIONS, fetchImpl }, 'silver.api_metrics', '2026-09-21'),
    ).rejects.toThrow(/non-numeric counts/);
  });
});
