/**
 * Constant-time bearer token check for the admin endpoints (/run, /status).
 * Both sides are SHA-256 hashed before comparison so the loop length does not
 * depend on the secret. An unset expected token rejects everything.
 */
export async function timingSafeEqual(a: string, b: string): Promise<boolean> {
  const encoder = new TextEncoder();
  const aBuffer = encoder.encode(a);
  const bBuffer = encoder.encode(b);

  const maxLength = Math.max(aBuffer.length, bBuffer.length, 1);
  const aPadded = new Uint8Array(maxLength);
  const bPadded = new Uint8Array(maxLength);
  aPadded.set(aBuffer);
  bPadded.set(bBuffer);

  const [aHash, bHash] = await Promise.all([
    crypto.subtle.digest('SHA-256', aPadded),
    crypto.subtle.digest('SHA-256', bPadded),
  ]);
  const aHashArray = new Uint8Array(aHash);
  const bHashArray = new Uint8Array(bHash);

  let result = aBuffer.length === bBuffer.length ? 1 : 0;
  for (let i = 0; i < aHashArray.length; i++) {
    result &= aHashArray[i] === bHashArray[i] ? 1 : 0;
  }
  return result === 1;
}

export async function verifyBearerToken(request: Request, expected: string): Promise<boolean> {
  if (!expected) return false;
  const header = request.headers.get('Authorization');
  if (!header) return false;
  const match = /^Bearer\s+(.+)$/i.exec(header.trim());
  if (!match) return false;
  return timingSafeEqual(match[1], expected);
}
