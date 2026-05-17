/** Decode JWT payload without verification (client-side expiry check only). */
export function decodeJwtPayload(token: string): Record<string, unknown> | null {
  try {
    const part = token.split(".")[1];
    if (!part) return null;
    const json = atob(part.replace(/-/g, "+").replace(/_/g, "/"));
    return JSON.parse(json) as Record<string, unknown>;
  } catch {
    return null;
  }
}

export function getJwtExpiryMs(token: string): number | null {
  const payload = decodeJwtPayload(token);
  const exp = payload?.exp;
  if (typeof exp !== "number") return null;
  return exp * 1000;
}

/** True if token is expired or will expire within skewSeconds. */
export function isTokenExpired(token: string, skewSeconds = 30): boolean {
  const expMs = getJwtExpiryMs(token);
  if (expMs === null) return true;
  return Date.now() >= expMs - skewSeconds * 1000;
}
