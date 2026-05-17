import { env } from "@/config/env";
import { parseError } from "@/utils/error";
import { translateApiMessage } from "@/utils/translateError";

export class ApiError extends Error {
  constructor(public readonly status: number, message: string) {
    super(message);
    this.name = "ApiError";
  }
}

type FetchOptions = Omit<RequestInit, "headers"> & {
  token?: string | null;
  headers?: Record<string, string>;
  /** Skip attaching Bearer token and 401 refresh retry (auth endpoints). */
  skipAuth?: boolean;
  _retried?: boolean;
};

type TokenProvider = () => Promise<string | null>;
type RefreshHandler = () => Promise<string | null>;

let tokenProvider: TokenProvider | null = null;
let refreshHandler: RefreshHandler | null = null;

export function setAuthHandlers(handlers: {
  getAccessToken: TokenProvider;
  refreshAccessToken: RefreshHandler;
} | null): void {
  if (!handlers) {
    tokenProvider = null;
    refreshHandler = null;
    return;
  }
  tokenProvider = handlers.getAccessToken;
  refreshHandler = handlers.refreshAccessToken;
}

export async function apiFetch(path: string, options: FetchOptions = {}): Promise<Response> {
  const { token: explicitToken, headers: extraHeaders, skipAuth, _retried, ...init } = options;

  const headers: Record<string, string> = {
    "Content-Type": "application/json",
    ...extraHeaders,
  };

  let token = explicitToken;
  if (!skipAuth && token === undefined && tokenProvider) {
    token = await tokenProvider();
  }
  if (token) headers["Authorization"] = `Bearer ${token}`;

  const res = await fetch(`${env.apiUrl}${path}`, { ...init, headers });

  if (
    res.status === 401 &&
    !skipAuth &&
    !_retried &&
    refreshHandler &&
    token
  ) {
    const newToken = await refreshHandler();
    if (newToken) {
      return apiFetch(path, {
        ...options,
        token: newToken,
        _retried: true,
      });
    }
  }

  if (!res.ok) {
    const msg = translateApiMessage(await parseError(res));
    throw new ApiError(res.status, msg);
  }

  return res;
}
