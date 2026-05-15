import { env } from "@/config/env";
import { parseError } from "@/utils/error";

export class ApiError extends Error {
  constructor(public readonly status: number, message: string) {
    super(message);
    this.name = "ApiError";
  }
}

type FetchOptions = Omit<RequestInit, "headers"> & {
  token?: string | null;
  headers?: Record<string, string>;
};

export async function apiFetch(path: string, options: FetchOptions = {}): Promise<Response> {
  const { token, headers: extraHeaders, ...init } = options;

  const headers: Record<string, string> = {
    "Content-Type": "application/json",
    ...extraHeaders,
  };
  if (token) headers["Authorization"] = `Bearer ${token}`;

  const res = await fetch(`${env.apiUrl}${path}`, { ...init, headers });

  if (!res.ok) {
    const msg = await parseError(res);
    throw new ApiError(res.status, msg);
  }

  return res;
}
