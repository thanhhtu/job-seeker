import { UserInfo } from "@/types/user";
import { apiFetch } from "./client";

export type AuthResponse = {
  access_token: string;
  refresh_token: string;
  expires_in: number;
  user: UserInfo;
};

export async function login(email: string, password: string): Promise<AuthResponse> {
  const res = await apiFetch("/api/auth/login", {
    method: "POST",
    body: JSON.stringify({ email, password }),
    skipAuth: true,
  });
  return res.json() as Promise<AuthResponse>;
}

export async function register(email: string, password: string): Promise<AuthResponse> {
  const res = await apiFetch("/api/auth/register", {
    method: "POST",
    body: JSON.stringify({ email, password }),
    skipAuth: true,
  });
  return res.json() as Promise<AuthResponse>;
}

export async function refreshTokens(refreshToken: string): Promise<AuthResponse> {
  const res = await apiFetch("/api/auth/refresh", {
    method: "POST",
    body: JSON.stringify({ refresh_token: refreshToken }),
    skipAuth: true,
  });
  return res.json() as Promise<AuthResponse>;
}
