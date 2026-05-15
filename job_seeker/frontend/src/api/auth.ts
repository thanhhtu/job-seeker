import { UserInfo } from "@/types/user";
import { apiFetch } from "./client";

export type AuthResponse = {
  access_token: string;
  user: UserInfo;
};

export async function login(email: string, password: string): Promise<AuthResponse> {
  const res = await apiFetch("/api/auth/login", {
    method: "POST",
    body: JSON.stringify({ email, password }),
  });
  return res.json() as Promise<AuthResponse>;
}

export async function register(email: string, password: string): Promise<AuthResponse> {
  const res = await apiFetch("/api/auth/register", {
    method: "POST",
    body: JSON.stringify({ email, password }),
  });
  return res.json() as Promise<AuthResponse>;
}
