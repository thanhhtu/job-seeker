import { AUTH_STORAGE_KEYS, STORAGE_KEYS } from "@/constant/storage";
import { UserInfo } from "@/types/user";

export type StoredAuth = {
  accessToken: string;
  refreshToken: string;
  user: UserInfo;
};

export function clearAuthStorage(): void {
  for (const key of AUTH_STORAGE_KEYS) {
    localStorage.removeItem(key);
  }
}

export function saveAuthStorage(auth: StoredAuth): void {
  localStorage.setItem(STORAGE_KEYS.token, auth.accessToken);
  localStorage.setItem(STORAGE_KEYS.refreshToken, auth.refreshToken);
  localStorage.setItem(STORAGE_KEYS.user, JSON.stringify(auth.user));
}

export function loadAuthStorage(): StoredAuth | null {
  const accessToken = localStorage.getItem(STORAGE_KEYS.token);
  const refreshToken = localStorage.getItem(STORAGE_KEYS.refreshToken);
  const rawUser = localStorage.getItem(STORAGE_KEYS.user);

  if (!accessToken && !refreshToken && !rawUser) return null;

  if (!rawUser) {
    clearAuthStorage();
    return null;
  }

  try {
    const user = JSON.parse(rawUser) as UserInfo;
    if (!user?.id || !user?.email) {
      clearAuthStorage();
      return null;
    }
    return {
      accessToken: accessToken ?? "",
      refreshToken: refreshToken ?? "",
      user,
    };
  } catch {
    clearAuthStorage();
    return null;
  }
}
