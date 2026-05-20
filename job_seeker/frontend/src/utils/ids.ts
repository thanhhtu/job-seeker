import { STORAGE_KEYS } from "@/constant/storage";

export function getGuestId(): string {
  const key = STORAGE_KEYS.guestId;
  const existing = localStorage.getItem(key);
  if (existing) {
    return existing;
  }

  const next = crypto.randomUUID();
  localStorage.setItem(key, next);
  
  return next;
}

export function getGuestSessionId(): string | null {
  return localStorage.getItem(STORAGE_KEYS.guestSessionId);
}

export function setGuestSessionId(sessionId: string): void {
  localStorage.setItem(STORAGE_KEYS.guestSessionId, sessionId);
}

export function clearGuestSessionId(): void {
  localStorage.removeItem(STORAGE_KEYS.guestSessionId);
}