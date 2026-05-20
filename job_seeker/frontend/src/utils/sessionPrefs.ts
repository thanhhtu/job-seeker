import { STORAGE_KEYS } from "@/constant/storage";

export function loadSessionTitles(): Record<string, string> {
  try {
    const raw = localStorage.getItem(STORAGE_KEYS.sessionTitles);
    if (!raw) {
      return {};
    }
    const parsed = JSON.parse(raw) as unknown;
    if (parsed && typeof parsed === "object" && !Array.isArray(parsed)) {
      return parsed as Record<string, string>;
    }
  } catch {
    /* ignore */
  }
  return {};
}

export function persistSessionTitle(sessionId: string, title: string): void {
  const titles = loadSessionTitles();
  titles[sessionId] = title.trim();
  localStorage.setItem(STORAGE_KEYS.sessionTitles, JSON.stringify(titles));
}

export function loadHiddenSessionIds(): Set<string> {
  try {
    const raw = localStorage.getItem(STORAGE_KEYS.hiddenSessions);
    if (!raw) {
      return new Set();
    }
    const parsed = JSON.parse(raw) as unknown;
    if (Array.isArray(parsed)) {
      return new Set(parsed.filter((id): id is string => typeof id === "string"));
    }
  } catch {
    /* ignore */
  }
  return new Set();
}

export function persistHiddenSession(sessionId: string): void {
  const hidden = loadHiddenSessionIds();
  hidden.add(sessionId);
  localStorage.setItem(STORAGE_KEYS.hiddenSessions, JSON.stringify([...hidden]));
}
