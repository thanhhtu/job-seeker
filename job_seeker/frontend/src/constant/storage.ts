export const STORAGE_KEYS = {
  token: "access_token",
  refreshToken: "refresh_token",
  user: "user",
  sessionTitles: "session_titles",
  hiddenSessions: "hidden_sessions",
  guestId: "guest_id",
  guestSessionId: "guest_session_id",
} as const;

/** Keys cleared on logout (auth only). */
export const AUTH_STORAGE_KEYS = [
  STORAGE_KEYS.token,
  STORAGE_KEYS.refreshToken,
  STORAGE_KEYS.user,
] as const;
