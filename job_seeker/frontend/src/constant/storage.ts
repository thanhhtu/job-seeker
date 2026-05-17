export const STORAGE_KEYS = {
  token: "job_seeker_access_token",
  refreshToken: "job_seeker_refresh_token",
  user: "job_seeker_user",
  sessionTitles: "job_seeker_session_titles",
  hiddenSessions: "job_seeker_hidden_sessions",
} as const;

/** Keys cleared on logout (auth only). */
export const AUTH_STORAGE_KEYS = [
  STORAGE_KEYS.token,
  STORAGE_KEYS.refreshToken,
  STORAGE_KEYS.user,
] as const;
