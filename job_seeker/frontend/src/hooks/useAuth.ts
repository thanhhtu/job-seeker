import { useCallback, useEffect, useRef, useState } from "react";
import { toast } from "react-hot-toast";
import { AuthResponse, refreshTokens } from "@/api/auth";
import { setAuthHandlers } from "@/api/client";
import {
  clearAuthStorage,
  loadAuthStorage,
  saveAuthStorage,
  type StoredAuth,
} from "@/utils/authStorage";
import { UserInfo } from "@/types/user";
import { AUTH_LOGOUT_EVENT } from "@/constant/auth";

function toStoredAuth(data: AuthResponse): StoredAuth {
  return {
    accessToken: data.access_token,
    refreshToken: data.refresh_token,
    user: data.user,
  };
}

export function useAuth() {
  const [accessToken, setAccessToken] = useState<string | null>(null);
  const [refreshToken, setRefreshToken] = useState<string | null>(null);
  const [user, setUser] = useState<UserInfo | null>(null);
  const [isBootstrapping, setIsBootstrapping] = useState(true);

  const refreshTokenRef = useRef<string | null>(null);
  const refreshPromiseRef = useRef<Promise<string | null> | null>(null);

  useEffect(() => {
    refreshTokenRef.current = refreshToken;
  }, [refreshToken]);

  const applyAuth = useCallback((data: AuthResponse) => {
    const stored = toStoredAuth(data);
    saveAuthStorage(stored);
    setAccessToken(stored.accessToken);
    setRefreshToken(stored.refreshToken);
    setUser(stored.user);
  }, []);

  const resetAuthState = useCallback(() => {
    setAccessToken(null);
    setRefreshToken(null);
    setUser(null);
    refreshPromiseRef.current = null;
  }, []);

  const logout = useCallback(() => {
    clearAuthStorage();
    resetAuthState();
    // Notify every other useAuth() instance to drop its auth state too,
    // so the whole app returns to the logged-out UI at once.
    window.dispatchEvent(new Event(AUTH_LOGOUT_EVENT));
  }, [resetAuthState]);

  useEffect(() => {
    const onGlobalLogout = () => {
      clearAuthStorage();
      resetAuthState();
    };
    window.addEventListener(AUTH_LOGOUT_EVENT, onGlobalLogout);
    return () => window.removeEventListener(AUTH_LOGOUT_EVENT, onGlobalLogout);
  }, [resetAuthState]);

  const refreshAccessToken = useCallback(async (): Promise<string | null> => {
    if (refreshPromiseRef.current) {
      return refreshPromiseRef.current;
    }

    const run = async (): Promise<string | null> => {
      const rt = refreshTokenRef.current ?? loadAuthStorage()?.refreshToken;
      if (!rt) {
        logout();
        return null;
      }

      try {
        const data = await refreshTokens(rt);
        applyAuth(data);
        return data.access_token;
      } catch {
        logout();
        toast.error("Phiên đăng nhập đã hết hạn. Vui lòng đăng nhập lại.");
        return null;
      }
    };

    refreshPromiseRef.current = run().finally(() => {
      refreshPromiseRef.current = null;
    });

    return refreshPromiseRef.current;
  }, [applyAuth, logout]);

  const getValidAccessToken = useCallback(async (): Promise<string | null> => {
    return accessToken;
  }, [accessToken]);

  const login = useCallback(
    (data: AuthResponse) => {
      applyAuth(data);
    },
    [applyAuth]
  );

  const updateUser = useCallback((next: UserInfo) => {
    setUser(next);
    const stored = loadAuthStorage();
    if (stored) {
      saveAuthStorage({ ...stored, user: next });
    }
  }, []);

  useEffect(() => {
    const stored = loadAuthStorage();
    if (!stored) {
      setIsBootstrapping(false);
      return;
    }

    setUser(stored.user);
    setRefreshToken(stored.refreshToken || null);
    refreshTokenRef.current = stored.refreshToken || null;

    if (stored.accessToken) {
      setAccessToken(stored.accessToken);
      setIsBootstrapping(false);
      return;
    }

    if (stored.refreshToken) {
      void refreshAccessToken().finally(() => setIsBootstrapping(false));
      return;
    }

    logout();
    setIsBootstrapping(false);
  }, [logout, refreshAccessToken]);

  useEffect(() => {
    setAuthHandlers({
      getAccessToken: getValidAccessToken,
      refreshAccessToken,
    });
    return () => setAuthHandlers(null);
  }, [getValidAccessToken, refreshAccessToken]);

  return {
    accessToken,
    user,
    isAuthenticated: Boolean(accessToken && user),
    isBootstrapping,
    login,
    logout,
    updateUser,
    getValidAccessToken,
  };
}
