import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { useNavigate, useParams } from "react-router-dom";
import { toast } from "react-hot-toast";
import { ChatArea } from "@/components/ChatArea";
import { Sidebar } from "@/components/Sidebar";
import {
  ApiError,
  AuthResponse,
  getSessionMessages,
  listSessions,
  sendMessage,
  updateSessionTitle,
} from "@/api";
import { useAuth } from "@/hooks/useAuth";
import { ChatMessage } from "@/types/chat";
import { SessionSummary } from "@/types/session";
import {
  clearGuestSessionId,
  getGuestId,
  getGuestSessionId,
  setGuestSessionId as persistGuestSessionId,
} from "@/utils/ids";
import { colors } from "@/theme/colors";
import { loadHiddenSessionIds, persistHiddenSession } from "@/utils/sessionPrefs";

export function ChatPage() {
  const { accessToken, user, isBootstrapping, login, logout } = useAuth();
  const { sessionId: urlSessionId } = useParams<{ sessionId?: string }>();
  const navigate = useNavigate();
  const currentSessionId = urlSessionId ?? null;

  const [guestSessionId, setGuestSessionIdState] = useState<string | null>(() => getGuestSessionId());
  const activeChatSessionId = accessToken ? currentSessionId : guestSessionId;

  const [sessions, setSessions] = useState<SessionSummary[]>([]);
  const [loadingSessions, setLoadingSessions] = useState(false);
  const [messages, setMessages] = useState<ChatMessage[]>([]);
  const [loadedSessionId, setLoadedSessionId] = useState<string | null>(null);
  const [input, setInput] = useState("");
  const [isSending, setIsSending] = useState(false);
  const ignoreInputChange = useRef(false);

  const handleInputChange = useCallback((value: string) => {
    if (ignoreInputChange.current) {
      return;
    }
    setInput(value);
  }, []);

  const [hiddenSessionIds, setHiddenSessionIds] = useState<Set<string>>(loadHiddenSessionIds);

  const visibleSessions = useMemo(
    () => sessions.filter((s) => !hiddenSessionIds.has(s.session_id)),
    [sessions, hiddenSessionIds]
  );

  const fetchSessions = useCallback(async () => {
    if (!accessToken) {
      setSessions([]);
      return;
    }
    setLoadingSessions(true);
    try {
      const data = await listSessions();
      setSessions(data);
    } catch (err) {
      if (err instanceof ApiError && err.status === 401) {
        logout();
        return;
      }
      if (err instanceof ApiError) {
        toast.error(err.message);
      }
    } finally {
      setLoadingSessions(false);
    }
  }, [accessToken, logout]);

  useEffect(() => {
    if (!isBootstrapping) {
      void fetchSessions();
    }
  }, [fetchSessions, isBootstrapping]);

  useEffect(() => {
    if (isBootstrapping) {
      return;
    }

    if (!accessToken) {
      if (urlSessionId) {
        navigate("/chat", { replace: true });
      }
      setMessages([]);
      return;
    }

    if (!urlSessionId) {
      setMessages([]);
      setLoadedSessionId(null);
      return;
    }

    setLoadedSessionId(null);
    let cancelled = false;
    void (async () => {
      try {
        const msgs = await getSessionMessages(urlSessionId);
        if (!cancelled) {
          setMessages(msgs);
          setLoadedSessionId(urlSessionId);
        }
      } catch (err) {
        if (cancelled) {
          return;
        }
        if (err instanceof ApiError && err.status === 401) {
          logout();
          return;
        }
        if (err instanceof ApiError && (err.status === 404 || err.status === 403)) {
          toast.error(err.message);
          setMessages([]);
          navigate("/chat", { replace: true });
          return;
        }
        if (err instanceof ApiError) {
          toast.error(err.message);
        }
      }
    })();

    return () => {
      cancelled = true;
    };
  }, [urlSessionId, accessToken, isBootstrapping, logout, navigate]);

  const handleRenameSession = async (sessionId: string, title: string) => {
    try {
      const updated = await updateSessionTitle(sessionId, title);
      setSessions((prev) =>
        prev.map((s) => (s.session_id === sessionId ? { ...s, title: updated.title } : s))
      );
    } catch (err) {
      if (err instanceof ApiError && err.status === 401) {
        logout();
        return;
      }
      if (err instanceof ApiError) {
        toast.error(err.message);
      }
    }
  };

  const handleDeleteSession = (sessionId: string) => {
    persistHiddenSession(sessionId);
    setHiddenSessionIds((prev) => new Set([...prev, sessionId]));
    if (currentSessionId === sessionId) {
      navigate("/chat");
    }
  };

  const handleSelectSession = (id: string) => {
    if (!accessToken) {
      return;
    }
    navigate(`/chat/${id}`);
  };

  const handleSend = async (text: string) => {
    if (!text || isSending) {
      return;
    }

    ignoreInputChange.current = true;
    setInput("");
    setIsSending(true);
    queueMicrotask(() => {
      ignoreInputChange.current = false;
    });

    const optimistic: ChatMessage = { role: "user", content: text };
    setMessages((prev) => [...prev, optimistic]);

    try {
      const sessionKnownForUser =
        urlSessionId != null && sessions.some((s) => s.session_id === urlSessionId);
      const activeSessionId = accessToken
        ? urlSessionId && (loadedSessionId === urlSessionId || sessionKnownForUser)
          ? urlSessionId
          : null
        : guestSessionId;

      const data = await sendMessage({
        message: text,
        sessionId: activeSessionId,
        ...(accessToken ? {} : { token: null, guestId: getGuestId() }),
      });
      setMessages((prev) => [
        ...prev.slice(0, -1),
        optimistic,
        { role: "assistant", content: data.assistant_message },
      ]);
      if (accessToken) {
        if (data.session_id !== currentSessionId) {
          navigate(`/chat/${data.session_id}`, { replace: true });
        }
        void fetchSessions();
      } else if (data.session_id !== guestSessionId) {
        persistGuestSessionId(data.session_id);
        setGuestSessionIdState(data.session_id);
      }
    } catch (err) {
      setMessages((prev) => prev.slice(0, -1));
      setInput(text);
      if (err instanceof ApiError && err.status === 401) {
        logout();
        return;
      }
      if (!accessToken && err instanceof ApiError && (err.status === 403 || err.status === 404)) {
        clearGuestSessionId();
        setGuestSessionIdState(null);
      }
      if (err instanceof ApiError) {
        toast.error(err.message);
      }
    } finally {
      setIsSending(false);
    }
  };

  const handleLogin = (data: AuthResponse) => {
    setMessages([]);
    setInput("");
    login(data);
    navigate("/chat");
  };

  const handleLogout = () => {
    logout();
    setSessions([]);
    setMessages([]);
    navigate("/chat");
  };

  const handleNewChat = () => {
    if (!accessToken) {
      clearGuestSessionId();
      setGuestSessionIdState(null);
      setMessages([]);
    }
    navigate("/chat");
  };

  if (isBootstrapping) {
    return (
      <div className={`flex h-full w-full items-center justify-center ${colors.page.shellBg}`}>
        <p className={colors.neutral.text500}>Đang tải…</p>
      </div>
    );
  }

  return (
    <div className={`flex h-full w-full ${colors.page.shellBg} !p-4 md:p-4 gap-4 overflow-hidden font-sans`}>
      <div
        className={`w-[320px] shrink-0 ${colors.basic.bgWhite} rounded-[28px] border ${colors.neutral.border100} flex flex-col overflow-y-hidden overflow-x-visible`}
        style={{ boxShadow: "rgba(0, 0, 0, 0.09) 0px 3px 12px" }}
      >
        <Sidebar
          token={accessToken}
          user={user}
          sessions={visibleSessions}
          currentSessionId={activeChatSessionId}
          loadingSessions={loadingSessions}
          onLogin={handleLogin}
          onLogout={handleLogout}
          onRefreshSessions={() => void fetchSessions()}
          onSelectSession={(id) => void handleSelectSession(id)}
          onRenameSession={handleRenameSession}
          onDeleteSession={handleDeleteSession}
          onNewChat={handleNewChat}
        />
      </div>
      <div className={`flex-1 ${colors.basic.bgWhite} rounded-[28px] flex flex-col overflow-hidden`}>
        <ChatArea
          messages={messages}
          sessionId={activeChatSessionId}
          input={input}
          isSending={isSending}
          onInputChange={handleInputChange}
          onSend={handleSend}
        />
      </div>
    </div>
  );
}
