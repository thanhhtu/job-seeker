import { FormEvent, useCallback, useEffect, useMemo, useState } from "react";
import { Toaster, toast } from "react-hot-toast";
import { ChatArea } from "@/components/ChatArea";
import { Sidebar } from "@/components/Sidebar";
import { ApiError, getSessionMessages, listSessions, sendMessage } from "@/api";
import { ChatMessage } from "@/types/chat";
import { SessionSummary } from "@/types/session";
import { UserInfo } from "@/types/user";
import { getGuestId } from "@/utils/ids";
import { STORAGE_KEYS } from "@/constant/storage";
import { colors } from "@/theme/colors";
import {
  loadHiddenSessionIds,
  loadSessionTitles,
  persistHiddenSession,
  persistSessionTitle,
} from "@/utils/sessionPrefs";

export default function App() {
  const [token, setToken] = useState<string | null>(null);
  const [user, setUser] = useState<UserInfo | null>(null);
  const [sessions, setSessions] = useState<SessionSummary[]>([]);
  const [loadingSessions, setLoadingSessions] = useState(false);
  const [currentSessionId, setCurrentSessionId] = useState<string | null>(null);
  const [messages, setMessages] = useState<ChatMessage[]>([]);
  const [input, setInput] = useState("");
  const [isSending, setIsSending] = useState(false);
  const [sessionTitles, setSessionTitles] = useState<Record<string, string>>(loadSessionTitles);
  const [hiddenSessionIds, setHiddenSessionIds] = useState<Set<string>>(loadHiddenSessionIds);

  const visibleSessions = useMemo(
    () => sessions.filter((s) => !hiddenSessionIds.has(s.session_id)),
    [sessions, hiddenSessionIds]
  );

  useEffect(() => {
    const t = localStorage.getItem(STORAGE_KEYS.token);
    const raw = localStorage.getItem(STORAGE_KEYS.user);
    if (!t || !raw) return;
    try {
      setToken(t);
      setUser(JSON.parse(raw) as UserInfo);
    } catch {
      localStorage.removeItem(STORAGE_KEYS.token);
      localStorage.removeItem(STORAGE_KEYS.user);
    }
  }, []);

  const fetchSessions = useCallback(async () => {
    if (!token) { setSessions([]); return; }
    setLoadingSessions(true);
    try {
      const data = await listSessions(token);
      setSessions(data);
    } catch (err) {
      if (err instanceof ApiError) toast.error(err.message);
    } finally {
      setLoadingSessions(false);
    }
  }, [token]);

  useEffect(() => { void fetchSessions(); }, [fetchSessions]);

  const handleRenameSession = (sessionId: string, title: string) => {
    persistSessionTitle(sessionId, title);
    setSessionTitles((prev) => ({ ...prev, [sessionId]: title }));
  };

  const handleDeleteSession = (sessionId: string) => {
    persistHiddenSession(sessionId);
    setHiddenSessionIds((prev) => new Set([...prev, sessionId]));
    if (currentSessionId === sessionId) {
      setCurrentSessionId(null);
      setMessages([]);
    }
  };

  const handleSelectSession = async (id: string) => {
    if (!token) return;
    setCurrentSessionId(id);
    try {
      const msgs = await getSessionMessages(id, token);
      setMessages(msgs);
    } catch (err) {
      if (err instanceof ApiError) toast.error(err.message);
    }
  };

  const handleSend = async (e: FormEvent) => {
    e.preventDefault();
    const text = input.trim();
    if (!text || isSending) return;

    setIsSending(true);
    setInput("");

    const optimistic: ChatMessage = { role: "user", content: text };
    setMessages((prev) => [...prev, optimistic]);

    try {
      const data = await sendMessage({
        message: text,
        sessionId: currentSessionId,
        token,
        guestId: token ? undefined : getGuestId(),
      });
      setCurrentSessionId(data.session_id);
      setMessages((prev) => [
        ...prev.slice(0, -1),
        optimistic,
        { role: "assistant", content: data.assistant_message },
      ]);
      if (token) void fetchSessions();
    } catch (err) {
      setMessages((prev) => prev.slice(0, -1));
      setInput(text);
      if (err instanceof ApiError) toast.error(err.message);
    } finally {
      setIsSending(false);
    }
  };

  return (
    <div className={`flex h-screen w-full ${colors.page.shellBg} !p-4 md:p-4 gap-4 overflow-hidden font-sans`}>
      <Toaster
        position="top-right"
        toastOptions={{
          error: {
            duration: 5000,
            style: {
              background: colors.basic.bgWhite,
              color: colors.status.error,
              border: `1px solid ${colors.status.errorBorder}`,
            },
          },
        }}
      />
      <div 
        className={`w-[320px] shrink-0 ${colors.basic.bgWhite} rounded-[28px] border ${colors.neutral.border100} flex flex-col overflow-y-hidden overflow-x-visible`}
        style={{ boxShadow: "rgba(0, 0, 0, 0.09) 0px 3px 12px" }}
      >
        <Sidebar
          token={token}
          user={user}
          sessions={visibleSessions}
          currentSessionId={currentSessionId}
          loadingSessions={loadingSessions}
          sessionTitles={sessionTitles}
          onLogin={(t, u) => { 
            setToken(t); 
            setUser(u); 
          }}
          onLogout={() => { 
            setToken(null); 
            setUser(null); 
            setSessions([]); 
            setCurrentSessionId(null); 
            setMessages([]); 
          }}
          onRefreshSessions={() => void fetchSessions()}
          onSelectSession={(id) => void handleSelectSession(id)}
          onRenameSession={handleRenameSession}
          onDeleteSession={handleDeleteSession}
          onNewChat={() => { 
            setCurrentSessionId(null); 
            setMessages([]); 
          }}
        />
      </div>
      <div className={`flex-1 ${colors.basic.bgWhite} rounded-[28px] flex flex-col overflow-hidden`}>
        <ChatArea
          messages={messages}
          sessionId={currentSessionId}
          input={input}
          isSending={isSending}
          onInputChange={setInput}
          onSend={handleSend}
        />
      </div>
    </div>
  );
}
