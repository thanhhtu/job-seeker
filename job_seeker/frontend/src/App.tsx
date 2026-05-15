import { FormEvent, useCallback, useEffect, useState } from "react";
import { Toaster, toast } from "react-hot-toast";
import { ChatArea } from "@/components/ChatArea";
import { Sidebar } from "@/components/Sidebar";
import { ApiError, getSessionMessages, listSessions, sendMessage } from "@/api";
import { ChatMessage } from "@/types/chat";
import { SessionSummary } from "@/types/session";
import { UserInfo } from "@/types/user";
import { getGuestId } from "@/utils/ids";
import { STORAGE_KEYS } from "@/constant/storage";

export default function App() {
  const [token, setToken] = useState<string | null>(null);
  const [user, setUser] = useState<UserInfo | null>(null);
  const [sessions, setSessions] = useState<SessionSummary[]>([]);
  const [loadingSessions, setLoadingSessions] = useState(false);
  const [currentSessionId, setCurrentSessionId] = useState<string | null>(null);
  const [messages, setMessages] = useState<ChatMessage[]>([]);
  const [input, setInput] = useState("");
  const [isSending, setIsSending] = useState(false);

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
    <div className="flex h-screen w-full bg-[#f8f9fd] p-3 md:p-4 gap-4 overflow-hidden font-sans">
      <Toaster
        position="top-right"
        toastOptions={{
          error: {
            duration: 5000,
            style: { background: "#fff", color: "#dc2626", border: "1px solid #fecaca" },
          },
        }}
      />
      <div className="w-[320px] shrink-0 bg-white rounded-[32px] shadow-sm border border-slate-100 flex flex-col overflow-hidden">
        <Sidebar
          token={token}
          user={user}
          sessions={sessions}
          currentSessionId={currentSessionId}
          loadingSessions={loadingSessions}
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
          onNewChat={() => { 
            setCurrentSessionId(null); 
            setMessages([]); 
          }}
        />
      </div>
      <div className="flex-1 bg-white rounded-[32px] shadow-sm border border-slate-100 flex flex-col overflow-hidden">
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
