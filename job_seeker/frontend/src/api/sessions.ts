import { ChatMessage, ChatRole } from "@/types/chat";
import { SessionSummary } from "@/types/session";
import { apiFetch } from "./client";

type RawMessage = {
  role: string;
  content: string;
  created_at?: string;
};

export async function listSessions(token: string): Promise<SessionSummary[]> {
  const res = await apiFetch("/api/me/chat-sessions", { token });
  return res.json() as Promise<SessionSummary[]>;
}

export async function getSessionMessages(sessionId: string, token: string): Promise<ChatMessage[]> {
  const res = await apiFetch(`/api/sessions/${sessionId}/messages`, { token });
  const data = (await res.json()) as { messages: RawMessage[] };
  return data.messages.map((m) => ({
    role: m.role as ChatRole,
    content: m.content,
    createdAt: m.created_at,
  }));
}
