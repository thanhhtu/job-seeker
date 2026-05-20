import { ChatMessage, ChatRole } from "@/types/chat";
import { SessionSummary } from "@/types/session";
import { apiFetch } from "./client";

type RawMessage = {
  role: string;
  content: string;
  created_at?: string;
};

export async function listSessions(): Promise<SessionSummary[]> {
  const res = await apiFetch("/api/me/chat-sessions");
  return res.json() as Promise<SessionSummary[]>;
}

export async function updateSessionTitle(
  sessionId: string,
  title: string
): Promise<SessionSummary> {
  const res = await apiFetch(`/api/me/chat-sessions/${sessionId}`, {
    method: "PATCH",
    body: JSON.stringify({ title }),
  });
  return res.json() as Promise<SessionSummary>;
}

export async function getSessionMessages(sessionId: string): Promise<ChatMessage[]> {
  const res = await apiFetch(`/api/sessions/${sessionId}/messages`);
  const data = (await res.json()) as { messages: RawMessage[] };
  return data.messages.map((m) => ({
    role: m.role as ChatRole,
    content: m.content,
    createdAt: m.created_at,
  }));
}
