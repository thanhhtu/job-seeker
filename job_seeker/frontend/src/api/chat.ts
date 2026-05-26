import { AssistantData } from "@/types/chat";
import { apiFetch } from "./client";

export type SendMessageParams = {
  message: string;
  sessionId: string | null;
  token?: string | null;
  guestId?: string;
};

export type SendMessageResponse = {
  session_id: string;
  assistant_message: string;
  data?: AssistantData | null;
};

export async function sendMessage(params: SendMessageParams): Promise<SendMessageResponse> {
  const { message, sessionId, token, guestId } = params;

  const body: Record<string, unknown> = { message, session_id: sessionId };
  if (!token && guestId) {
    body["user_id"] = guestId;
  }

  const res = await apiFetch("/api/chat", {
    method: "POST",
    token,
    body: JSON.stringify(body),
  });

  return res.json() as Promise<SendMessageResponse>;
}
