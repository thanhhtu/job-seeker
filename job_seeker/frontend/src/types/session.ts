export type SessionSummary = {
  session_id: string;
  title: string | null;
  created_at: string;
  last_message_at: string | null;
  message_count: number;
};
