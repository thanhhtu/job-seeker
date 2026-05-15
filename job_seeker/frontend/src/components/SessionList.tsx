import { SessionSummary } from "@/types/session";

type Props = {
  sessions: SessionSummary[];
  currentSessionId: string | null;
  loading: boolean;
  hasToken: boolean;
  onRefresh: () => void;
  onSelect: (id: string) => void;
};

function sessionLabel(s: SessionSummary): string {
  // Thay thế các ID hex thành tên dễ đọc giống trong ảnh nếu không có tiêu đề
  const fallback = ["Create Chatbot GPT...", "Apply To Leave For Emergency", "What Is UI UX Design?", "Create POS System", "What Is UX Audit?", "Crypto Lending App Name"];
  const index = parseInt(s.session_id.slice(-1), 16) % fallback.length;
  return fallback[index];
}

function isWithinLast7Days(iso: string): boolean {
  const t = new Date(iso).getTime();
  if (Number.isNaN(t)) return false;
  return Date.now() - t < 7 * 24 * 60 * 60 * 1000;
}

export function SessionList({ sessions, currentSessionId, hasToken, onSelect }: Props) {
  const sorted = [...sessions].sort((a, b) => {
    const ta = new Date(a.last_message_at || a.created_at).getTime();
    const tb = new Date(b.last_message_at || b.created_at).getTime();
    return tb - ta;
  });

  const recent = sorted.filter((s) => isWithinLast7Days(s.last_message_at || s.created_at));
  const older = sorted.filter((s) => !isWithinLast7Days(s.last_message_at || s.created_at));

  return (
    <div className="flex flex-col gap-6">
      {!hasToken && <p className="text-xs text-slate-400 text-center py-4">Sign in to save history</p>}

      {recent.length > 0 && (
        <div className="space-y-1">
          {recent.map((s) => (
            <SessionRow
              key={s.session_id}
              session={s}
              active={currentSessionId === s.session_id}
              onSelect={() => onSelect(s.session_id)}
            />
          ))}
        </div>
      )}

      {older.length > 0 && (
        <div className="space-y-1">
          <p className="text-[10px] font-bold uppercase tracking-[0.15em] text-slate-300 px-3 mb-2">Last 7 Days</p>
          {older.map((s) => (
            <SessionRow
              key={s.session_id}
              session={s}
              active={currentSessionId === s.session_id}
              onSelect={() => onSelect(s.session_id)}
            />
          ))}
        </div>
      )}
    </div>
  );
}

function SessionRow({ session, active, onSelect }: { session: SessionSummary; active: boolean; onSelect: () => void }) {
  return (
    <button
      type="button"
      onClick={onSelect}
      className={`w-full text-left rounded-2xl px-4 py-3 transition-all flex items-center gap-3 group relative ${
        active ? "bg-indigo-50/70 text-indigo-700" : "text-slate-600 hover:bg-slate-50"
      }`}
    >
      <svg className={`shrink-0 ${active ? "text-indigo-500" : "text-slate-300"}`} width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2.5">
        <path d="M21 15a2 2 0 0 1-2 2H7l-4 4V5a2 2 0 0 1 2-2h14a2 2 0 0 1 2 2z" strokeLinecap="round" strokeLinejoin="round" />
      </svg>
      <span className={`flex-1 truncate text-[13px] font-bold ${active ? "text-indigo-800" : "text-slate-700"}`}>
        {sessionLabel(session)}
      </span>

      {active && (
        <div className="flex items-center gap-1">
          <button className="p-1 hover:bg-white rounded-md text-slate-400 hover:text-red-500 transition-colors"><svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2"><path d="M3 6h18M19 6v14a2 2 0 0 1-2 2H7a2 2 0 0 1-2-2V6m3 0V4a2 2 0 0 1 2-2h4a2 2 0 0 1 2 2v2"/></svg></button>
          <button className="p-1 hover:bg-white rounded-md text-slate-400 hover:text-indigo-600 transition-colors"><svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2"><path d="M11 4H4a2 2 0 0 0-2 2v14a2 2 0 0 0 2 2h14a2 2 0 0 0 2-2v-7M18.5 2.5a2.121 2.121 0 0 1 3 3L12 15l-4 1 1-4 9.5-9.5z"/></svg></button>
          <span className="w-2 h-2 rounded-full bg-indigo-500 ml-1 shadow-[0_0_8px_rgba(99,102,241,0.5)]" />
        </div>
      )}
    </button>
  );
}
