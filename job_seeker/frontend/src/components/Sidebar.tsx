import { UserInfo } from "@/types/user";
import { SessionSummary } from "@/types/session";
import { AuthForm } from "@/components/AuthForm";
import { SessionList } from "@/components/SessionList";
import { Plus, Search } from "lucide-react";

type Props = {
  token: string | null;
  user: UserInfo | null;
  sessions: SessionSummary[];
  currentSessionId: string | null;
  loadingSessions: boolean;
  onLogin: (token: string, user: UserInfo) => void;
  onLogout: () => void;
  onRefreshSessions: () => void;
  onSelectSession: (id: string) => void;
  onNewChat: () => void;
};

export function Sidebar({ token, user, sessions, currentSessionId, loadingSessions, onLogin, onLogout, onRefreshSessions, onSelectSession, onNewChat }: Props) {
  return (
    <aside className="flex flex-col h-full">
      <div className="px-5 pt-8 pb-4 shrink-0">
        <h1 className="text-[20px] font-black tracking-tight text-slate-900 mb-8">
          JOB SEEKER
        </h1>

        <div className="flex items-center gap-3 mb-8">
          <button
            onClick={onNewChat}
            className="flex-1 inline-flex items-center justify-center gap-2 rounded-full hover:bg-indigo-700 text-white text-[13px] font-bold shadow-md shadow-indigo-100 transition-all active:scale-95"
          >
            <Plus className="w-4 h-4" />
            New chat
          </button>
          <button className="h-full rounded-full bg-slate-900 text-white flex items-center justify-center hover:bg-slate-800 transition-colors shrink-0">
            <Search className="w-4 h-4" />
          </button>
        </div>

        <div className="flex items-center justify-between px-1">
          <span className="text-[10px] font-bold text-slate-400 uppercase tracking-widest">Your conversations</span>
          <button onClick={onRefreshSessions} className="text-[10px] font-bold text-indigo-600 hover:underline">Clear All</button>
        </div>
      </div>

      <div className="flex-1 overflow-y-auto px-3">
        {!token ? (
          <AuthForm onLogin={onLogin} />
        ) : (
          <SessionList
            sessions={sessions}
            currentSessionId={currentSessionId}
            loading={loadingSessions}
            hasToken={!!token}
            onRefresh={onRefreshSessions}
            onSelect={onSelectSession}
          />
        )}
      </div>

      <div className="p-4 border-t border-slate-50">
        <button className="w-full flex items-center gap-3 rounded-2xl px-4 py-3 text-[13px] font-bold text-slate-600 hover:bg-slate-50 transition-colors">
          <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2"><circle cx="12" cy="12" r="3"/><path d="M19.4 15a1.65 1.65 0 0 0 .33 1.82l.06.06a2 2 0 0 1 0 2.83 2 2 0 0 1-2.83 0l-.06-.06a1.65 1.65 0 0 0-1.82-.33 1.65 1.65 0 0 0-1 1.51V21a2 2 0 0 1-2 2 2 2 0 0 1-2-2v-.09A1.65 1.65 0 0 0 9 19.4a1.65 1.65 0 0 0-1.82.33l-.06.06a2 2 0 0 1-2.83 0 2 2 0 0 1 0-2.83l.06-.06a1.65 1.65 0 0 0 .33-1.82 1.65 1.65 0 0 0-1.51-1H3a2 2 0 0 1-2-2 2 2 0 0 1 2-2h.09A1.65 1.65 0 0 0 4.6 9a1.65 1.65 0 0 0-.33-1.82l-.06-.06a2 2 0 0 1 0-2.83 2 2 0 0 1 2.83 0l.06.06a1.65 1.65 0 0 0 1.82.33H9a1.65 1.65 0 0 0 1-1.51V3a2 2 0 0 1 2-2 2 2 0 0 1 2 2v.09a1.65 1.65 0 0 0 1 1.51 1.65 1.65 0 0 0 1.82-.33l.06-.06a2 2 0 0 1 2.83 0 2 2 0 0 1 0 2.83l-.06.06a1.65 1.65 0 0 0-.33 1.82V9a1.65 1.65 0 0 0 1.51 1H21a2 2 0 0 1 2 2 2 2 0 0 1-2 2h-.09a1.65 1.65 0 0 0-1.51 1z"/></svg>
          Settings
        </button>
        {token && user && (
          <div className="mt-2 flex items-center gap-3 px-4 py-2 hover:bg-slate-50 rounded-2xl cursor-pointer">
            <div className="w-8 h-8 rounded-full bg-indigo-100 flex items-center justify-center text-indigo-700 font-bold text-[10px] shrink-0">{user.email[0].toUpperCase()}</div>
            <span className="text-[13px] font-bold text-slate-800 truncate flex-1">{user.email.split('@')[0]}</span>
            <button onClick={onLogout} className="text-slate-300 hover:text-red-500 shrink-0"><svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2"><path d="M9 21H5a2 2 0 0 1-2-2V5a2 2 0 0 1 2-2h4m7 14 5-5-5-5m5 5H9"/></svg></button>
          </div>
        )}
      </div>
    </aside>
  );
}
