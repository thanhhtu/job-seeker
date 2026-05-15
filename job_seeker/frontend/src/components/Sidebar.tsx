// import { UserInfo } from "@/types/user";
// import { SessionSummary } from "@/types/session";
// import { AuthForm } from "@/components/AuthForm";
// import { SessionList } from "@/components/SessionList";

// type Props = {
//   token: string | null;
//   user: UserInfo | null;
//   sessions: SessionSummary[];
//   currentSessionId: string | null;
//   loadingSessions: boolean;
//   onLogin: (token: string, user: UserInfo) => void;
//   onLogout: () => void;
//   onRefreshSessions: () => void;
//   onSelectSession: (id: string) => void;
//   onNewChat: () => void;
// };

// function displayName(user: UserInfo | null): string {
//   if (!user?.email) return "Khách";
//   const local = user.email.split("@")[0];
//   return local.charAt(0).toUpperCase() + local.slice(1);
// }

// export function Sidebar({
//   token,
//   user,
//   sessions,
//   currentSessionId,
//   loadingSessions,
//   onLogin,
//   onLogout,
//   onRefreshSessions,
//   onSelectSession,
//   onNewChat,
// }: Props) {
//   return (
//     <aside className="w-[220px] shrink-0 bg-white border-r border-slate-200/70 flex flex-col h-full min-h-0 shadow-[1px_0_8px_0_rgba(0,0,0,0.04)]">
//       {/* Brand */}
//       <div className="px-5 pt-6 pb-4 shrink-0">
//         <h1 className="text-[17px] font-extrabold tracking-tight text-slate-900 mb-4">
//           CHAT A.I<span className="text-indigo-600">+</span>
//         </h1>

//         <div className="flex items-center gap-2">
//           <button
//             type="button"
//             onClick={onNewChat}
//             className="flex-1 inline-flex items-center justify-center gap-1.5 rounded-full bg-indigo-600 hover:bg-indigo-700 text-white text-[13px] font-semibold py-2 px-3 shadow-sm shadow-indigo-600/20 transition-colors active:scale-[0.98]"
//           >
//             <svg width="14" height="14" viewBox="0 0 24 24" fill="none" aria-hidden>
//               <path d="M12 5v14M5 12h14" stroke="currentColor" strokeWidth="2.4" strokeLinecap="round" />
//             </svg>
//             Chat mới
//           </button>
//           <button
//             type="button"
//             title="Tìm kiếm"
//             className="shrink-0 w-9 h-9 rounded-full bg-slate-900 text-white flex items-center justify-center hover:bg-slate-700 transition-colors"
//           >
//             <svg width="15" height="15" viewBox="0 0 24 24" fill="none" aria-hidden>
//               <circle cx="11" cy="11" r="7" stroke="currentColor" strokeWidth="2" />
//               <path d="M20 20l-3-3" stroke="currentColor" strokeWidth="2" strokeLinecap="round" />
//             </svg>
//           </button>
//         </div>
//       </div>

//       {/* Auth form (when not logged in) */}
//       {!token && (
//         <div className="px-4 pb-4 shrink-0">
//           <div className="rounded-2xl border border-slate-200 bg-slate-50/60 p-4">
//             <AuthForm onLogin={onLogin} />
//           </div>
//         </div>
//       )}

//       {/* Session list */}
//       {token ? (
//         <div className="flex-1 min-h-0 px-3 pb-2 overflow-hidden flex flex-col">
//           <SessionList
//             sessions={sessions}
//             currentSessionId={currentSessionId}
//             loading={loadingSessions}
//             hasToken={!!token}
//             onRefresh={onRefreshSessions}
//             onSelect={onSelectSession}
//           />
//         </div>
//       ) : (
//         <div className="flex-1 min-h-0" />
//       )}

//       {/* Footer */}
//       <div className="p-3 border-t border-slate-100 shrink-0 space-y-1">
//         <button
//           type="button"
//           className="w-full flex items-center gap-2.5 rounded-xl px-3 py-2.5 text-[13px] font-medium text-slate-600 hover:bg-slate-100 transition-colors"
//         >
//           <svg
//             width="17"
//             height="17"
//             viewBox="0 0 24 24"
//             fill="none"
//             className="text-slate-500 shrink-0"
//             aria-hidden
//           >
//             <path
//               d="M12 15a3 3 0 100-6 3 3 0 000 6zM19.4 15a1.65 1.65 0 00.33 1.82l.06.06a2 2 0 010 2.83 2 2 0 01-2.83 0l-.06-.06a1.65 1.65 0 00-1.82-.33 1.65 1.65 0 00-1 1.51V21a2 2 0 01-2 2 2 2 0 01-2-2v-.09A1.65 1.65 0 009 19.4a1.65 1.65 0 00-1.82.33l-.06.06a2 2 0 01-2.83 0 2 2 0 010-2.83l.06-.06a1.65 1.65 0 00.33-1.82 1.65 1.65 0 00-1.51-1H3a2 2 0 01-2-2 2 2 0 012-2h.09A1.65 1.65 0 004.6 9a1.65 1.65 0 00-.33-1.82l-.06-.06a2 2 0 010-2.83 2 2 0 012.83 0l.06.06a1.65 1.65 0 001.82.33H9a1.65 1.65 0 001-1.51V3a2 2 0 012-2 2 2 0 012 2v.09a1.65 1.65 0 001 1.51 1.65 1.65 0 001.82-.33l.06-.06a2 2 0 012.83 0 2 2 0 010 2.83l-.06.06a1.65 1.65 0 00-.33 1.82V9a1.65 1.65 0 001.51 1H21a2 2 0 012 2 2 2 0 01-2 2h-.09a1.65 1.65 0 00-1.51 1z"
//               stroke="currentColor"
//               strokeWidth="1.4"
//               strokeLinecap="round"
//               strokeLinejoin="round"
//             />
//           </svg>
//           Cài đặt
//         </button>

//         {token && user ? (
//           <div className="flex items-center gap-2.5 px-3 py-2.5 rounded-xl hover:bg-slate-50 transition-colors cursor-default">
//             <div className="w-8 h-8 rounded-full bg-gradient-to-br from-indigo-400 to-indigo-600 flex items-center justify-center text-xs font-bold text-white shadow-sm shrink-0">
//               {user.email.charAt(0).toUpperCase()}
//             </div>
//             <div className="flex-1 min-w-0">
//               <p className="text-[13px] font-semibold text-slate-900 truncate leading-tight">
//                 {displayName(user)}
//               </p>
//             </div>
//             <button
//               type="button"
//               onClick={onLogout}
//               title="Đăng xuất"
//               className="shrink-0 text-slate-400 hover:text-red-500 transition-colors p-1 rounded-lg hover:bg-red-50"
//             >
//               <svg width="15" height="15" viewBox="0 0 24 24" fill="none" aria-hidden>
//                 <path
//                   d="M9 21H5a2 2 0 01-2-2V5a2 2 0 012-2h4M16 17l5-5-5-5M21 12H9"
//                   stroke="currentColor"
//                   strokeWidth="2"
//                   strokeLinecap="round"
//                   strokeLinejoin="round"
//                 />
//               </svg>
//             </button>
//           </div>
//         ) : (
//           <p className="px-3 py-2 text-[11px] text-slate-400 text-center">
//             Đăng nhập để lưu lịch sử
//           </p>
//         )}
//       </div>
//     </aside>
//   );
// }

import { UserInfo } from "@/types/user";
import { SessionSummary } from "@/types/session";
import { AuthForm } from "@/components/AuthForm";
import { SessionList } from "@/components/SessionList";

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
          CHAT A.I<span className="text-indigo-600">+</span>
        </h1>

        <div className="flex items-center gap-2 mb-8">
          <button
            onClick={onNewChat}
            className="flex-1 inline-flex items-center justify-center gap-2 rounded-2xl bg-[#5d5fef] hover:bg-indigo-700 text-white text-[13px] font-bold py-3.5 shadow-md shadow-indigo-100 transition-all active:scale-95"
          >
            <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="3"><path d="M12 5v14M5 12h14"/></svg>
            New chat
          </button>
          <button className="w-12 h-12 rounded-2xl bg-slate-900 text-white flex items-center justify-center hover:bg-slate-800 transition-colors shrink-0">
            <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2.5"><circle cx="11" cy="11" r="8"/><path d="m21 21-4.3-4.3"/></svg>
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
