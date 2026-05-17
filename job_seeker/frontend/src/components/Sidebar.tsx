import { UserInfo } from "@/types/user";
import { SessionSummary } from "@/types/session";
import { AuthForm } from "./AuthForm";
import { SessionList } from "./SessionList";
import { LogOut, Plus, Search, Settings } from "lucide-react";
import { Button } from "./common";
import { colors } from "@/theme/colors";

type Props = {
  token: string | null;
  user: UserInfo | null;
  sessions: SessionSummary[];
  currentSessionId: string | null;
  loadingSessions: boolean;
  sessionTitles: Record<string, string>;
  onLogin: (token: string, user: UserInfo) => void;
  onLogout: () => void;
  onRefreshSessions: () => void;
  onSelectSession: (id: string) => void;
  onRenameSession: (sessionId: string, title: string) => void;
  onDeleteSession: (sessionId: string) => void;
  onNewChat: () => void;
};

export function Sidebar({
  token,
  user,
  sessions,
  currentSessionId,
  loadingSessions,
  onLogin,
  onLogout,
  onRefreshSessions,
  onSelectSession,
  sessionTitles,
  onRenameSession,
  onDeleteSession,
  onNewChat,
}: Props) {
  return (
    <aside className="flex flex-col h-full my-10">
      <div className="pb-4 shrink-0">
        <h1 className={`mx-10 text-[19px] font-bold tracking-tight ${colors.neutral.text900} mb-6`}>
          JOB SEEKER
        </h1>

        <div className="mx-10 flex items-center gap-6 mb-6 mt-8">
          <Button
            onClick={onNewChat}
            className="flex-1 inline-flex items-center justify-center gap-4 active:scale-95 text-[13px] text-semibold !p-4"
          >
            <Plus className="w-6 h-6" />
            Đoạn chat mới
          </Button>
          <Button variant="secondary" className="h-full shrink-0 !p-4">
            <Search className="w-6 h-6" />
          </Button>
        </div>

        <div className={`flex items-center justify-between px-1 border-y ${colors.neutral.border200} py-6 text-[13px]`}>
          <span className={`mx-10 font-semibold ${colors.neutral.text400} tracking-wide`}>
            Lịch sử trò chuyện
          </span>
          <Button
            onClick={onRefreshSessions}
            variant="transparent"
            className={`mx-10 font-semibold !p-0 ${colors.primary.text} hover:opacity-80`}
          >
            Xóa tất cả
          </Button>
        </div>
      </div>

      <div className="flex-1 overflow-y-auto mx-5 min-h-0">
        {!token ? (
          <AuthForm onLogin={onLogin} />
        ) : (
          <SessionList
            sessions={sessions}
            currentSessionId={currentSessionId}
            loading={loadingSessions}
            hasToken={!!token}
            sessionTitles={sessionTitles}
            onRefresh={onRefreshSessions}
            onSelect={onSelectSession}
            onRenameSession={onRenameSession}
            onDeleteSession={onDeleteSession}
          />
        )}
      </div>

      <div className={`mx-10 pt-4 shrink-0 border-t ${colors.neutral.border100} space-y-2 pb-2`}>
        <button
          type="button"
          className={`w-full flex items-center gap-3 rounded-full px-4 py-3 font-semibold text-[13px] ${colors.neutral.text800} ${colors.neutral.hoverBg50} transition-colors cursor-pointer`}
        >
          <Settings className="w-5 h-5" />
          Settings
        </button>

        {token && user && (
          <div className={`flex items-center gap-3 rounded-full px-3 py-2.5 ${colors.neutral.hoverBg50}`}>
            <div
              className={`w-9 h-9 rounded-full ${colors.primary.lightBg} flex items-center justify-center ${colors.primary.text} font-bold text-[14px] shrink-0`}
            >
              {user.email[0].toUpperCase()}
            </div>
            <span className={`font-semibold text-[13px] ${colors.neutral.text900} truncate flex-1 capitalize`}>
              {user.email.split("@")[0].replace(/[._]/g, " ")}
            </span>
            <button
              type="button"
              onClick={onLogout}
              aria-label="Log out"
              className={`p-1.5 rounded-full ${colors.neutral.text400} hover:${colors.status.error} transition-colors cursor-pointer`}
            >
              <LogOut className="w-4 h-4" />
            </button>
          </div>
        )}
      </div>
    </aside>
  );
}
