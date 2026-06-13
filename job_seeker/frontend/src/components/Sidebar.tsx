import { useState } from "react";
import { useNavigate } from "react-router-dom";
import { UserInfo } from "@/types/user";
import { SessionSummary } from "@/types/session";
import { AuthForm } from "./AuthForm";
import { SessionList } from "./SessionList";
import { LogOut, Plus, Search, Settings } from "lucide-react";
import { Button, Dialog } from "./common";
import { AuthResponse } from "@/api";
import { colors } from "@/theme/colors";

type Props = {
  token: string | null;
  user: UserInfo | null;
  sessions: SessionSummary[];
  currentSessionId: string | null;
  loadingSessions: boolean;
  onLogin: (data: AuthResponse) => void;
  onLogout: () => void;
  onRefreshSessions: () => void;
  onSelectSession: (id: string) => void;
  onRenameSession: (sessionId: string, title: string) => void | Promise<void>;
  onDeleteSession: (sessionId: string) => void | Promise<void>;
  onDeleteAllSessions: () => void | Promise<void>;
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
  onRenameSession,
  onDeleteSession,
  onDeleteAllSessions,
  onNewChat,
}: Props) {
  const navigate = useNavigate();
  const [showDeleteAllConfirm, setShowDeleteAllConfirm] = useState(false);
  const [isDeletingAll, setIsDeletingAll] = useState(false);

  const confirmDeleteAll = async () => {
    setIsDeletingAll(true);
    try {
      await onDeleteAllSessions();
      setShowDeleteAllConfirm(false);
    } finally {
      setIsDeletingAll(false);
    }
  };

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

        <div
          className={`flex items-center px-1 border-y ${colors.neutral.border200} py-6 text-[13px] ${
            token ? "justify-between" : ""
          }`}
        >
          {token ? (
            <>
              <span className={`mx-10 font-semibold ${colors.neutral.text400} tracking-wide`}>
                Lịch sử trò chuyện
              </span>
              <Button
                onClick={() => setShowDeleteAllConfirm(true)}
                variant="transparent"
                className={`mx-10 font-semibold !p-0 ${colors.action.textDanger} ${colors.action.hoverTextDanger}`}
              >
                Xóa tất cả
              </Button>
            </>
          ) : (
            <span className={`mx-10 font-semibold ${colors.neutral.text400} tracking-wide`}>
              Đăng nhập để lưu lịch sử trò chuyện
            </span>
          )}
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
          onClick={() => navigate("/settings")}
          className={`w-full flex items-center gap-3 rounded-full px-4 py-3 font-semibold text-[13px] ${colors.neutral.text800} ${colors.neutral.hoverBg50} transition-colors cursor-pointer`}
        >
          <Settings className="w-5 h-5" />
          Cài đặt
        </button>

        {token && user && (
          <div className={`flex items-center gap-3 rounded-full px-3 py-2.5 ${colors.neutral.hoverBg50}`}>
            <div
              className={`w-9 h-9 rounded-full ${colors.primary.lightBg} flex items-center justify-center ${colors.primary.text} font-bold text-[15px] shrink-0`}
            >
              {user.email[0].toUpperCase()}
            </div>
            <span className={`font-semibold text-[13px] ${colors.neutral.text900} truncate flex-1 capitalize`}>
              {user.email.split("@")[0].replace(/[._]/g, " ")}
            </span>
            <button
              type="button"
              onClick={onLogout}
              aria-label="Đăng xuất"
              title="Đăng xuất"
              className={`p-1.5 rounded-full transition-colors cursor-pointer ${colors.action.icon} ${colors.action.hoverDelete}`}
            >
              <LogOut className="w-5 h-5" />
            </button>
          </div>
        )}
      </div>
      <Dialog
        open={showDeleteAllConfirm}
        onClose={() => setShowDeleteAllConfirm(false)}
        title="Xóa tất cả cuộc trò chuyện?"
        footer={
          <>
            <Button
              variant="transparent"
              className={`!p-2.5 !px-4 ${colors.neutral.text600}`}
              onClick={() => setShowDeleteAllConfirm(false)}
            >
              Hủy
            </Button>
            <Button
              variant="destructive"
              className="!p-2.5 !px-5"
              isLoading={isDeletingAll}
              onClick={() => void confirmDeleteAll()}
            >
              Xóa tất cả
            </Button>
          </>
        }
      >
        <p className={`text-[13px] leading-relaxed ${colors.neutral.text600}`}>
          Bạn có chắc muốn xóa tất cả cuộc trò chuyện? Hành động này không thể hoàn tác.
        </p>
      </Dialog>
    </aside>
  );
}
