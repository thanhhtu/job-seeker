import { useState } from "react";
import { colors } from "@/theme/colors";
import { SessionSummary } from "@/types/session";
import { MessageSquareMore, PencilLine, Trash } from "lucide-react";
import { Button, Dialog, Input } from "./common";

type Props = {
  sessions: SessionSummary[];
  currentSessionId: string | null;
  loading: boolean;
  onRefresh: () => void;
  onSelect: (id: string) => void;
  onRenameSession: (sessionId: string, title: string) => void | Promise<void>;
  onDeleteSession: (sessionId: string) => void;
};

function defaultLabel(s: SessionSummary): string {
  const fallback = [
    "Create Chatbot GPT...",
    "Create Html Game Environment...",
    "Apply To Leave For Emergency",
    "What Is UI UX Design?",
    "Min States For Binary DFA",
    "Crypto Lending App Name",
  ];
  const index = parseInt(s.session_id.slice(-1), 16) % fallback.length;
  return fallback[index];
}

function isWithinLast7Days(iso: string): boolean {
  const t = new Date(iso).getTime();
  if (Number.isNaN(t)) return false;
  return Date.now() - t < 7 * 24 * 60 * 60 * 1000;
}

export function SessionList({
  sessions,
  currentSessionId,
  onSelect,
  onRenameSession,
  onDeleteSession,
}: Props) {
  const [renameTarget, setRenameTarget] = useState<{ id: string; title: string } | null>(null);
  const [deleteTarget, setDeleteTarget] = useState<SessionSummary | null>(null);
  const [draftTitle, setDraftTitle] = useState("");

  const sorted = [...sessions].sort((a, b) => {
    const ta = new Date(a.last_message_at || a.created_at).getTime();
    const tb = new Date(b.last_message_at || b.created_at).getTime();
    return tb - ta;
  });

  const recent = sorted.filter((s) => isWithinLast7Days(s.last_message_at || s.created_at));
  const older = sorted.filter((s) => !isWithinLast7Days(s.last_message_at || s.created_at));

  const getTitle = (s: SessionSummary) => {
    const fromApi = s.title?.trim();
    if (fromApi) return fromApi;
    return defaultLabel(s);
  };

  const openRename = (session: SessionSummary) => {
    const title = getTitle(session);
    setDraftTitle(title);
    setRenameTarget({ id: session.session_id, title });
  };

  const confirmRename = () => {
    if (!renameTarget) return;
    const next = draftTitle.trim();
    if (!next) return;
    void onRenameSession(renameTarget.id, next);
    setRenameTarget(null);
  };

  const confirmDelete = () => {
    if (!deleteTarget) return;
    onDeleteSession(deleteTarget.session_id);
    setDeleteTarget(null);
  };

  return (
    <>
      <div className="flex flex-col gap-1">
        {recent.map((s) => (
          <SessionRow
            key={s.session_id}
            title={getTitle(s)}
            active={currentSessionId === s.session_id}
            onSelect={() => onSelect(s.session_id)}
            onEdit={() => openRename(s)}
            onDelete={() => setDeleteTarget(s)}
          />
        ))}

        {older.length > 0 && (
          <div className="mt-5">
            <p className={`text-[13px] font-semibold ${colors.neutral.text400} px-2 mb-2`}>
              Last 7 Days
            </p>
            <div className="flex flex-col gap-1">
              {older.map((s) => (
                <SessionRow
                  key={s.session_id}
                  title={getTitle(s)}
                  active={currentSessionId === s.session_id}
                  onSelect={() => onSelect(s.session_id)}
                  onEdit={() => openRename(s)}
                  onDelete={() => setDeleteTarget(s)}
                  faded={currentSessionId !== s.session_id}
                />
              ))}
            </div>
          </div>
        )}
      </div>

      <Dialog
        open={renameTarget !== null}
        onClose={() => setRenameTarget(null)}
        title="Đổi tên cuộc trò chuyện"
        footer={
          <>
            <Button variant="transparent" className={`!p-2.5 !px-4 ${colors.neutral.text600}`} onClick={() => setRenameTarget(null)}>
              Hủy
            </Button>
            <Button className="!p-2.5 !px-5" onClick={confirmRename} disabled={!draftTitle.trim()}>
              Lưu
            </Button>
          </>
        }
      >
        <Input
          autoFocus
          value={draftTitle}
          onChange={(e) => setDraftTitle(e.target.value)}
          onKeyDown={(e) => {
            if (e.key === "Enter") confirmRename();
          }}
          placeholder="Nhập tên cuộc trò chuyện"
          className="!rounded-xl"
        />
      </Dialog>

      <Dialog
        open={deleteTarget !== null}
        onClose={() => setDeleteTarget(null)}
        title="Xóa cuộc trò chuyện?"
        footer={
          <>
            <Button variant="transparent" className={`!p-2.5 !px-4 ${colors.neutral.text600}`} onClick={() => setDeleteTarget(null)}>
              Hủy
            </Button>
            <Button className={`!p-2.5 !px-5 ${colors.status.bgError}`} onClick={confirmDelete}>
              Xóa
            </Button>
          </>
        }
      >
        <p className={`text-[13px] leading-relaxed ${colors.neutral.text600}`}>
          Bạn có chắc muốn xóa{" "}
          <span className={`font-semibold ${colors.neutral.text800}`}>
            {deleteTarget ? getTitle(deleteTarget) : ""}
          </span>
          ? Hành động này không thể hoàn tác.
        </p>
      </Dialog>
    </>
  );
}

function SessionRow({
  title,
  active,
  onSelect,
  onEdit,
  onDelete,
  faded = false,
}: {
  title: string;
  active: boolean;
  onSelect: () => void;
  onEdit: () => void;
  onDelete: () => void;
  faded?: boolean;
}) {
  const iconClass = active
    ? colors.primary.text
    : faded
    ? colors.neutral.text400
    : colors.neutral.text900;

  const labelClass = active
    ? `font-semibold ${colors.primary.text}`
    : faded
    ? `font-medium ${colors.neutral.text400}`
    : `font-medium ${colors.neutral.text900}`;

  return (
    <div
      role="button"
      tabIndex={0}
      onClick={onSelect}
      onKeyDown={(e) => {
        if (e.key === "Enter" || e.key === " ") {
          e.preventDefault();
          onSelect();
        }
      }}
      className={`relative w-full rounded-full px-6 py-2 min-h-[44px] flex items-center gap-2.5 transition-colors cursor-pointer select-none ${
        active ? colors.primary.xLightBg : colors.neutral.hoverBg50
      } ${!active && faded ? "opacity-60" : ""}`}
    >
      <MessageSquareMore className={`w-5 h-5 shrink-0 ${iconClass}`} />

      <span className={`flex-1 min-w-0 truncate text-[13px] leading-none ${labelClass}`}>
        {title}
      </span>

      {active && (
        <div className={`absolute right-2.5 top-1/2 -translate-y-1/2 flex items-center gap-2.5 py-3 px-4 rounded-3xl ${colors.primary.lightBg}`}>
          <button
            type="button"
            onClick={(e) => {
              e.stopPropagation();
              onEdit();
            }}
            aria-label="Rename session"
            className={`p-0.5 rounded-full transition-colors cursor-pointer ${colors.action.icon} ${colors.action.hoverEdit}`}
          >
            <PencilLine className="w-4 h-4" />
          </button>

          <button
            type="button"
            onClick={(e) => {
              e.stopPropagation();
              onDelete();
            }}
            aria-label="Delete session"
            className={`p-0.5 rounded-full transition-colors cursor-pointer ${colors.action.icon} ${colors.action.hoverDelete}`}
          >
            <Trash className="w-4 h-4" />
          </button>
        </div>
      )}
    </div>
  );
}
