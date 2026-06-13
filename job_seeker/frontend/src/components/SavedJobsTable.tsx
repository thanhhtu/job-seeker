import { useEffect, useMemo, useRef, useState } from "react";
import {
  ChevronDown,
  ChevronLeft,
  ChevronRight,
  ChevronsUpDown,
  ChevronUp,
  ExternalLink,
  StickyNote,
  Trash2,
} from "lucide-react";
import { STATUS_LABELS, STATUS_ORDER, STATUS_RANK } from "@/constant/savedJob";
import { SavedJob, SavedJobStatus } from "@/types/savedJob";
import { colors } from "@/theme/colors";

const STATUS_PILL: Record<SavedJobStatus, string> = {
  saved: `${colors.badge.indigoBg} ${colors.badge.indigoText}`,
  applied: `${colors.primary.bgSolid} ${colors.basic.textWhite}`,
  interviewing: `${colors.badge.amberBg} ${colors.badge.amberText}`,
  offer: `${colors.badge.greenBg} ${colors.badge.greenText}`,
  rejected: `${colors.badge.redBg} ${colors.badge.redText}`,
};

function formatDate(value: string | null): string {
  if (!value) return "—";
  const d = new Date(value);
  if (Number.isNaN(d.getTime())) return "—";
  return d.toLocaleDateString("vi-VN", { year: "numeric", month: "2-digit", day: "2-digit" });
}

function formatJobType(workMode: string | null): string {
  if (!workMode || workMode === "unknown") return "—";
  return workMode.charAt(0).toUpperCase() + workMode.slice(1);
}

function formatSalary(job: SavedJob): string {
  if (!job.salary_min && !job.salary_max) {
    return job.salary_negotiable ? "Thỏa thuận" : "—";
  }
  const cur = job.salary_currency ?? "VND";
  const fmt = (n: number) => n.toLocaleString("vi-VN");
  if (job.salary_min && job.salary_max) return `${fmt(job.salary_min)}–${fmt(job.salary_max)} ${cur}`;
  if (job.salary_min) return `Từ ${fmt(job.salary_min)} ${cur}`;
  return `Đến ${fmt(job.salary_max!)} ${cur}`;
}

/* ----------------------------- Sorting ----------------------------- */

type SortKey = "title" | "company" | "saved_at" | "salary" | "status";
type SortDir = "asc" | "desc";

function salaryValue(job: SavedJob): number {
  return job.salary_min ?? job.salary_max ?? -1;
}

function compareJobs(a: SavedJob, b: SavedJob, key: SortKey): number {
  switch (key) {
    case "title":
      return a.title.localeCompare(b.title, "vi");
    case "company":
      return a.company_name.localeCompare(b.company_name, "vi");
    case "saved_at":
      return new Date(a.created_at).getTime() - new Date(b.created_at).getTime();
    case "salary":
      return salaryValue(a) - salaryValue(b);
    case "status":
      return STATUS_RANK[a.status] - STATUS_RANK[b.status];
  }
}

function SortHeader({
  label,
  sortKey,
  active,
  dir,
  onSort,
  className,
}: {
  label: string;
  sortKey: SortKey;
  active: boolean;
  dir: SortDir;
  onSort: (key: SortKey) => void;
  className?: string;
}) {
  return (
    <th className={`px-5 py-3.5 ${className ?? ""}`}>
      <button
        type="button"
        onClick={() => onSort(sortKey)}
        className={`inline-flex items-center gap-1 uppercase tracking-wide transition-colors cursor-pointer ${
          active ? colors.neutral.text700 : ""
        } ${colors.neutral.hoverText700}`}
      >
        {label}
        {active ? (
          dir === "asc" ? (
            <ChevronUp className="w-3.5 h-3.5" />
          ) : (
            <ChevronDown className="w-3.5 h-3.5" />
          )
        ) : (
          <ChevronsUpDown className="w-3.5 h-3.5 opacity-40" />
        )}
      </button>
    </th>
  );
}

/* ----------------------------- Status dropdown ----------------------------- */

function shouldDropUp(triggerEl: HTMLElement | null, panelH: number): boolean {
  if (!triggerEl) return false;
  const rect = triggerEl.getBoundingClientRect();
  const below = window.innerHeight - rect.bottom;
  const above = rect.top;
  return below < panelH + 12 && above > below;
}

function StatusDropdown({
  value,
  busy,
  onChange,
}: {
  value: SavedJobStatus;
  busy: boolean;
  onChange: (s: SavedJobStatus) => void;
}) {
  const [open, setOpen] = useState(false);
  const [dropUp, setDropUp] = useState(false);
  const ref = useRef<HTMLDivElement>(null);
  const btnRef = useRef<HTMLButtonElement>(null);

  useEffect(() => {
    if (!open) return;
    const onClick = (e: MouseEvent) => {
      if (ref.current && !ref.current.contains(e.target as Node)) setOpen(false);
    };
    document.addEventListener("mousedown", onClick);
    return () => document.removeEventListener("mousedown", onClick);
  }, [open]);

  const toggle = () => {
    if (!open) setDropUp(shouldDropUp(btnRef.current, STATUS_ORDER.length * 38 + 12));
    setOpen((v) => !v);
  };

  return (
    <div className="relative inline-block" ref={ref}>
      <button
        ref={btnRef}
        type="button"
        disabled={busy}
        onClick={toggle}
        className={`inline-flex items-center gap-1.5 px-3 py-1 rounded-full text-[12px] font-semibold transition-opacity disabled:opacity-50 cursor-pointer ${STATUS_PILL[value]}`}
      >
        {STATUS_LABELS[value]}
        <ChevronDown className="w-3.5 h-3.5" />
      </button>

      {open && (
        <div
          className={`absolute right-0 z-20 w-40 rounded-xl border ${colors.neutral.border200} ${colors.basic.bgWhite} py-1 shadow-lg ${
            dropUp ? "bottom-full mb-1.5" : "mt-1.5"
          }`}
        >
          {STATUS_ORDER.map((s) => (
            <button
              key={s}
              type="button"
              onClick={() => {
                setOpen(false);
                if (s !== value) onChange(s);
              }}
              className={`w-full flex items-center gap-2 px-3 py-2 text-left text-[13px] transition-colors cursor-pointer ${
                s === value
                  ? `${colors.primary.text} font-semibold ${colors.neutral.bg50}`
                  : `${colors.neutral.text700} ${colors.neutral.hoverBg50}`
              }`}
            >
              <span className={`w-2 h-2 rounded-full ${STATUS_PILL[s].split(" ")[0]}`} />
              {STATUS_LABELS[s]}
            </button>
          ))}
        </div>
      )}
    </div>
  );
}

/* ----------------------------- Note popover ----------------------------- */
function NotePopover({
  note,
  busy,
  onSave,
}: {
  note: string | null;
  busy: boolean;
  onSave: (note: string) => void;
}) {
  const [open, setOpen] = useState(false);
  const [dropUp, setDropUp] = useState(false);
  const [draft, setDraft] = useState(note ?? "");
  const ref = useRef<HTMLDivElement>(null);
  const btnRef = useRef<HTMLButtonElement>(null);
  const hasNote = !!note && note.trim().length > 0;

  useEffect(() => {
    if (open) setDraft(note ?? "");
  }, [open, note]);

  useEffect(() => {
    if (!open) return;
    const onClick = (e: MouseEvent) => {
      if (ref.current && !ref.current.contains(e.target as Node)) setOpen(false);
    };
    document.addEventListener("mousedown", onClick);
    return () => document.removeEventListener("mousedown", onClick);
  }, [open]);

  const trimmed = draft.trim();
  const dirty = trimmed !== (note ?? "").trim();

  const toggle = () => {
    if (!open) setDropUp(shouldDropUp(btnRef.current, 200));
    setOpen((v) => !v);
  };

  return (
    <div className="relative inline-block bottom-[-1.5px]" ref={ref}>
      <button
        ref={btnRef}
        type="button"
        onClick={toggle}
        aria-label={hasNote ? "Xem/Sửa ghi chú" : "Thêm ghi chú"}
        title={hasNote ? "Xem/Sửa ghi chú" : "Thêm ghi chú"}
        className={`p-1.5 rounded-full transition-colors cursor-pointer ${
          hasNote ? colors.primary.text : colors.action.icon
        } ${colors.primary.hoverText}`}
      >
        <StickyNote className="w-6 h-6" />
        {hasNote && <span className={`absolute top-1 right-1 w-1.5 h-1.5 rounded-full ${colors.primary.bgSolid}`} />}
      </button>

      {open && (
        <div
          className={`absolute right-0 z-30 w-85 rounded-xl border ${colors.neutral.border200} ${colors.basic.bgWhite} p-3 shadow-lg text-left ${
            dropUp ? "bottom-full mb-1.5" : "mt-1.5"
          }`}
        >
          <p className={`text-[12px] font-semibold ${colors.neutral.text600} mb-1.5`}>Ghi chú</p>
          <textarea
            value={draft}
            onChange={(e) => setDraft(e.target.value)}
            maxLength={2000}
            rows={4}
            placeholder="Thêm ghi chú cho công việc này…"
            className={`w-full resize-none rounded-lg border ${colors.neutral.border200} px-2.5 py-2 text-[13px] ${colors.neutral.text800} focus:outline-none ${colors.primary.focusBorder}`}
          />
          <div className="mt-2 flex items-center justify-between">
            <span className={`text-[11px] ${colors.neutral.text400}`}>{draft.length}/2000</span>
            <div className="flex items-center gap-2">
              <button
                type="button"
                onClick={() => setOpen(false)}
                className={`text-[12px] font-medium ${colors.neutral.text500} ${colors.neutral.hoverText700} cursor-pointer`}
              >
                Hủy
              </button>
              <button
                type="button"
                disabled={busy || !dirty}
                onClick={() => {
                  onSave(trimmed);
                  setOpen(false);
                }}
                className={`text-[12px] font-semibold ${colors.basic.textWhite} ${colors.primary.bgSolid} ${colors.primary.hoverBg} rounded-lg px-3 py-1.5 transition-colors disabled:opacity-50 cursor-pointer`}
              >
                Lưu
              </button>
            </div>
          </div>
        </div>
      )}
    </div>
  );
}

/* ----------------------------- Table ----------------------------- */
const PAGE_SIZE = 10;

type Props = {
  jobs: SavedJob[];
  busyId: string | null;
  onChangeStatus: (jobId: string, status: SavedJobStatus) => void;
  onSaveNote: (jobId: string, note: string) => void;
  onRemove: (jobId: string) => void;
};

export function SavedJobsTable({ jobs, busyId, onChangeStatus, onSaveNote, onRemove }: Props) {
  const [sortKey, setSortKey] = useState<SortKey>("saved_at");
  const [sortDir, setSortDir] = useState<SortDir>("desc");
  const [page, setPage] = useState(1);

  const wrapRef = useRef<HTMLDivElement>(null);
  const pageSize = PAGE_SIZE;

  const handleSort = (key: SortKey) => {
    if (key === sortKey) {
      setSortDir((d) => (d === "asc" ? "desc" : "asc"));
    } else {
      setSortKey(key);
      setSortDir(key === "saved_at" ? "desc" : "asc");
    }
  };

  const sortedJobs = useMemo(() => {
    const sign = sortDir === "asc" ? 1 : -1;
    return [...jobs].sort((a, b) => sign * compareJobs(a, b, sortKey));
  }, [jobs, sortKey, sortDir]);

  const totalPages = Math.max(1, Math.ceil(sortedJobs.length / pageSize));

  useEffect(() => {
    setPage((p) => Math.min(p, totalPages));
  }, [totalPages]);

  const pageJobs = useMemo(
    () => sortedJobs.slice((page - 1) * pageSize, page * pageSize),
    [sortedJobs, page, pageSize]
  );

  const from = sortedJobs.length === 0 ? 0 : (page - 1) * pageSize + 1;
  const to = Math.min(page * pageSize, sortedJobs.length);

  return (
    <div ref={wrapRef} className={`flex flex-col h-full border rounded-xl overflow-hidden ${colors.neutral.border200}`}>
      <div className="flex-1 overflow-x-auto">
        <table className="w-full border-collapse text-left">
          <thead>
            <tr className={`${colors.neutral.bg50} text-[12px] font-semibold ${colors.neutral.text500}`}>
              <SortHeader label="Công việc" sortKey="title" active={sortKey === "title"} dir={sortDir} onSort={handleSort} />
              <SortHeader label="Công ty" sortKey="company" active={sortKey === "company"} dir={sortDir} onSort={handleSort} />
              <SortHeader
                label="Ngày lưu"
                sortKey="saved_at"
                active={sortKey === "saved_at"}
                dir={sortDir}
                onSort={handleSort}
                className="whitespace-nowrap"
              />
              <th className="px-5 py-3.5 whitespace-nowrap uppercase tracking-wide">Hình thức</th>
              <th className="px-5 py-3.5 uppercase tracking-wide">Địa điểm</th>
              <SortHeader
                label="Mức lương"
                sortKey="salary"
                active={sortKey === "salary"}
                dir={sortDir}
                onSort={handleSort}
                className="whitespace-nowrap"
              />
              <SortHeader label="Trạng thái" sortKey="status" active={sortKey === "status"} dir={sortDir} onSort={handleSort} />
              <th className="px-5 py-3.5 text-right" />
            </tr>
          </thead>
          <tbody>
            {pageJobs.map((job) => (
              <tr
                key={job.job_id}
                className={`border-t ${colors.neutral.border100} text-[13px] ${colors.neutral.hoverBg50} transition-colors`}
              >
                <td className="px-5 py-4 max-w-[220px]">
                  <span className={`font-semibold ${colors.neutral.text900} line-clamp-2`}>{job.title}</span>
                </td>
                <td className={`px-5 py-4 ${colors.neutral.text600} max-w-[160px]`}>
                  <span className="line-clamp-2">{job.company_name}</span>
                </td>
                <td className={`px-5 py-4 ${colors.neutral.text600} whitespace-nowrap`}>
                  {formatDate(job.created_at)}
                </td>
                <td className={`px-5 py-4 ${colors.neutral.text600} whitespace-nowrap`}>
                  {formatJobType(job.work_mode)}
                </td>
                <td className={`px-5 py-4 ${colors.neutral.text600} max-w-[140px]`}>
                  <span className="line-clamp-2">
                    {job.locations.length > 0 ? job.locations.join(", ") : "—"}
                  </span>
                </td>
                <td className={`px-5 py-4 whitespace-nowrap font-medium ${colors.status.success}`}>
                  {formatSalary(job)}
                </td>
                <td className="px-5 py-4">
                  <StatusDropdown
                    value={job.status}
                    busy={busyId === job.job_id}
                    onChange={(s) => onChangeStatus(job.job_id, s)}
                  />
                </td>
                <td className="px-5 py-4 text-right whitespace-nowrap">
                  <div className="inline-flex items-center gap-2">
                    <NotePopover
                      note={job.note}
                      busy={busyId === job.job_id}
                      onSave={(note) => onSaveNote(job.job_id, note)}
                    />
                    {job.url && (
                      <a
                        href={job.url}
                        target="_blank"
                        rel="noopener noreferrer"
                        aria-label="Xem chi tiết"
                        title="Xem chi tiết"
                        className={`p-1.5 rounded-full transition-colors ${colors.action.icon} ${colors.primary.hoverText}`}
                      >
                        <ExternalLink className="w-6 h-6" />
                      </a>
                    )}
                    <button
                      type="button"
                      onClick={() => onRemove(job.job_id)}
                      disabled={busyId === job.job_id}
                      aria-label="Bỏ lưu"
                      title="Bỏ lưu"
                      className={`p-1.5 rounded-full transition-colors cursor-pointer disabled:opacity-50 ${colors.action.icon} ${colors.action.hoverDelete}`}
                    >
                      <Trash2 className="w-6 h-6" />
                    </button>
                  </div>
                </td>
              </tr>
            ))}
            {/* Filler rows keep the table height constant across pages */}
            {Array.from({ length: Math.max(0, pageSize - pageJobs.length) }).map((_, i) => (
              <tr key={`filler-${i}`} className={`border-t ${colors.neutral.border100}`} aria-hidden>
                <td className="px-5 py-4" colSpan={8}>
                  <span className="block text-[13px] leading-snug invisible">&nbsp;</span>
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>

      {/* Pagination footer */}
      <div className={`shrink-0 flex items-center justify-between gap-3 px-5 py-3 border-t ${colors.neutral.border100} ${colors.neutral.bg50}`}>
        <span className={`text-[12px] ${colors.neutral.text500}`}>
          {from}–{to} / {sortedJobs.length}
        </span>
        <div className="flex items-center gap-2">
          <button
            type="button"
            onClick={() => setPage((p) => Math.max(1, p - 1))}
            disabled={page <= 1}
            aria-label="Trang trước"
            className={`p-1.5 rounded-lg border ${colors.neutral.border200} ${colors.basic.bgWhite} transition-colors cursor-pointer disabled:opacity-40 disabled:cursor-default ${colors.action.icon} ${colors.primary.enabledHoverText}`}
          >
            <ChevronLeft className="w-5 h-5" />
          </button>
          <span className={`text-[12px] font-medium ${colors.neutral.text600} min-w-[64px] text-center`}>
            Trang {page}/{totalPages}
          </span>
          <button
            type="button"
            onClick={() => setPage((p) => Math.min(totalPages, p + 1))}
            disabled={page >= totalPages}
            aria-label="Trang sau"
            className={`p-1.5 rounded-lg border ${colors.neutral.border200} ${colors.basic.bgWhite} transition-colors cursor-pointer disabled:opacity-40 disabled:cursor-default ${colors.action.icon} ${colors.primary.enabledHoverText}`}
          >
            <ChevronRight className="w-5 h-5" />
          </button>
        </div>
      </div>
    </div>
  );
}
