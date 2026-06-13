import { STATUS_LABELS, STATUS_ORDER } from "@/constant/savedJob";
import { SavedJob, SavedJobStatus } from "@/types/savedJob";
import { Building2, ExternalLink, MapPin, Trash2, Wallet } from "lucide-react";
import { colors } from "@/theme/colors";

const STATUS_BADGE: Record<SavedJobStatus, string> = {
  saved: `${colors.badge.indigoBg} ${colors.badge.indigoText}`,
  applied: `${colors.badge.blueBg} ${colors.badge.blueText}`,
  interviewing: `${colors.badge.amberBg} ${colors.badge.amberText}`,
  offer: `${colors.badge.greenBg} ${colors.badge.greenText}`,
  rejected: `${colors.badge.redBg} ${colors.badge.redText}`,
};

function formatSalary(job: SavedJob): string | null {
  if (!job.salary_min && !job.salary_max) {
    return job.salary_negotiable ? "Thỏa thuận" : null;
  }
  const cur = job.salary_currency ?? "VND";
  const fmt = (n: number) => n.toLocaleString("vi-VN");
  if (job.salary_min && job.salary_max) return `${fmt(job.salary_min)} – ${fmt(job.salary_max)} ${cur}`;
  if (job.salary_min) return `Từ ${fmt(job.salary_min)} ${cur}`;
  return `Đến ${fmt(job.salary_max!)} ${cur}`;
}

type Props = {
  job: SavedJob;
  onChangeStatus: (jobId: string, status: SavedJobStatus) => void;
  onRemove: (jobId: string) => void;
  busy?: boolean;
};

export function SavedJobCard({ job, onChangeStatus, onRemove, busy }: Props) {
  const salary = formatSalary(job);

  return (
    <div className={`rounded-2xl border ${colors.neutral.border200} ${colors.basic.bgWhite} p-4 hover:shadow-md transition-shadow`}>
      <div className="flex items-start justify-between gap-3">
        <div className="flex-1 min-w-0 space-y-2">
          <div>
            <h4 className={`font-semibold text-[15px] ${colors.neutral.text900} leading-snug`}>{job.title}</h4>
            <p className={`flex items-center gap-1.5 text-[13px] ${colors.neutral.text600} mt-0.5`}>
              <Building2 className="w-3.5 h-3.5 shrink-0" />
              {job.company_name}
            </p>
          </div>

          {job.locations.length > 0 && (
            <p className={`flex items-center gap-1.5 text-[13px] ${colors.neutral.text500}`}>
              <MapPin className="w-3.5 h-3.5 shrink-0" />
              {job.locations.join(", ")}
            </p>
          )}

          {salary && (
            <p className={`flex items-center gap-1.5 text-[13px] font-medium ${colors.status.success}`}>
              <Wallet className="w-3.5 h-3.5 shrink-0" />
              {salary}
            </p>
          )}

          {job.skills.length > 0 && (
            <div className="flex flex-wrap gap-1.5">
              {job.skills.slice(0, 8).map((skill) => (
                <span
                  key={skill}
                  className={`px-2 py-0.5 rounded-full ${colors.primary.xLightBg} text-[11px] ${colors.primary.text} font-medium`}
                >
                  {skill}
                </span>
              ))}
            </div>
          )}

          {job.url && (
            <a
              href={job.url}
              target="_blank"
              rel="noopener noreferrer"
              className={`inline-flex items-center gap-1 text-[13px] ${colors.primary.text} hover:underline font-medium mt-1`}
            >
              Xem chi tiết <ExternalLink className="w-3.5 h-3.5" />
            </a>
          )}
        </div>

        <div className="flex flex-col items-end gap-2 shrink-0">
          <span
            className={`px-2.5 py-0.5 rounded-full text-[11px] font-semibold ${STATUS_BADGE[job.status]}`}
          >
            {STATUS_LABELS[job.status]}
          </span>
          <button
            type="button"
            onClick={() => onRemove(job.job_id)}
            disabled={busy}
            aria-label="Bỏ lưu"
            title="Bỏ lưu"
            className={`p-1.5 rounded-full transition-colors cursor-pointer disabled:opacity-50 ${colors.action.icon} ${colors.action.hoverDelete}`}
          >
            <Trash2 className="w-5 h-5" />
          </button>
        </div>
      </div>

      <div className={`mt-3 pt-3 border-t ${colors.neutral.border100} flex items-center gap-2`}>
        <label className={`text-[12px] font-medium ${colors.neutral.text500}`}>Trạng thái:</label>
        <select
          value={job.status}
          disabled={busy}
          onChange={(e) => onChangeStatus(job.job_id, e.target.value as SavedJobStatus)}
          className={`text-[12px] font-medium ${colors.neutral.text700} rounded-lg border ${colors.neutral.border200} px-2 py-1 ${colors.basic.bgWhite} cursor-pointer disabled:opacity-50 focus:outline-none ${colors.primary.focusBorder}`}
        >
          {STATUS_ORDER.map((s) => (
            <option key={s} value={s}>
              {STATUS_LABELS[s]}
            </option>
          ))}
        </select>
      </div>
    </div>
  );
}
