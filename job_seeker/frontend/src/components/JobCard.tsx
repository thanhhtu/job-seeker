import { JobCard as JobCardType } from "@/types/chat";
import { useSavedJobs } from "@/hooks/useSavedJobs";
import { Bookmark, Building2, ExternalLink, MapPin, Wallet } from "lucide-react";
import { formatSalary } from "@/utils/utils";
import { colors } from "@/theme/colors";

export function JobCard({ job, index }: { job: JobCardType; index: number }) {
  const salary = formatSalary(job);
  const meta: string[] = [];
  if (job.work_mode) meta.push(job.work_mode.charAt(0).toUpperCase() + job.work_mode.slice(1));
  if (job.experience_years_min) meta.push(`${job.experience_years_min} + năm KN`);

  const savedJobs = useSavedJobs();
  const canBookmark = Boolean(savedJobs?.enabled && job.id);
  const saved = canBookmark ? savedJobs!.isSaved(job.id!) : false;
  const bookmarkBusy = canBookmark ? savedJobs!.isBusy(job.id!) : false;

  return (
    <div className={`rounded-2xl border ${colors.neutral.border200} ${colors.basic.bgWhite} p-4 hover:shadow-md transition-shadow`}>
      <div className="flex items-start gap-3">
        <span className={`shrink-0 flex items-center justify-center w-7 h-7 rounded-full ${colors.primary.xLightBg} ${colors.primary.text} text-xs font-bold`}>
          {index}
        </span>
        <div className="flex-1 min-w-0 space-y-2">
          <div className="flex items-start gap-2">
            <div className="flex-1 min-w-0">
              <h4 className={`font-semibold text-[15px] ${colors.neutral.text900} leading-snug`}>{job.title}</h4>
              <p className={`flex items-center gap-1.5 text-[13px] ${colors.neutral.text600} mt-0.5`}>
                <Building2 className="w-3.5 h-3.5 shrink-0" />
                {job.company_name}
              </p>
            </div>
            {canBookmark && (
              <button
                type="button"
                onClick={() => void savedJobs!.toggleSave(job.id!)}
                disabled={bookmarkBusy}
                aria-label={saved ? "Bỏ lưu công việc" : "Lưu công việc"}
                aria-pressed={saved}
                title={saved ? "Bỏ lưu" : "Lưu công việc"}
                className={`shrink-0 p-1.5 rounded-full transition-colors cursor-pointer disabled:opacity-50 ${
                  saved
                    ? `${colors.primary.text} ${colors.primary.hoverTextStrong}`
                    : `${colors.neutral.text400} ${colors.primary.hoverText}`
                }`}
              >
                <Bookmark className="w-5 h-5" fill={saved ? "currentColor" : "none"} />
              </button>
            )}
          </div>

          {job.locations && job.locations.length > 0 && (
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

          {meta.length > 0 && (
            <div className="flex flex-wrap gap-1.5">
              {meta.map((tag) => (
                <span key={tag} className={`px-2 py-0.5 rounded-full ${colors.neutral.bg100} text-[11px] ${colors.neutral.text600} font-medium`}>
                  {tag}
                </span>
              ))}
            </div>
          )}

          {job.skills && job.skills.length > 0 && (
            <div className="flex flex-wrap gap-1.5">
              {job.skills.map((skill) => (
                <span key={skill} className={`px-2 py-0.5 rounded-full ${colors.primary.xLightBg} text-[11px] ${colors.primary.text} font-medium`}>
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
              className={`inline-flex items-center gap-2 text-[13px] ${colors.primary.text} hover:underline font-medium mt-1`}
            >
              Xem chi tiết <ExternalLink className="w-5 h-5 pb-1" />
            </a>
          )}
        </div>
      </div>
    </div>
  );
}
