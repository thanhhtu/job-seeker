import { JobCard as JobCardType } from "@/types/chat";
import { Building2, ExternalLink, MapPin, Wallet } from "lucide-react";

function formatSalary(job: JobCardType): string | null {
  if (!job.salary_min && !job.salary_max) {
    return job.salary_negotiable ? "Thỏa thuận" : null;
  }
  const cur = job.salary_currency ?? "VND";
  const fmt = (n: number) => n.toLocaleString("vi-VN");
  if (job.salary_min && job.salary_max) return `${fmt(job.salary_min)} – ${fmt(job.salary_max)} ${cur}`;
  if (job.salary_min) return `Từ ${fmt(job.salary_min)} ${cur}`;
  return `Đến ${fmt(job.salary_max!)} ${cur}`;
}

export function JobCard({ job, index }: { job: JobCardType; index: number }) {
  const salary = formatSalary(job);
  const meta: string[] = [];
  if (job.work_mode) meta.push(job.work_mode.charAt(0).toUpperCase() + job.work_mode.slice(1));
  if (job.experience_years_min) meta.push(`${job.experience_years_min} + năm KN`);

  return (
    <div className="rounded-2xl border border-[#e2e8f0] bg-white p-4 hover:shadow-md transition-shadow">
      <div className="flex items-start gap-3">
        <span className="shrink-0 flex items-center justify-center w-7 h-7 rounded-full bg-[#eef2ff] text-[#4f46e5] text-xs font-bold">
          {index}
        </span>
        <div className="flex-1 min-w-0 space-y-2">
          <div>
            <h4 className="font-semibold text-[15px] text-[#0f172a] leading-snug">{job.title}</h4>
            <p className="flex items-center gap-1.5 text-[13px] text-[#475569] mt-0.5">
              <Building2 className="w-3.5 h-3.5 shrink-0" />
              {job.company_name}
            </p>
          </div>

          {job.locations && job.locations.length > 0 && (
            <p className="flex items-center gap-1.5 text-[13px] text-[#64748b]">
              <MapPin className="w-3.5 h-3.5 shrink-0" />
              {job.locations.join(", ")}
            </p>
          )}

          {salary && (
            <p className="flex items-center gap-1.5 text-[13px] font-medium text-[#22c55e]">
              <Wallet className="w-3.5 h-3.5 shrink-0" />
              {salary}
            </p>
          )}

          {meta.length > 0 && (
            <div className="flex flex-wrap gap-1.5">
              {meta.map((tag) => (
                <span key={tag} className="px-2 py-0.5 rounded-full bg-[#f1f5f9] text-[11px] text-[#475569] font-medium">
                  {tag}
                </span>
              ))}
            </div>
          )}

          {job.skills && job.skills.length > 0 && (
            <div className="flex flex-wrap gap-1.5">
              {job.skills.map((skill) => (
                <span key={skill} className="px-2 py-0.5 rounded-full bg-[#eef2ff] text-[11px] text-[#4f46e5] font-medium">
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
              className="inline-flex items-center gap-1 text-[13px] text-[#4f46e5] hover:underline font-medium mt-1"
            >
              Xem chi tiết <ExternalLink className="w-3.5 h-3.5" />
            </a>
          )}
        </div>
      </div>
    </div>
  );
}
