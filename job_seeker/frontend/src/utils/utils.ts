import { JobCard } from "@/types/chat";

export const formatSalary = (job: JobCard): string | null => {
  if (!job.salary_min && !job.salary_max) {
    return job.salary_negotiable ? "Thỏa thuận" : null;
  }
  const cur = job.salary_currency ?? "VND";
  const fmt = (n: number) => n.toLocaleString("vi-VN");
  if (job.salary_min && job.salary_max) return `${fmt(job.salary_min)} – ${fmt(job.salary_max)} ${cur}`;
  if (job.salary_min) return `Từ ${fmt(job.salary_min)} ${cur}`;
  return `Đến ${fmt(job.salary_max!)} ${cur}`;
}
