import { AssistantData, ChatMessage, JobCard as JobCardType } from "@/types/chat";
import {
  Briefcase,
  Building2,
  Check,
  ChevronRight,
  Copy,
  ExternalLink,
  MapPin,
  Sparkles,
  Star,
  Wallet,
} from "lucide-react";
import { useState } from "react";
import { colors } from "@/theme/colors";

function useCopy(content: string) {
  const [copied, setCopied] = useState(false);
  const handleCopy = () => {
    navigator.clipboard.writeText(content);
    setCopied(true);
    setTimeout(() => setCopied(false), 2000);
  };
  return { copied, handleCopy };
}

function CopyButton({ copied, onCopy }: { copied: boolean; onCopy: () => void }) {
  return (
    <button
      type="button"
      onClick={onCopy}
      className={`p-1 mt-1 rounded-md opacity-0 group-hover:opacity-100 transition-opacity cursor-pointer ${colors.neutral.hoverBg50}`}
      title="Sao chép"
    >
      {copied ? (
        <Check className={`w-4 h-4 ${colors.status.success}`} />
      ) : (
        <Copy className={`w-4 h-4 ${colors.neutral.text400}`} />
      )}
    </button>
  );
}

/* ── Salary formatter ── */
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

/* ── Job card component ── */
function JobCard({ job, index }: { job: JobCardType; index: number }) {
  const salary = formatSalary(job);
  const meta: string[] = [];
  if (job.work_mode) meta.push(job.work_mode.charAt(0).toUpperCase() + job.work_mode.slice(1));
  if (job.job_level) meta.push(job.job_level.charAt(0).toUpperCase() + job.job_level.slice(1));
  if (job.experience_years_min) meta.push(`${job.experience_years_min}+ năm KN`);

  return (
    <div className="rounded-2xl border border-[#e2e8f0] bg-white p-4 hover:shadow-md transition-shadow">
      <div className="flex items-start gap-3">
        <span className="shrink-0 flex items-center justify-center w-7 h-7 rounded-full bg-[#eef2ff] text-[#4f46e5] text-xs font-bold">
          {index}
        </span>
        <div className="flex-1 min-w-0 space-y-2">
          {/* Title + company */}
          <div>
            <h4 className="font-semibold text-[15px] text-[#0f172a] leading-snug">{job.title}</h4>
            <p className="flex items-center gap-1.5 text-[13px] text-[#475569] mt-0.5">
              <Building2 className="w-3.5 h-3.5 shrink-0" />
              {job.company_name}
            </p>
          </div>

          {/* Location */}
          {job.locations && job.locations.length > 0 && (
            <p className="flex items-center gap-1.5 text-[13px] text-[#64748b]">
              <MapPin className="w-3.5 h-3.5 shrink-0" />
              {job.locations.join(", ")}
            </p>
          )}

          {/* Salary */}
          {salary && (
            <p className="flex items-center gap-1.5 text-[13px] font-medium text-[#22c55e]">
              <Wallet className="w-3.5 h-3.5 shrink-0" />
              {salary}
            </p>
          )}

          {/* Meta tags */}
          {meta.length > 0 && (
            <div className="flex flex-wrap gap-1.5">
              {meta.map((tag) => (
                <span key={tag} className="px-2 py-0.5 rounded-full bg-[#f1f5f9] text-[11px] text-[#475569] font-medium">
                  {tag}
                </span>
              ))}
            </div>
          )}

          {/* Skills */}
          {job.skills && job.skills.length > 0 && (
            <div className="flex flex-wrap gap-1.5">
              {job.skills.map((skill) => (
                <span key={skill} className="px-2 py-0.5 rounded-full bg-[#eef2ff] text-[11px] text-[#4f46e5] font-medium">
                  {skill}
                </span>
              ))}
            </div>
          )}

          {/* Apply link */}
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

/* ── Structured assistant message ── */
function StructuredMessage({ data }: { data: AssistantData }) {
  const { copied, handleCopy } = useCopy(
    data.match_summary ?? data.message ?? ""
  );

  if (data.type === "clarification" || data.type === "no_results") {
    return (
      <div className="group flex gap-3 w-full">
        <Sparkles className={`w-6 h-6 shrink-0 mt-1 ${colors.primary.text}`} strokeWidth={2} />
        <div className="flex-1 min-w-0">
          <p className="text-[15px] text-[#1e293b] leading-relaxed">
            {data.message}
          </p>
          <CopyButton copied={copied} onCopy={handleCopy} />
        </div>
      </div>
    );
  }

  const jobs = data.jobs ?? [];
  const recommendations = data.recommendations ?? [];
  const actions = data.suggested_actions ?? [];

  return (
    <div className="group flex gap-3 w-full">
      <Sparkles className={`w-6 h-6 shrink-0 mt-1 ${colors.primary.text}`} strokeWidth={2} />
      <div className="flex-1 min-w-0 space-y-4">
        {/* Match summary */}
        {data.match_summary && (
          <p className="text-[15px] text-[#1e293b] leading-relaxed">
            {data.match_summary}
          </p>
        )}

        {/* Recommendations */}
        {recommendations.length > 0 && (
          <div className="space-y-1.5">
            <h3 className="flex items-center gap-1.5 text-[13px] font-semibold text-[#475569] uppercase tracking-wide">
              <Star className="w-3.5 h-3.5" /> Gợi ý
            </h3>
            <div className="space-y-1">
              {recommendations.map((rec) => (
                <div key={rec.rank} className="flex items-start gap-2 text-[13px] text-[#334155]">
                  <ChevronRight className="w-4 h-4 text-[#4f46e5] shrink-0 mt-0.5" />
                  <span>
                    <span className="font-semibold">{rec.title}</span>
                    <span className="text-[#64748b]"> – {rec.company}:</span>{" "}
                    {rec.reason}
                  </span>
                </div>
              ))}
            </div>
          </div>
        )}

        {/* Job cards */}
        {jobs.length > 0 && (
          <div className="space-y-1.5">
            <h3 className="flex items-center gap-1.5 text-[13px] font-semibold text-[#475569] uppercase tracking-wide">
              <Briefcase className="w-3.5 h-3.5" /> {jobs.length} công việc phù hợp
            </h3>
            <div className="grid gap-3">
              {jobs.map((job, i) => (
                <JobCard key={`${job.title}-${job.company_name}-${i}`} job={job} index={i + 1} />
              ))}
            </div>
          </div>
        )}

        {/* Suggested actions */}
        {actions.length > 0 && (
          <div className="flex flex-wrap gap-2">
            {actions.map((action) => (
              <span
                key={action}
                className="px-3 py-1.5 rounded-full bg-[#f1f5f9] text-[12px] text-[#475569] font-medium"
              >
                {action}
              </span>
            ))}
          </div>
        )}

        <CopyButton copied={copied} onCopy={handleCopy} />
      </div>
    </div>
  );
}

/* ── Plain text fallback (markdown) ── */
function renderInlineBold(line: string) {
  const parts = line.split(/\*\*(.+?)\*\*/g);
  return parts.map((part, idx) =>
    idx % 2 === 1 ? (
      <span key={idx} className="font-semibold">{part}</span>
    ) : (
      part
    )
  );
}

function parseMarkdown(text: string) {
  const lines = text.split("\n");
  const elements: React.ReactNode[] = [];
  let i = 0;
  while (i < lines.length) {
    const line = lines[i];
    const numberedBold = line.match(/^(\d+)\.\s+\*\*(.+?)\*\*[:\s]*(.*)/);
    if (numberedBold) {
      const [, num, bold, rest] = numberedBold;
      elements.push(
        <div key={i} className="flex gap-2 py-1">
          <span className={`shrink-0 font-medium ${colors.neutral.text600}`}>{num}.</span>
          <p className={`leading-relaxed ${colors.neutral.text800}`}>
            <span className="font-semibold">{bold}:</span>{rest ? ` ${rest}` : ""}
          </p>
        </div>
      );
      i++;
      continue;
    }
    const plainNumbered = line.match(/^(\d+)\.\s+(.*)/);
    if (plainNumbered) {
      const [, num, rest] = plainNumbered;
      elements.push(
        <div key={i} className="flex gap-2 py-1">
          <span className={`shrink-0 font-medium ${colors.neutral.text600}`}>{num}.</span>
          <p className={`leading-relaxed ${colors.neutral.text800}`}>{rest}</p>
        </div>
      );
      i++;
      continue;
    }
    if (line.match(/^[-*•]\s+/)) {
      const rest = line.replace(/^[-*•]\s+/, "");
      elements.push(
        <li key={i} className={`leading-relaxed ${colors.neutral.text800} ml-5 list-disc`}>
          {renderInlineBold(rest)}
        </li>
      );
      i++;
      continue;
    }
    if (line.includes("**")) {
      elements.push(
        <p key={i} className={`leading-relaxed ${colors.neutral.text800} py-0.5`}>{renderInlineBold(line)}</p>
      );
      i++;
      continue;
    }
    if (line.trim() === "") {
      elements.push(<div key={i} className="h-1" />);
      i++;
      continue;
    }
    elements.push(
      <p key={i} className={`leading-relaxed ${colors.neutral.text800} py-0.5`}>{line}</p>
    );
    i++;
  }
  return elements;
}

/* ── User message ── */
function UserMessage({ content }: { content: string }) {
  const { copied, handleCopy } = useCopy(content);
  return (
    <div className="group flex justify-end w-full">
      <div className="flex flex-col items-end gap-1 max-w-[min(85%,42rem)]">
        <div
          className={`rounded-[20px] px-4 py-2.5 ${colors.primary.xLightBg} ${colors.neutral.text800} text-[15px] leading-relaxed`}
        >
          {content}
        </div>
        <CopyButton copied={copied} onCopy={handleCopy} />
      </div>
    </div>
  );
}

/* ── Plain assistant message (history / fallback) ── */
function PlainAssistantMessage({ content }: { content: string }) {
  const { copied, handleCopy } = useCopy(content);
  return (
    <div className="group flex gap-3 w-full">
      <Sparkles className={`w-6 h-6 shrink-0 mt-1 ${colors.primary.text}`} strokeWidth={2} />
      <div className="flex-1 min-w-0">
        <div className="space-y-0.5 text-[15px]">{parseMarkdown(content)}</div>
        <CopyButton copied={copied} onCopy={handleCopy} />
      </div>
    </div>
  );
}

/* ── Entry point ── */
export function MessageBubble({ message }: { message: ChatMessage }) {
  if (message.role === "user") {
    return <UserMessage content={message.content} />;
  }
  if (message.data) {
    return <StructuredMessage data={message.data} />;
  }
  return <PlainAssistantMessage content={message.content} />;
}
