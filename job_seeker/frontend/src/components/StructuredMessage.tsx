import { AssistantData } from "@/types/chat";
import { JobCard } from "@/components/JobCard";
import { CopyButton, useCopy } from "@/components/CopyButton";
import { renderInlineBold } from "@/utils/renderInlineBold";
import { Briefcase, ChevronRight, Sparkles, Star } from "lucide-react";
import { colors } from "@/theme/colors";

export function StructuredMessage({ data }: { data: AssistantData }) {
  const { copied, handleCopy } = useCopy(
    data.match_summary ?? data.message ?? ""
  );

  if (data.type === "clarification" || data.type === "no_results") {
    return (
      <div className="group flex gap-3 w-full">
        <Sparkles className={`w-6 h-6 shrink-0 mt-1 ${colors.primary.text}`} strokeWidth={2} />
        <div className="flex-1 min-w-0">
          <p className="text-[15px] text-[#1e293b] leading-relaxed">
            {renderInlineBold(data.message ?? "")}
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
        {data.match_summary && (
          <p className="text-[15px] text-[#1e293b] leading-relaxed">
            {renderInlineBold(data.match_summary)}
          </p>
        )}

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
                    {renderInlineBold(rec.reason)}
                  </span>
                </div>
              ))}
            </div>
          </div>
        )}

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
