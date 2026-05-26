import { CopyButton, useCopy } from "@/components/CopyButton";
import { renderInlineBold } from "@/utils/renderInlineBold";
import { Sparkles } from "lucide-react";
import { colors } from "@/theme/colors";

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

export function PlainAssistantMessage({ content }: { content: string }) {
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
