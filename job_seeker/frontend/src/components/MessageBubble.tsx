import { ChatMessage } from "@/types/chat";
import { Check, Copy, Sparkles } from "lucide-react";
import { useState } from "react";
import { colors } from "@/theme/colors";

function renderInlineBold(line: string) {
  const parts = line.split(/\*\*(.+?)\*\*/g);
  return parts.map((part, idx) =>
    idx % 2 === 1 ? (
      <span key={idx} className="font-semibold">
        {part}
      </span>
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
            <span className="font-semibold">{bold}:</span>
            {rest ? ` ${rest}` : ""}
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
        <p key={i} className={`leading-relaxed ${colors.neutral.text800} py-0.5`}>
          {renderInlineBold(line)}
        </p>
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
      <p key={i} className={`leading-relaxed ${colors.neutral.text800} py-0.5`}>
        {line}
      </p>
    );
    i++;
  }

  return elements;
}

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
        <Check className={`w-5 h-5 ${colors.status.success}`} />
      ) : (
        <Copy className={`w-5 h-5 ${colors.neutral.text400}`} />
      )}
    </button>
  );
}

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

function AssistantMessage({ content }: { content: string }) {
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

export function MessageBubble({ message }: { message: ChatMessage }) {
  if (message.role === "user") {
    return <UserMessage content={message.content} />;
  }
  return <AssistantMessage content={message.content} />;
}
