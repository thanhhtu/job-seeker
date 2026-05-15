import { ChatMessage } from "@/types/chat";
import { Copy, Check } from "lucide-react";
import { useState } from "react";

function parseMarkdown(text: string) {
  const lines = text.split("\n");
  const elements: React.ReactNode[] = [];
  let i = 0;

  while (i < lines.length) {
    const line = lines[i];

    // Numbered list: "1. **Bold:** rest"
    const numberedBold = line.match(/^(\d+)\.\s+\*\*(.+?)\*\*[:\s]*(.*)/);
    if (numberedBold) {
      const [, num, bold, rest] = numberedBold;
      elements.push(
        <div key={i} className="flex gap-3 py-1.5">
          <span className="shrink-0 w-6 h-6 rounded-full bg-indigo-50 text-indigo-600 text-[12px] font-semibold flex items-center justify-center mt-0.5">
            {num}
          </span>
          <p className="text-[14.5px] leading-relaxed text-slate-700">
            <span className="font-semibold text-slate-800">{bold}:</span>{" "}
            {rest}
          </p>
        </div>
      );
      i++;
      continue;
    }

    // Plain numbered list: "1. text"
    const plainNumbered = line.match(/^(\d+)\.\s+(.*)/);
    if (plainNumbered) {
      const [, num, rest] = plainNumbered;
      elements.push(
        <div key={i} className="flex gap-3 py-1.5">
          <span className="shrink-0 w-6 h-6 rounded-full bg-indigo-50 text-indigo-600 text-[12px] font-semibold flex items-center justify-center mt-0.5">
            {num}
          </span>
          <p className="text-[14.5px] leading-relaxed text-slate-700">{rest}</p>
        </div>
      );
      i++;
      continue;
    }

    // Inline bold **text**
    if (line.includes("**")) {
      const parts = line.split(/\*\*(.+?)\*\*/g);
      elements.push(
        <p key={i} className="text-[14.5px] leading-relaxed text-slate-700 py-0.5">
          {parts.map((part, idx) =>
            idx % 2 === 1 ? (
              <span key={idx} className="font-semibold text-slate-800">
                {part}
              </span>
            ) : (
              part
            )
          )}
        </p>
      );
      i++;
      continue;
    }

    // Empty line
    if (line.trim() === "") {
      elements.push(<div key={i} className="h-2" />);
      i++;
      continue;
    }

    // Normal text
    elements.push(
      <p key={i} className="text-[14.5px] leading-relaxed text-slate-700 py-0.5">
        {line}
      </p>
    );
    i++;
  }

  return elements;
}

export function MessageBubble({ message }: { message: ChatMessage }) {
  const isUser = message.role === "user";
  const [copied, setCopied] = useState(false);

  const handleCopy = () => {
    navigator.clipboard.writeText(message.content);
    setCopied(true);
    setTimeout(() => setCopied(false), 2000);
  };

  if (isUser) {
    return (
      <div className="group flex items-start gap-3 py-5 border-b border-slate-100/70">
        {/* User avatar */}
        <div className="shrink-0 w-8 h-8 rounded-full bg-slate-100 flex items-center justify-center shadow-[0_1px_4px_rgba(0,0,0,0.06)]">
          <svg className="w-4 h-4 text-slate-500" fill="currentColor" viewBox="0 0 24 24">
            <path d="M12 12c2.7 0 4.8-2.1 4.8-4.8S14.7 2.4 12 2.4 7.2 4.5 7.2 7.2 9.3 12 12 12zm0 2.4c-3.2 0-9.6 1.6-9.6 4.8v2.4h19.2v-2.4c0-3.2-6.4-4.8-9.6-4.8z" />
          </svg>
        </div>

        {/* Text */}
        <div className="flex-1 min-w-0">
          <p className="text-[14.5px] leading-relaxed text-slate-800 font-medium">
            {message.content}
          </p>
        </div>

        {/* Copy icon — visible on hover */}
        {/* <button
          onClick={handleCopy}
          className="shrink-0 opacity-0 group-hover:opacity-100 transition-opacity p-1.5 rounded hover:bg-slate-100"
          title="Copy"
        >
          {copied ? (
            <Check className="w-4 h-4 text-green-500" />
          ) : (
            <Copy className="w-4 h-4 text-slate-400" />
          )}
        </button> */}
      </div>
    );
  }

  return (
    <div className="group flex items-start gap-3 py-5 border-b border-slate-100/70">
      {/* AI avatar */}
      <div className="shrink-0 w-8 h-8 rounded-full bg-gradient-to-br from-indigo-100 to-indigo-50 flex items-center justify-center shadow-[0_1px_4px_rgba(93,95,239,0.15)]">
        <svg className="w-4 h-4 text-indigo-600" viewBox="0 0 24 24" fill="currentColor">
          <path d="M12 3C8.5 3 6 5.5 6 8c0 1.8 1 3.4 2.5 4.3V14l2-1.2c.5.1 1 .2 1.5.2s1-.1 1.5-.2L15 14v-1.7C16.5 11.4 18 9.8 18 8c0-2.5-2.5-5-6-5zm0 9c-.4 0-.7 0-1-.1l-.5.3-.5-.3c-.3.1-.7.1-1 .1C7 12 5 10.2 5 8c0-2.8 3-6 7-6s7 3.2 7 6c0 2.2-2 4-4 4z" />
        </svg>
      </div>

      {/* Content */}
      <div className="flex-1 min-w-0">
        {/* Header label */}
        <div className="flex items-center gap-1.5 mb-3">
          <span className="text-[11px] font-bold text-indigo-500 tracking-wider uppercase">
            CHAT Job Seeker
          </span>
          <span className="text-slate-300">·</span>
          <svg
            className="w-3.5 h-3.5 text-slate-400"
            fill="none"
            stroke="currentColor"
            viewBox="0 0 24 24"
          >
            <circle cx="12" cy="12" r="10" strokeWidth="2" />
            <path d="M12 8v4M12 16h.01" strokeWidth="2" strokeLinecap="round" />
          </svg>
        </div>

        {/* Parsed message */}
        <div className="space-y-0.5">{parseMarkdown(message.content)}</div>

        {/* Action row */}
        <div className="flex items-center gap-1 mt-4">
          {/* Copy */}
          <button
            onClick={handleCopy}
            className="p-1.5 rounded-md hover:bg-slate-100 transition-colors"
            title="Copy"
          >
            {copied ? (
              <Check className="w-4 h-4 text-green-500" />
            ) : (
              <Copy className="w-4 h-4 text-slate-400" />
            )}
          </button>
        </div>
      </div>
    </div>
  );
}