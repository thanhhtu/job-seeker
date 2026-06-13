import { Check, Copy } from "lucide-react";
import { useState } from "react";
import { colors } from "@/theme/colors";

export function useCopy(content: string) {
  const [copied, setCopied] = useState(false);
  const handleCopy = () => {
    navigator.clipboard.writeText(content);
    setCopied(true);
    setTimeout(() => setCopied(false), 2000);
  };
  return { copied, handleCopy };
}

export function CopyButton({ copied, onCopy }: { copied: boolean; onCopy: () => void }) {
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
