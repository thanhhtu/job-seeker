import { CopyButton, useCopy } from "@/components/CopyButton";
import { colors } from "@/theme/colors";

export function UserMessage({ content }: { content: string }) {
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
