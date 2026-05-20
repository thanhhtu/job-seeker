import { FormEvent, KeyboardEvent, useEffect, useRef } from "react";
import { ChatMessage } from "@/types/chat";
import { Brain, Send, Sparkles } from "lucide-react";
import { MessageBubble } from "./MessageBubble";
import { Button, Textarea } from "./common";
import { colors } from "@/theme/colors";

function TypingIndicator() {
  return (
    <div className="flex gap-3 w-full" aria-live="polite" aria-label="AI đang trả lời">
      <Sparkles className={`w-6 h-6 shrink-0 mt-1 ${colors.primary.text}`} strokeWidth={2} />
      <>
        <style>{`
          @keyframes dot-bounce {
            0%   { transform: translateY(0) scaleY(1); animation-timing-function: cubic-bezier(0.45, 0, 0.55, 0); }
            35%  { transform: translateY(-8px) scaleY(0.97); animation-timing-function: cubic-bezier(0.22, 1, 0.36, 1); }
            55%  { transform: translateY(1px) scaleY(0.94); animation-timing-function: cubic-bezier(0.22, 1, 0.36, 1); }
            75%  { transform: translateY(0) scaleY(1); }
            100% { transform: translateY(0) scaleY(1); }
          }
        `}</style>
        <span className="inline-flex items-center gap-1">
          {[0, 150, 300].map((delay) => (
            <span
              key={delay}
              className="w-1.5 h-1.5 rounded-full bg-current"
              style={{ animation: `dot-bounce 1.5s ${delay}ms infinite ease-in-out` }}
            />
          ))}
        </span>
      </>
    </div>
  );
}

type Props = {
  messages: ChatMessage[];
  sessionId: string | null;
  input: string;
  isSending: boolean;
  onInputChange: (v: string) => void;
  onSend: (text: string) => void;
};

export function ChatArea({ messages, input, isSending, onInputChange, onSend }: Props) {
  const scrollRef = useRef<HTMLDivElement>(null);
  const formRef = useRef<HTMLFormElement>(null);

  const handleSubmit = (e: FormEvent) => {
    e.preventDefault();
    const textarea = formRef.current?.querySelector("textarea");
    const text = (textarea?.value ?? input).trim();
    if (!text || isSending) {
      return;
    }
    onSend(text);
  };

  const handleKeyDown = (e: KeyboardEvent<HTMLTextAreaElement>) => {
    if (e.key !== "Enter" || e.shiftKey || e.nativeEvent.isComposing) {
      return;
    }
    const text = e.currentTarget.value.trim();
    if (!text || isSending) {
      return;
    }
    formRef.current?.requestSubmit();
  };

  useEffect(() => {
    scrollRef.current?.scrollIntoView({ behavior: "smooth" });
  }, [messages, isSending]);

  return (
    <main className={`flex flex-col h-full ${colors.basic.bgWhite} relative`}>
      <div className="flex-1 overflow-y-auto px-4 py-6 md:px-8 pb-32">
        <div className="max-w-7xl mx-auto space-y-4">
          {messages.length === 0 && (
            <div className={`text-[15px] text-center ${colors.neutral.text400} mt-24`}>
              <p>Hệ thống chatbot hỗ trợ tìm kiếm việc làm Công nghệ Thông tin.</p>
            </div>
          )}
          {messages.map((m, i) => (
            <MessageBubble key={i} message={m} />
          ))}
          {isSending && <TypingIndicator />}
          <div ref={scrollRef} className="h-4" />
        </div>
      </div>

      {/* Input bar */}
      <div className="w-full px-6 absolute bottom-0 left-0 right-0">
        <div className="max-w-5xl mx-auto pb-8">
          <form
            ref={formRef}
            onSubmit={handleSubmit}
            className={`flex items-center ${colors.basic.bgWhite} rounded-[40px] p-2 pl-6 shadow-[0_8px_30px_rgb(0,0,0,0.06)] border ${colors.neutral.border50} transition-all focus-within:shadow-[0_10px_40px_rgba(0,0,0,0.1)]`}
          >
            <div className="shrink-0 flex items-center justify-center mr-3">
              <span className="text-2xl filter">
                <Brain className="w-6 h-6" />
              </span>
            </div>
            <Textarea
              placeholder="Nhập câu hỏi..."
              value={input}
              variant="secondary"
              className="flex-1 py-3 text-[13px]"
              onChange={(e) => onInputChange(e.target.value)}
              onKeyDown={handleKeyDown}
            />
            <Button
              type="submit"
              disabled={!input.trim() || isSending}
            >
              {isSending ? (
                <div className={`w-6 h-6 border-2 ${colors.basic.borderWhiteSoft} ${colors.basic.borderTopWhite} rounded-full animate-spin`} />
              ) : (
                <Send className="w-6 h-6" />
              )}
            </Button>
          </form>
        </div>
      </div>
    </main>
  );
}
