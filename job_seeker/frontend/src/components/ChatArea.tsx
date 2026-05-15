import { FormEvent, useEffect, useRef } from "react";
import { ChatMessage } from "@/types/chat";
import { MessageBubble } from "./MessageBubble";
import { Brain, Send } from "lucide-react";

type Props = {
  messages: ChatMessage[];
  sessionId: string | null;
  input: string;
  isSending: boolean;
  onInputChange: (v: string) => void;
  onSend: (e: FormEvent) => void;
};

export function ChatArea({ messages, input, isSending, onInputChange, onSend }: Props) {
  const scrollRef = useRef<HTMLDivElement>(null);

  useEffect(() => {
    scrollRef.current?.scrollIntoView({ behavior: "smooth" });
  }, [messages, isSending]);

  return (
    <main className="flex flex-col h-full bg-[#f8faff] relative">
      {/* Messages */}
      <div className="flex-1 overflow-y-auto px-6 py-10 md:px-12">
        <div className="max-w-6xl mx-auto space-y-3">
          {messages.length === 0 && (
            <div className="text-center text-slate-400 mt-20">
              <p>Hệ thống chatbot có thể hỗ trợ tìm kiếm việc làm Công nghệ Thông tin...</p>
            </div>
          )}
          {messages.map((m, i) => (
            <MessageBubble key={i} message={m} />
          ))}
          {isSending && (
            <div className="flex items-end gap-2">
              <div className="w-8 h-8 rounded-full bg-white flex items-center justify-center shrink-0 border border-slate-200 shadow-sm">
                <span className="text-[10px] font-black text-indigo-600">AI</span>
              </div>
              <div className="bg-white border border-slate-100 px-4 py-2.5 rounded-2xl rounded-bl-sm shadow-sm">
                <div className="flex gap-1 items-center h-5">
                  <span className="w-2 h-2 rounded-full bg-indigo-400 animate-bounce [animation-delay:0ms]" />
                  <span className="w-2 h-2 rounded-full bg-indigo-400 animate-bounce [animation-delay:150ms]" />
                  <span className="w-2 h-2 rounded-full bg-indigo-400 animate-bounce [animation-delay:300ms]" />
                </div>
              </div>
            </div>
          )}
          <div ref={scrollRef} className="h-4" />
        </div>
      </div>

      {/* Input bar */}
      <div className="w-full px-6 absolute bottom-0 left-0 right-0">
        <div className="max-w-4xl mx-auto pb-8">
          <form
            onSubmit={onSend}
            className="flex items-center bg-white rounded-[40px] p-2 pl-6 shadow-[0_8px_30px_rgb(0,0,0,0.06)] border border-slate-50 transition-all focus-within:shadow-[0_10px_40px_rgba(0,0,0,0.1)]"
          >
            <div className="shrink-0 flex items-center justify-center mr-3">
              <span className="text-2xl filter">
                <Brain className="w-6 h-6" />
              </span>
            </div>
            <input
              type="text"
              placeholder="Nhập tin nhắn..."
              value={input}
              onChange={(e) => onInputChange(e.target.value)}
              className="flex-1 bg-transparent py-3 text-[15px] text-slate-600 placeholder:text-slate-400 border-0 border-none focus:ring-0 focus:outline-none"
            />
            <button
              type="submit"
              disabled={!input.trim() || isSending}
              className="max-w-12 h-12 !rounded-full hover:bg-indigo-600 text-white flex items-center justify-center shadow-[0_5px_15px_rgba(93,95,239,0.4)] transition-all active:scale-95 disabled:opacity-40 shrink-0 ml-2"
            >
              {isSending ? (
                <div className="w-5 h-5 border-2 border-white/30 border-t-white rounded-full animate-spin" />
              ) : (
                <Send className="w-6 h-6" />
              )}
            </button>
          </form>
        </div>
      </div>
    </main>
  );
}
