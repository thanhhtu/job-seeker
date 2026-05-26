import { ChatMessage } from "@/types/chat";
import { StructuredMessage } from "@/components/StructuredMessage";
import { UserMessage } from "@/components/UserMessage";
import { PlainAssistantMessage } from "@/components/PlainAssistantMessage";

export function MessageBubble({ message }: { message: ChatMessage }) {
  if (message.role === "user") {
    return <UserMessage content={message.content} />;
  }
  if (message.data) {
    return <StructuredMessage data={message.data} />;
  }
  return <PlainAssistantMessage content={message.content} />;
}
