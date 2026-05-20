import { SESSION_PLACEHOLDER_TITLE } from "@/constant/placeholder";
import { colors } from "@/theme/colors";

export function getSessionDisplayTitle(title?: string | null): string {
  const trimmed = title?.trim();
  if (trimmed) {
    return trimmed;
  }
  return SESSION_PLACEHOLDER_TITLE;
}

export function isPlaceholderSessionTitle(title: string): boolean {
  return title === SESSION_PLACEHOLDER_TITLE;
}

export function placeholderSessionTitleClassName(): string {
  return `italic ${colors.neutral.text400}`;
}
