import { MESSAGES_BY_CODE } from "@/constant/errorMessages";
import type { ApiErrorPayload } from "@/types/apiError";

const VIETNAMESE_RE =
  /[àáảãạăằắẳẵặâầấẩẫậèéẻẽẹêềếểễệìíỉĩịòóỏõọôồốổỗộơờớởỡợùúủũụưừứửữựỳýỷỹỵđ]/i;

/** Map API error payload to Vietnamese for toasts (by message_code). */
export function translateApiError(payload: ApiErrorPayload): string {
  const byCode = MESSAGES_BY_CODE[payload.message_code];
  if (byCode) {
    return byCode;
  }

  const detail = payload.message.trim();
  if (detail && VIETNAMESE_RE.test(detail)) {
    return detail;
  }

  return MESSAGES_BY_CODE.UNKNOWN_ERROR;
}
