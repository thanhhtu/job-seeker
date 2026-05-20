import type { ApiErrorPayload } from "@/types/apiError";

function isApiErrorBody(value: unknown): value is ApiErrorPayload {
  if (!value || typeof value !== "object") {
    return false;
  }
  const o = value as Record<string, unknown>;
  return typeof o.message_code === "string" && typeof o.message === "string";
}

function validationFallback(errors: Array<{ msg?: string }>): ApiErrorPayload {
  const message = errors.map((i) => i.msg).filter(Boolean).join("; ") || "Validation failed.";
  return { message_code: "VALIDATION_ERROR", message };
}

export async function parseError(res: Response): Promise<ApiErrorPayload> {
  const text = await res.text();
  if (!text) {
    return {
      message_code: "UNKNOWN_ERROR",
      message: `HTTP ${res.status}`,
    };
  }
  try {
    const data = JSON.parse(text) as { detail?: unknown };
    const detail = data.detail;

    if (isApiErrorBody(detail)) {
      return detail;
    }

    if (typeof detail === "string") {
      return { message_code: "UNKNOWN_ERROR", message: detail };
    }

    if (Array.isArray(detail)) {
      return validationFallback(detail as Array<{ msg?: string }>);
    }

    return { message_code: "UNKNOWN_ERROR", message: text };
  } catch {
    return { message_code: "UNKNOWN_ERROR", message: text };
  }
}
