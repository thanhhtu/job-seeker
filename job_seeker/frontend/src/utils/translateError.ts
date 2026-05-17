const EXACT_MESSAGES: Record<string, string> = {
  "Email already registered.": "Email này đã được đăng ký.",
  "Incorrect email or password.": "Email hoặc mật khẩu không chính xác.",
  "Refresh token expired": "Token làm mới đã hết hạn.",
  "Invalid refresh token": "Token làm mới không hợp lệ.",
  "User no longer exists": "Tài khoản không còn tồn tại.",
  "Not authenticated": "Chưa đăng nhập.",
  "Token expired": "Token đã hết hạn.",
  "Invalid token": "Token không hợp lệ.",
  "Invalid token subject": "Token không hợp lệ.",
  "Message must not be empty.": "Tin nhắn không được để trống.",
  "Session does not belong to user.": "Phiên chat không thuộc về người dùng này.",
  "Session not found.": "Không tìm thấy phiên chat.",
  "You do not have access to this session.": "Bạn không có quyền truy cập phiên chat này.",
};

const PARTIAL_MESSAGES: Array<{ match: RegExp; vi: string }> = [
  { match: /at least 8 character/i, vi: "Mật khẩu phải có ít nhất 8 ký tự." },
  { match: /valid email/i, vi: "Email không hợp lệ." },
  { match: /field required/i, vi: "Vui lòng điền đầy đủ thông tin." },
];

const VIETNAMESE_RE =
  /[àáảãạăằắẳẵặâầấẩẫậèéẻẽẹêềếểễệìíỉĩịòóỏõọôồốổỗộơờớởỡợùúủũụưừứửữựỳýỷỹỵđ]/i;

function translatePart(message: string): string {
  const trimmed = message.trim();
  if (!trimmed) return trimmed;

  const exact = EXACT_MESSAGES[trimmed];
  if (exact) return exact;

  const httpMatch = /^HTTP (\d+)$/.exec(trimmed);
  if (httpMatch) return `Đã xảy ra lỗi (mã ${httpMatch[1]}).`;

  for (const { match, vi } of PARTIAL_MESSAGES) {
    if (match.test(trimmed)) return vi;
  }

  if (VIETNAMESE_RE.test(trimmed)) return trimmed;

  return "Đã xảy ra lỗi. Vui lòng thử lại.";
}

/** Map known English API / validation messages to Vietnamese for toasts. */
export function translateApiMessage(message: string): string {
  if (message.includes("; ")) {
    return message
      .split("; ")
      .map((part) => translatePart(part))
      .join("; ");
  }
  return translatePart(message);
}
