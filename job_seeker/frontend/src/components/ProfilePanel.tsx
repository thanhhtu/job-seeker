import { Mail } from "lucide-react";
import { UserInfo } from "@/types/user";
import { colors } from "@/theme/colors";

export function ProfilePanel({ user }: { user: UserInfo | null }) {
  if (!user) return null;
  const displayName = user.email.split("@")[0].replace(/[._]/g, " ");

  return (
    <div className="flex-1 overflow-y-auto px-8 py-8">
      <div className="max-w-2xl">
        <div className="flex items-center gap-4">
          <div
            className={`w-16 h-16 rounded-full ${colors.primary.lightBg} flex items-center justify-center ${colors.primary.text} font-bold text-[26px] shrink-0`}
          >
            {user.email[0].toUpperCase()}
          </div>
          <div className="min-w-0">
            <p className={`text-[18px] font-bold ${colors.neutral.text900} capitalize truncate`}>
              {displayName}
            </p>
            <p className={`text-[13px] ${colors.neutral.text500} truncate`}>{user.email}</p>
          </div>
        </div>

        <div className={`mt-8 rounded-2xl border ${colors.neutral.border200} divide-y ${colors.neutral.border100}`}>
          <div className="flex items-center gap-3 px-5 py-4">
            <Mail className={`w-5 h-5 shrink-0 ${colors.neutral.text400}`} />
            <div className="min-w-0">
              <p className={`text-[12px] font-medium ${colors.neutral.text400}`}>Email</p>
              <p className={`text-[14px] ${colors.neutral.text800} truncate`}>{user.email}</p>
            </div>
          </div>
        </div>
      </div>
    </div>
  );
}
