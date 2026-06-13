import { useNavigate } from "react-router-dom";
import { ArrowLeft, LogOut } from "lucide-react";
import { SETTINGS_NAV_ITEMS, SettingsSection } from "@/constant/settings";
import { UserInfo } from "@/types/user";
import { colors } from "@/theme/colors";

type Props = {
  user: UserInfo | null;
  active: SettingsSection;
  onSelect: (section: SettingsSection) => void;
  onLogout: () => void;
};

export function SettingsSidebar({ user, active, onSelect, onLogout }: Props) {
  const navigate = useNavigate();

  return (
    <aside className="flex flex-col h-full my-10">
      <div className="pb-4 shrink-0">
        <h1 className={`mx-10 text-[19px] font-bold tracking-tight ${colors.neutral.text900} mb-6`}>
          JOB SEEKER
        </h1>

        <button
          type="button"
          onClick={() => navigate("/chat")}
          className={`mx-10 my-4 inline-flex items-center gap-2 text-[13px] font-medium ${colors.neutral.text500} ${colors.primary.hoverText} transition-colors cursor-pointer mt-2`}
        >
          <ArrowLeft className="w-5 h-5" />
          Quay lại trò chuyện
        </button>
      </div>

      <nav className="flex-1 overflow-y-auto mx-5 min-h-0 space-y-1">
        {SETTINGS_NAV_ITEMS.map(({ key, label, icon: Icon }) => {
          const isActive = active === key;
          return (
            <button
              key={key}
              type="button"
              onClick={() => onSelect(key)}
              className={`w-full flex items-center gap-3 rounded-2xl px-5 py-3 font-semibold text-[13px] transition-colors cursor-pointer ${
                isActive
                  ? `${colors.primary.xLightBg} ${colors.primary.text}`
                  : `${colors.neutral.text700} ${colors.neutral.hoverBg50}`
              }`}
            >
              <Icon className="w-5 h-5 shrink-0" />
              {label}
            </button>
          );
        })}
      </nav>

      {user && (
        <div className={`mx-10 pt-4 shrink-0 border-t ${colors.neutral.border100} pb-2`}>
          <div className={`flex items-center gap-3 rounded-full px-3 py-2.5 ${colors.neutral.hoverBg50}`}>
            <div
              className={`w-9 h-9 rounded-full ${colors.primary.lightBg} flex items-center justify-center ${colors.primary.text} font-bold text-[15px] shrink-0`}
            >
              {user.email[0].toUpperCase()}
            </div>
            <span className={`font-semibold text-[13px] ${colors.neutral.text900} truncate flex-1 capitalize`}>
              {user.email.split("@")[0].replace(/[._]/g, " ")}
            </span>
            <button
              type="button"
              onClick={onLogout}
              aria-label="Đăng xuất"
              title="Đăng xuất"
              className={`p-1.5 rounded-full transition-colors cursor-pointer ${colors.action.icon} ${colors.action.hoverDelete}`}
            >
              <LogOut className="w-5 h-5" />
            </button>
          </div>
        </div>
      )}
    </aside>
  );
}
