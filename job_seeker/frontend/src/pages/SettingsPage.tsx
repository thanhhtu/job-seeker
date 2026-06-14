import { Navigate, useNavigate, useParams } from "react-router-dom";
import { Spinner } from "@/components/common";
import { PasswordPanel } from "@/components/PasswordPanel";
import { ProfilePanel } from "@/components/ProfilePanel";
import { SavedJobsPanel } from "@/components/SavedJobsPanel";
import { SettingsSidebar } from "@/components/SettingsSidebar";
import { SECTION_META, SettingsSection, VALID_SECTIONS } from "@/constant/settings";
import { useAuth } from "@/hooks/useAuth";
import { colors } from "@/theme/colors";

export function SettingsPage() {
  const { user, isBootstrapping, accessToken, logout, updateUser } = useAuth();
  const navigate = useNavigate();
  const { section: sectionParam } = useParams<{ section: string }>();

  const isValidSection = VALID_SECTIONS.includes(sectionParam as SettingsSection);
  const section = sectionParam as SettingsSection;
  const setSection = (next: SettingsSection) => navigate(`/settings/${next}`);

  if (!isValidSection) {
    return <Navigate to="/settings/saved-jobs" replace />;
  }

  const meta = SECTION_META[section];

  return (
    <div className={`flex h-full w-full ${colors.page.shellBg} !p-4 gap-4 overflow-hidden font-sans`}>
      {/* Sidebar */}
      <div
        className={`w-[320px] shrink-0 ${colors.basic.bgWhite} rounded-[28px] border ${colors.neutral.border100} flex flex-col overflow-y-hidden overflow-x-visible`}
        style={{ boxShadow: "rgba(0, 0, 0, 0.09) 0px 3px 12px" }}
      >
        <SettingsSidebar
          user={user}
          active={section}
          onSelect={setSection}
          onLogout={() => {
            logout();
            navigate("/chat");
          }}
        />
      </div>

      {/* Content */}
      <div
        className={`flex-1 ${colors.basic.bgWhite} rounded-[28px] flex flex-col overflow-hidden`}
        style={{ boxShadow: "rgba(0, 0, 0, 0.09) 0px 3px 12px" }}
      >
        <div className={`shrink-0 px-8 pt-8 pb-4 border-b ${colors.neutral.border100}`}>
          <h1 className={`text-[22px] font-bold ${colors.neutral.text900}`}>{meta.title}</h1>
          <p className={`text-[13px] ${colors.neutral.text500} mt-1`}>{meta.subtitle}</p>
        </div>

        {isBootstrapping ? (
          <div className="flex-1 flex items-center justify-center">
            <Spinner size="md" colorVariant="muted" aria-label="Đang tải" />
          </div>
        ) : !accessToken ? (
          <div className="flex-1 flex flex-col items-center justify-center gap-3 px-8 text-center">
            <p className={`text-[15px] font-semibold ${colors.neutral.text700}`}>
              Bạn cần đăng nhập để xem trang này
            </p>
            <button
              type="button"
              onClick={() => navigate("/chat")}
              className={`text-[13px] font-semibold ${colors.primary.text} hover:underline cursor-pointer`}
            >
              Tới trang đăng nhập
            </button>
          </div>
        ) : section === "profile" ? (
          <ProfilePanel user={user} onUserUpdate={updateUser} />
        ) : section === "password" ? (
          <PasswordPanel />
        ) : (
          <SavedJobsPanel />
        )}
      </div>
    </div>
  );
}
