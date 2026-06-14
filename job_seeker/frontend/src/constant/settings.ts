import { Bookmark, Lock, User } from "lucide-react";

export type SettingsSection = "profile" | "password" | "saved-jobs";

export const VALID_SECTIONS: SettingsSection[] = ["profile", "password", "saved-jobs"];

export const SECTION_META: Record<SettingsSection, { title: string; subtitle: string }> = {
  profile: {
    title: "Thông tin cá nhân",
    subtitle: "Thông tin tài khoản của bạn",
  },
  password: {
    title: "Đổi mật khẩu",
    subtitle: "Cập nhật mật khẩu đăng nhập của bạn",
  },
  "saved-jobs": {
    title: "Công việc đã lưu",
    subtitle: "Công việc đã lưu và theo dõi ứng tuyển",
  },
};

export const SETTINGS_NAV_ITEMS: {
  key: SettingsSection;
  label: string;
  icon: typeof User;
}[] = [
  { key: "profile", label: "Thông tin cá nhân", icon: User },
  { key: "password", label: "Đổi mật khẩu", icon: Lock },
  { key: "saved-jobs", label: "Công việc đã lưu", icon: Bookmark },
];
