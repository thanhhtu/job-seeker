import { useState } from "react";
import { toast } from "react-hot-toast";
import { Button, PasswordInput } from "@/components/common";
import { ApiError } from "@/api";
import { changePassword } from "@/api/profile";

export function PasswordPanel() {
  const [currentPassword, setCurrentPassword] = useState("");
  const [newPassword, setNewPassword] = useState("");
  const [confirmPassword, setConfirmPassword] = useState("");
  const [saving, setSaving] = useState(false);

  const handleChangePassword = async (e: React.FormEvent) => {
    e.preventDefault();
    if (saving) return;
    if (newPassword.length < 8) {
      toast.error("Mật khẩu mới phải có ít nhất 8 ký tự");
      return;
    }
    if (newPassword !== confirmPassword) {
      toast.error("Mật khẩu xác nhận không khớp");
      return;
    }
    setSaving(true);
    try {
      await changePassword(currentPassword, newPassword);
      setCurrentPassword("");
      setNewPassword("");
      setConfirmPassword("");
      toast.success("Đã đổi mật khẩu");
    } catch (err) {
      if (err instanceof ApiError) toast.error(err.message);
      else toast.error("Đã xảy ra lỗi. Vui lòng thử lại");
    } finally {
      setSaving(false);
    }
  };

  return (
    <div className="flex-1 overflow-y-auto px-8 py-8">
      <form onSubmit={handleChangePassword} className="max-w-2xl flex flex-col gap-4">
        <PasswordInput
          value={currentPassword}
          onChange={(e) => setCurrentPassword(e.target.value)}
          placeholder="Mật khẩu hiện tại"
          autoComplete="current-password"
        />
        <PasswordInput
          value={newPassword}
          onChange={(e) => setNewPassword(e.target.value)}
          placeholder="Mật khẩu mới (tối thiểu 8 ký tự)"
          autoComplete="new-password"
        />
        <PasswordInput
          value={confirmPassword}
          onChange={(e) => setConfirmPassword(e.target.value)}
          placeholder="Xác nhận mật khẩu mới"
          autoComplete="new-password"
        />

        <div className="flex justify-end">
          <Button
            type="submit"
            isLoading={saving}
            loadingLabel="Đang lưu"
            disabled={!currentPassword || !newPassword || !confirmPassword}
            className="px-6 py-[10px] text-[14px]"
          >
            Đổi mật khẩu
          </Button>
        </div>
      </form>
    </div>
  );
}
