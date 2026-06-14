import { useForm } from "react-hook-form";
import { zodResolver } from "@hookform/resolvers/zod";
import { Mail, Phone, User as UserIcon } from "lucide-react";
import { toast } from "react-hot-toast";
import { Button, Input } from "@/components/common";
import { ApiError } from "@/api";
import { updateProfile } from "@/api/profile";
import { profileSchema, type ProfileValues } from "@/schemas/profile";
import { UserInfo } from "@/types/user";
import { colors } from "@/theme/colors";

type ProfilePanelProps = {
  user: UserInfo | null;
  onUserUpdate: (next: UserInfo) => void;
};

export function ProfilePanel({ user, onUserUpdate }: ProfilePanelProps) {
  const {
    register,
    handleSubmit,
    reset,
    formState: { errors, isSubmitting, isDirty },
  } = useForm<ProfileValues>({
    resolver: zodResolver(profileSchema),
    mode: "onTouched",
    defaultValues: {
      name: user?.name ?? "",
      phone: user?.phone ?? "",
    },
  });

  if (!user) return null;

  const displayName = (user.name || user.email.split("@")[0].replace(/[._]/g, " ")).trim();

  const onSubmit = async (values: ProfileValues) => {
    const payload = {
      name: values.name || null,
      phone: values.phone || null,
    };
    try {
      const updated = await updateProfile(payload);
      onUserUpdate(updated);
      reset({ name: updated.name ?? "", phone: updated.phone ?? "" });
      toast.success("Đã cập nhật thông tin cá nhân");
    } catch (err) {
      if (err instanceof ApiError) toast.error(err.message);
      else toast.error("Đã xảy ra lỗi. Vui lòng thử lại");
    }
  };

  return (
    <div className="flex-1 overflow-y-auto px-8 py-8">
      <div className="max-w-2xl flex flex-col gap-8">
        {/* Header */}
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

        {/* Profile form */}
        <form onSubmit={handleSubmit(onSubmit)} noValidate className="flex flex-col gap-4">
          <div className="flex flex-col gap-1.5">
            <label className={`text-[13px] font-medium ${colors.neutral.text700} flex items-center gap-2`}>
              <Mail className={`w-4 h-4 ${colors.neutral.text400}`} /> Email
            </label>
            <Input value={user.email} disabled className="opacity-60 cursor-not-allowed" />
          </div>

          <div className="flex flex-col gap-1.5">
            <label className={`text-[13px] font-medium ${colors.neutral.text700} flex items-center gap-2`}>
              <UserIcon className={`w-4 h-4 ${colors.neutral.text400}`} /> Họ và tên
            </label>
            <Input
              placeholder="Nhập họ và tên"
              error={errors.name?.message}
              {...register("name")}
            />
          </div>

          <div className="flex flex-col gap-1.5">
            <label className={`text-[13px] font-medium ${colors.neutral.text700} flex items-center gap-2`}>
              <Phone className={`w-4 h-4 ${colors.neutral.text400}`} /> Số điện thoại
            </label>
            <Input
              placeholder="Nhập số điện thoại"
              error={errors.phone?.message}
              {...register("phone")}
            />
          </div>

          <div className="flex justify-end">
            <Button
              type="submit"
              isLoading={isSubmitting}
              loadingLabel="Đang lưu"
              disabled={!isDirty}
              className="px-6 py-[10px] text-[14px]"
            >
              Lưu thay đổi
            </Button>
          </div>
        </form>
      </div>
    </div>
  );
}
