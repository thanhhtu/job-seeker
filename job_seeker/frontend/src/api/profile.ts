import { UserInfo } from "@/types/user";
import { apiFetch } from "./client";

export type UpdateProfilePayload = {
  name: string | null;
  phone: string | null;
};

export async function updateProfile(payload: UpdateProfilePayload): Promise<UserInfo> {
  const res = await apiFetch("/api/auth/me", {
    method: "PATCH",
    body: JSON.stringify(payload),
  });
  return res.json() as Promise<UserInfo>;
}

export async function changePassword(
  currentPassword: string,
  newPassword: string
): Promise<void> {
  await apiFetch("/api/auth/me/password", {
    method: "PATCH",
    body: JSON.stringify({
      current_password: currentPassword,
      new_password: newPassword,
    }),
  });
}
