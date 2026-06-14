import { z } from "zod";

export const profileSchema = z.object({
  name: z
    .string()
    .trim()
    .max(120, "Họ và tên tối đa 120 ký tự"),
  phone: z
    .string()
    .trim()
    .max(32, "Số điện thoại tối đa 32 ký tự")
    .refine(
      (v) => !v || /^[+0-9][0-9\s.-]{7,}$/.test(v),
      "Số điện thoại không hợp lệ"
    ),
});

export type ProfileValues = z.infer<typeof profileSchema>;
