import { z } from "zod";

export const emailSchema = z
  .string()
  .trim()
  .min(1, "Vui lòng điền đầy đủ thông tin")
  .email("Email không hợp lệ");

export const passwordSchema = z.string().min(8, "Mật khẩu phải có ít nhất 8 ký tự");

export const loginSchema = z.object({
  email: emailSchema,
  password: z.string().min(1, "Vui lòng điền đầy đủ thông tin"),
});

export const registerSchema = z.object({
  email: emailSchema,
  password: passwordSchema,
});

export type LoginValues = z.infer<typeof loginSchema>;
export type RegisterValues = z.infer<typeof registerSchema>;
