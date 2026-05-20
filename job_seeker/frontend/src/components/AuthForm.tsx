import { FormEvent, useState } from "react";
import { toast } from "react-hot-toast";
import { ApiError, AuthResponse, login, register } from "@/api";
import { Button, Input } from "./common";
import { colors } from "@/theme/colors";
import { AuthMode } from "@/types/user";

type Props = {
  onLogin: (data: AuthResponse) => void;
};

export function AuthForm({ onLogin }: Props) {
  const [mode, setMode] = useState<AuthMode>(AuthMode.LOGIN);
  const [email, setEmail] = useState("");
  const [password, setPassword] = useState("");
  const [loading, setLoading] = useState(false);

  async function handleSubmit(e: FormEvent) {
    e.preventDefault();
    setLoading(true);
    try {
      const data =
        mode === "login"
          ? await login(email, password)
          : await register(email, password);
      onLogin(data);
    } catch (err) {
      if (err instanceof ApiError) {
        toast.error(err.message);
      }
    } finally {
      setLoading(false);
    }
  }

  return (
    <div className={`${colors.neutral.bg50}/80 rounded-[28px] p-5 border ${colors.neutral.border100} flex flex-col gap-4`}>
      <div>
        <h2 className={`font-semibold ${colors.neutral.text900} text-[13px] leading-tight pb-1 uppercase`}>
          {mode === AuthMode.LOGIN ? "Đăng nhập" : "Đăng ký"}
        </h2>
      </div>

      <form onSubmit={handleSubmit} className="flex flex-col gap-3">
        <Input
          type="email"
          placeholder="Email"
          value={email}
          onChange={(e) => setEmail(e.target.value)}
          required
          className="!rounded-xl"
        />
        <Input
          type="password"
          placeholder="Mật khẩu"
          value={password}
          onChange={(e) => setPassword(e.target.value)}
          required
          minLength={mode === "register" ? 8 : 1}
          className="!rounded-xl"
        />
        <Button
          type="submit"
          disabled={loading}
          className={`w-full font-semibold py-3.5 rounded-xl shadow-lg shadow-primary-light active:scale-95 mt-1`}
        >
          {loading ? "…" : mode === AuthMode.LOGIN ? "Đăng nhập" : "Đăng ký"}
        </Button>
      </form>

      <div className="text-center pt-2">
        <button
          type="button"
          onClick={() => setMode((m) => (m === AuthMode.LOGIN ? AuthMode.REGISTER : AuthMode.LOGIN))}
          className={`font-semibold ${colors.neutral.text900} hover:${colors.primary.text} p-0 cursor-pointer`}
        >
          {mode === AuthMode.LOGIN ? "Bạn mới ở đây? Tạo tài khoản" : "Bạn đã có tài khoản? Đăng nhập"}
        </button>
      </div>
      
    </div>
  );
}
