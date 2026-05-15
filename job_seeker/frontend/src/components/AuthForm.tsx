import { FormEvent, useState } from "react";
import { toast } from "react-hot-toast";
import { ApiError, login, register } from "@/api";
import { UserInfo } from "@/types/user";
import { STORAGE_KEYS } from "@/constant/storage";
import { Button, Input } from "./common";

type Props = {
  onLogin: (token: string, user: UserInfo) => void;
};

export function AuthForm({ onLogin }: Props) {
  const [mode, setMode] = useState<"login" | "register">("login");
  const [email, setEmail] = useState("");
  const [password, setPassword] = useState("");
  const [loading, setLoading] = useState(false);

  async function handleSubmit(e: FormEvent) {
    e.preventDefault();
    setLoading(true);
    try {
      const data = mode === "login"
        ? await login(email, password)
        : await register(email, password);
      localStorage.setItem(STORAGE_KEYS.token, data.access_token);
      localStorage.setItem(STORAGE_KEYS.user, JSON.stringify(data.user));
      onLogin(data.access_token, data.user);
    } catch (err) {
      if (err instanceof ApiError) toast.error(err.message);
    } finally {
      setLoading(false);
    }
  }

  return (
    <div className="bg-slate-50/80 rounded-[28px] p-5 border border-slate-100 flex flex-col gap-4">
      <div>
        <h2 className="font-black text-slate-900 leading-tight">Welcome Back</h2>
        <p className="font-bold text-slate-400 uppercase tracking-widest mt-1">
          Sign in to your AI workspace
        </p>
      </div>

      <form onSubmit={handleSubmit} className="flex flex-col gap-3">
        <Input
          type="email"
          placeholder="Email address"
          value={email}
          onChange={(e) => setEmail(e.target.value)}
          required
          className="!rounded-xl"
        />
        <Input
          type="password"
          placeholder="Password"
          value={password}
          onChange={(e) => setPassword(e.target.value)}
          required
          minLength={mode === "register" ? 8 : 1}
          className="!rounded-xl"
        />
        <Button
          type="submit"
          disabled={loading}
          className="w-full font-bold py-3.5 rounded-xl shadow-lg shadow-indigo-100 active:scale-95 mt-1"
        >
          {loading ? "…" : mode === "login" ? "Sign In" : "Register"}
        </Button>
      </form>

      <Button
        type="button"
        onClick={() => setMode((m) => (m === "login" ? "register" : "login"))}
        variant="secondary"
        className="font-bold text-indigo-600 bg-indigo-50 hover:bg-indigo-100 rounded-xl"
      >
        {mode === "login" ? "New here? Create an account" : "Already have an account? Login"}
      </Button>
    </div>
  );
}
