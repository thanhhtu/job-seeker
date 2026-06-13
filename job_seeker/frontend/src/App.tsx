import { Toaster } from "react-hot-toast";
import { BrowserRouter, Navigate, Route, Routes } from "react-router-dom";
import { ChatPage } from "@/pages/ChatPage";
import { SettingsPage } from "@/pages/SettingsPage";
import { colors } from "@/theme/colors";

export default function App() {
  return (
    <BrowserRouter>
      <div className={`flex h-screen w-full ${colors.page.shellBg} font-sans`}>
        <Toaster
          position="top-right"
          toastOptions={{
            error: {
              duration: 5000,
              style: {
                background: colors.basic.bgWhite,
                color: colors.status.error,
                border: `1px solid ${colors.status.errorBorder}`,
              },
            },
          }}
        />
        <Routes>
          <Route path="/chat" element={<ChatPage />} />
          <Route path="/chat/:sessionId" element={<ChatPage />} />
          <Route path="/settings" element={<Navigate to="/settings/profile" replace />} />
          <Route path="/settings/:section" element={<SettingsPage />} />
          <Route path="*" element={<Navigate to="/chat" replace />} />
        </Routes>
      </div>
    </BrowserRouter>
  );
}
