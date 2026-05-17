export const colors = {
  basic: {
    textWhite: 'text-white',            // White
    bgWhite: 'bg-white',                // White
    borderWhite: 'border-white',        // White
    borderWhiteSoft: 'border-white/30', // White with 30% opacity
    borderTopWhite: 'border-t-white',   // White (Top border)
    bgTransparent: 'bg-transparent',    // Transparent
  },

  primary: { 
    bg: 'bg-[#4f46e5]/80',              // #4f46e5 (80% opacity)
    bg50: 'bg-[#4f46e5]/50',            // #4f46e5 (50% opacity)
    hover: 'hover:bg-[#4f46e5]',        // #4f46e5
    text: 'text-[#4f46e5]',             // #4f46e5
    textMuted: 'text-[#6366f1]',        // #6366f1
    textSoft: 'text-[#4f46e5]/80',      // #4f46e5 (80% opacity)
    lightBg: 'bg-[#e0e7ff]',            // #e0e7ff
    xLightBg: 'bg-[#eef2ff]',           // #eef2ff
    border: 'border-[#4f46e5]',         // #4f46e5
  },

  secondary: {
    bg: 'bg-[#1f2937]',                 // #1f2937
    hover: 'hover:bg-[#1f2937]',        // #1f2937 
    text: 'text-[#1f2937]/80',          // #1f2937
  },

  neutral: {
    text900: 'text-[#0f172a]',          // #0f172a
    text800: 'text-[#1e293b]',          // #1e293b
    text700: 'text-[#334155]',          // #334155
    text600: 'text-[#475569]',          // #475569
    text500: 'text-[#64748b]',          // #64748b
    text400: 'text-[#94a3b8]',          // #94a3b8
    text300: 'text-[#cbd5e1]',          // #cbd5e1
    
    bg50: 'bg-[#f8fafc]',               // #f8fafc
    bg100: 'bg-[#f1f5f9]',              // #f1f5f9
    bg200: 'bg-[#e2e8f0]',              // #e2e8f0
    
    hoverBg50: 'hover:bg-[#f8fafc]',    // #f8fafc (Hover)
    border50: 'border-[#f8fafc]',       // #f8fafc (Border)
    border100: 'border-[#f1f5f9]',      // #f1f5f9 (Border)
    border200: 'border-[#e2e8f0]',      // #e2e8f0 (Border)
  },

  page: {
    bg: 'bg-[#f3f6fb]',                 // #f3f6fb
    shellBg: 'bg-[#f8f9fd]',            // #f8f9fd
    chatBg: 'bg-[#f8faff]',             // #f8faff  
    text: 'text-[#0f172a]',             // #0f172a  
  },

  status: {
    success: 'text-[#22c55e]',          // #22c55e  
    bgSuccess: 'bg-[#22c55e]',          // #22c55e  
    error: 'text-[#ef4444]',            // #ef4444  
    bgError: 'bg-[#ef4444]',            // #ef4444  
    errorBorder: 'border-[#ef4444]',    // #ef4444  
  },

  action: {
    icon: 'text-[#64748b]',
    hoverEdit: 'hover:text-[#4f46e5]',
    hoverDelete: 'hover:text-[#ef4444]',
  },
} as const;
