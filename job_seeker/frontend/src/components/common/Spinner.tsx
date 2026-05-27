import { colors } from "@/theme/colors";

type SpinnerProps = {
  variant?: "ring" | "dots";
  size?: "sm" | "md";
  
  colorVariant?: "light" | "muted";
  className?: string;
  "aria-label"?: string;
};

const ringSizeClass = {
  sm: "h-4 w-4 border-2",
  md: "h-6 w-6 border-2",
};

const ringColorClass = {
  light: `${colors.basic.borderWhiteSoft} ${colors.basic.borderTopWhite}`,
  muted: `${colors.neutral.border200} border-t-[#4f46e5]`,
};

const dotSizeClass = {
  sm: "h-1 w-1",
  md: "h-1.5 w-1.5",
};

const dotDelaysMs = [0, 150, 300] as const;

function RingSpinner({
  size,
  colorVariant,
  className,
  ariaLabel,
}: {
  size: "sm" | "md";
  colorVariant: "light" | "muted";
  className: string;
  ariaLabel?: string;
}) {
  return (
    <span
      role="status"
      aria-label={ariaLabel}
      aria-hidden={ariaLabel ? undefined : true}
      className={`inline-block shrink-0 animate-spin rounded-full ${ringSizeClass[size]} ${ringColorClass[colorVariant]} ${className}`}
    />
  );
}

function DotsSpinner({ size, className }: { size: "sm" | "md"; className: string }) {
  return (
    <span className={`inline-flex items-center gap-1 ${className}`} role="status" aria-hidden>
      {dotDelaysMs.map((delay) => (
        <span
          key={delay}
          className={`rounded-full bg-current ${dotSizeClass[size]}`}
          style={{ animation: `dot-bounce 1.5s ${delay}ms infinite ease-in-out` }}
        />
      ))}
    </span>
  );
}

export function Spinner({
  variant = "ring",
  size = "sm",
  colorVariant = "light",
  className = "",
  "aria-label": ariaLabel,
}: SpinnerProps) {
  if (variant === "dots") {
    return <DotsSpinner size={size} className={className} />;
  }
  return (
    <RingSpinner
      size={size}
      colorVariant={colorVariant}
      className={className}
      ariaLabel={ariaLabel}
    />
  );
}
