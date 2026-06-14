import React from 'react';
import { colors } from '@/theme/colors';

interface InputProps extends React.InputHTMLAttributes<HTMLInputElement> {
  label?: string;
  error?: string;
  variant?: 'primary' | 'secondary' | 'error';
}

const Input = React.forwardRef<HTMLInputElement, InputProps>(
  ({ label, error, variant = 'primary', className = '', ...props }, ref) => {
    const baseStyles = 'w-full p-[10px]';

    const variantStyles = {
      primary: `border ${colors.neutral.border200} rounded-md focus:outline-none focus:ring-2 focus:ring-accent transition-colors`,
      secondary: `${colors.basic.bgTransparent} ${colors.neutral.text600} placeholder:${colors.neutral.text400} border-0 border-none focus:ring-0 focus:outline-none`,
      error: `border ${colors.status.errorBorder} rounded-md focus:outline-none focus:ring-2 focus:ring-error transition-colors`,
    };

    if (!label && !error) {
      return (
        <input
          ref={ref}
          {...props}
          className={`${baseStyles} ${variantStyles[variant]} ${className}`}
        />
      );
    }

    return (
      <div className="flex flex-col gap-1.25">
        {label && <label className={`text-[13px] font-medium ${colors.neutral.text700}`}>{label}</label>}

        <input
          ref={ref}
          {...props}
          className={`
            ${baseStyles}
            ${error ? variantStyles.error : variantStyles[variant]}
            ${className}`
          }
        />

        {error && <span className={`text-[12px] ${colors.status.error}`}>{error}</span>}
      </div>
    );
  }
);

Input.displayName = 'Input';

export default Input;
