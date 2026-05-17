import React from 'react';
import { colors } from '@/theme/colors';

interface ButtonProps extends React.ButtonHTMLAttributes<HTMLButtonElement> {
  children: React.ReactNode;
  variant?: 'primary' | 'secondary' | 'transparent';
  isLoading?: boolean;
}

const Button: React.FC<ButtonProps> = ({
  children,
  variant = 'primary',
  isLoading = false,
  disabled,
  className = '',
  ...props
}) => {
  const baseStyles = `rounded-full cursor-pointer border-none p-[15px] transition-colors duration-300`;
  
  const variantStyles = {
    primary: `${colors.primary.bg} ${colors.primary.hover} ${colors.basic.textWhite} disabled:opacity-60 disabled:cursor-not-allowed disabled:hover:bg-accent`,
    secondary: `${colors.secondary.bg} ${colors.secondary.hover} ${colors.basic.textWhite} disabled:opacity-60 disabled:cursor-not-allowed disabled:hover:bg-secondary`,
    transparent: `disabled:opacity-60 disabled:cursor-not-allowed`,
  };

  return (
    <button
      {...props}
      disabled={disabled || isLoading}
      className={`${baseStyles} ${variantStyles[variant]} ${className}`}
    >
      {isLoading ? 'Loading...' : children}
    </button>
  );
};

export default Button;
