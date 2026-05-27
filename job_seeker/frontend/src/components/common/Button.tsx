import React from 'react';
import { colors } from '@/theme/colors';
import { Spinner } from './Spinner';

interface ButtonProps extends React.ButtonHTMLAttributes<HTMLButtonElement> {
  children: React.ReactNode;
  variant?: 'primary' | 'secondary' | 'destructive' | 'transparent';
  isLoading?: boolean;
  loadingLabel?: string;
}

const Button: React.FC<ButtonProps> = ({
  children,
  variant = 'primary',
  isLoading = false,
  loadingLabel,
  disabled,
  className = '',
  ...props
}) => {
  const baseStyles = `rounded-full cursor-pointer border-none p-[15px] transition-colors duration-300 inline-flex items-center justify-center gap-2`;
  
  const variantStyles = {
    primary: `${colors.primary.bg} ${colors.primary.hover} ${colors.basic.textWhite} disabled:opacity-60 disabled:cursor-not-allowed`,
    secondary: `${colors.secondary.bg} ${colors.secondary.hover} ${colors.basic.textWhite} disabled:opacity-60 disabled:cursor-not-allowed`,
    destructive: `${colors.status.bgError} ${colors.status.bgErrorHover} ${colors.basic.textWhite} disabled:opacity-60 disabled:cursor-not-allowed`,
    transparent: `disabled:opacity-60 disabled:cursor-not-allowed`,
  };

  const spinnerVariant = variant === 'transparent' ? 'muted' : 'light';

  return (
    <button
      {...props}
      disabled={disabled || isLoading}
      className={`${baseStyles} ${variantStyles[variant]} ${className}`}
      aria-busy={isLoading}
    >
      {isLoading ? (
        <>
          <Spinner size="sm" colorVariant={spinnerVariant} />
          {loadingLabel ? <span>{loadingLabel}</span> : null}
        </>
      ) : (
        children
      )}
    </button>
  );
};

export default Button;
