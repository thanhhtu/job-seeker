import React from 'react';

interface ButtonProps extends React.ButtonHTMLAttributes<HTMLButtonElement> {
  children: React.ReactNode;
  variant?: 'primary' | 'secondary';
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
  const baseStyles = 'rounded-full cursor-pointer text-white border-none p-[15px] transition-colors duration-300';
  
  const variantStyles = {
    primary: 'bg-blue-600 hover:bg-indigo-600 disabled:opacity-60 disabled:cursor-not-allowed',
    secondary: 'bg-gray-500 hover:bg-gray-600 disabled:opacity-60 disabled:cursor-not-allowed',
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
