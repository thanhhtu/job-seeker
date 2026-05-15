import React from 'react';

interface InputProps extends React.InputHTMLAttributes<HTMLInputElement> {
  label?: string;
  error?: string;
  variant?: 'primary' | 'secondary' | 'error';
}

const Input: React.FC<InputProps> = ({
  label,
  error,
  variant = 'primary',
  className = '',
  ...props
}) => {
  const baseStyles = 'w-full p-[10px]';
  
  const variantStyles = {
    primary: 'border-gray-300 focus:ring-blue-500 border border-gray-300 rounded-md focus:outline-none focus:ring-2 focus:ring-blue-500 transition-colors',
    secondary: 'bg-transparent text-slate-600 placeholder:text-slate-400 border-0 border-none focus:ring-0 focus:outline-none',
    error: 'border-red-500 focus:ring-red-500 border border-gray-300 rounded-md focus:outline-none focus:ring-2 focus:ring-red-500 transition-colors',
  };
  
  if (!label && !error) {
    return (
      <input
        {...props}
        className={`${baseStyles} ${variantStyles[variant]} ${className}`}
      />
    );
  }
  
  return (
    <div className="flex flex-col gap-1.25">
      {label && <label className="text-sm font-medium text-gray-700">{label}</label>}
      
      <input
        {...props}
        className={`
          ${baseStyles} 
          ${error ? variantStyles.error : variantStyles[variant]} 
          ${className}`
        }
      />

      {error && <span className="text-xs text-red-500">{error}</span>}
    </div>
  );
};

export default Input;
