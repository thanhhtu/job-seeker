import React, { useRef, useCallback, useLayoutEffect } from 'react';
import { colors } from '@/theme/colors';

interface InputProps
  extends React.TextareaHTMLAttributes<HTMLTextAreaElement> {
  label?: string;
  error?: string;
  variant?: 'primary' | 'secondary' | 'error';
}

const MAX_ROWS = 5;

function adjustHeight(el: HTMLTextAreaElement) {
  const originalTransition = el.style.transition;
  el.style.transition = 'none';

  el.style.height = '1px';
  el.style.overflow = 'hidden';

  const style = window.getComputedStyle(el);
  const paddingTop = parseFloat(style.paddingTop) || 0;
  const paddingBottom = parseFloat(style.paddingBottom) || 0;
  
  let lineHeight = parseFloat(style.lineHeight);
  if (isNaN(lineHeight)) {
    const fontSize = parseFloat(style.fontSize) || 16;
    lineHeight = fontSize * 1.5; 
  }

  const maxHeight = lineHeight * MAX_ROWS + paddingTop + paddingBottom;
  const needed = el.scrollHeight;

  if (needed <= maxHeight) {
    el.style.height = `${needed}px`;
    el.style.overflow = 'hidden';
  } else {
    el.style.height = `${maxHeight}px`;
    el.style.overflow = 'auto';
  }

  void el.offsetHeight;

  el.style.transition = originalTransition;
}

const Textarea: React.FC<InputProps> = ({
  label,
  error,
  variant = 'primary',
  className = '',
  onKeyDown,
  onChange,
  value,
  ...props
}) => {
  const textareaRef = useRef<HTMLTextAreaElement>(null);

  const baseStyles =
    'w-full p-[10px] resize-none transition-[height] duration-150';

  const variantStyles = {
    primary:
      `border ${colors.neutral.border200} rounded-md focus:outline-none focus:ring-2 focus:ring-accent transition-colors`,
    secondary:
      `${colors.basic.bgTransparent} ${colors.neutral.text600} placeholder:${colors.neutral.text400} border-0 border-none focus:ring-0 focus:outline-none`,
    error:
      `border ${colors.status.errorBorder} rounded-md focus:outline-none focus:ring-2 focus:ring-error transition-colors`,
  };

  // Re-adjust whenever value changes (handles controlled component re-renders)
  // useLayoutEffect runs before browser paint, so no flicker
  useLayoutEffect(() => {
    if (textareaRef.current) {
      adjustHeight(textareaRef.current);
    }
  }, [value]);

  const handleKeyDown = useCallback(
    (e: React.KeyboardEvent<HTMLTextAreaElement>) => {
      if (e.key === 'Enter' && e.shiftKey) {
        // Let browser insert the newline, then measure
        setTimeout(() => {
          if (textareaRef.current) {
            adjustHeight(textareaRef.current);
          }
        }, 0);
      }

      if (e.key === 'Enter' && !e.shiftKey && !e.nativeEvent.isComposing) {
        e.preventDefault();
      }

      onKeyDown?.(e);
    },
    [onKeyDown]
  );

  const handleChange = useCallback(
    (e: React.ChangeEvent<HTMLTextAreaElement>) => {
      // For uncontrolled usage: adjust immediately on change
      adjustHeight(e.target);
      onChange?.(e);
    },
    [onChange]
  );

  const sharedProps = {
    ref: textareaRef,
    rows: 1,
    value,
    onKeyDown: handleKeyDown,
    onChange: handleChange,
    ...props,
  };

  if (!label && !error) {
    return (
      <textarea
        {...sharedProps}
        className={`${baseStyles} ${variantStyles[variant]} ${className}`}
      />
    );
  }

  return (
    <div className="flex flex-col gap-[5px]">
      {label && (
        <label className={`text-[13px] font-medium ${colors.neutral.text700}`}>{label}</label>
      )}

      <textarea
        {...sharedProps}
        className={`
          ${baseStyles}
          ${error ? variantStyles.error : variantStyles[variant]}
          ${className}`}
      />

      {error && <span className={`${colors.status.error}`}>{error}</span>}
    </div>
  );
};

export default Textarea;