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
  // 1. Lưu lại và tắt transition tạm thời để việc set height = '1px' ăn ngay lập tức
  const originalTransition = el.style.transition;
  el.style.transition = 'none';

  // 2. Thu nhỏ về 1px để tính toán lại scrollHeight từ đầu
  el.style.height = '1px';
  el.style.overflow = 'hidden';

  // 3. Lấy các thông số CSS
  const style = window.getComputedStyle(el);
  const paddingTop = parseFloat(style.paddingTop) || 0;
  const paddingBottom = parseFloat(style.paddingBottom) || 0;
  
  // Xử lý trường hợp lineHeight trả về 'normal' (có thể gây lỗi NaN)
  let lineHeight = parseFloat(style.lineHeight);
  if (isNaN(lineHeight)) {
    const fontSize = parseFloat(style.fontSize) || 16;
    lineHeight = fontSize * 1.5; // Giá trị fallback an toàn
  }

  const maxHeight = lineHeight * MAX_ROWS + paddingTop + paddingBottom;
  const needed = el.scrollHeight;

  // 4. Áp dụng chiều cao mới
  if (needed <= maxHeight) {
    el.style.height = `${needed}px`;
    el.style.overflow = 'hidden';
  } else {
    el.style.height = `${maxHeight}px`;
    el.style.overflow = 'auto';
  }

  // 5. Force reflow: Ép trình duyệt nhận diện chiều cao mới trước khi bật lại transition
  void el.offsetHeight;

  // 6. Trả lại transition ban đầu để giữ hiệu ứng animation
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
          if (textareaRef.current) adjustHeight(textareaRef.current);
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