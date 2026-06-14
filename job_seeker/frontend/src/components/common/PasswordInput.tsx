import React, { useState } from 'react';
import { Eye, EyeOff } from 'lucide-react';
import { colors } from '@/theme/colors';
import Input from './Input';

type InputProps = React.ComponentProps<typeof Input>;

/** Password field with a show/hide (eye) toggle. */
const PasswordInput = React.forwardRef<HTMLInputElement, InputProps>(
  ({ className = '', ...props }, ref) => {
    const [visible, setVisible] = useState(false);

    return (
      <div className="relative">
        <Input
          ref={ref}
          {...props}
          type={visible ? 'text' : 'password'}
          className={`pr-11 ${className}`}
        />
        <button
          type="button"
          onClick={() => setVisible((v) => !v)}
          tabIndex={-1}
          aria-label={visible ? 'Ẩn mật khẩu' : 'Hiện mật khẩu'}
          title={visible ? 'Ẩn mật khẩu' : 'Hiện mật khẩu'}
          className={`absolute right-3 top-[21px] -translate-y-1/2 ${colors.neutral.text400} ${colors.neutral.hoverText700} transition-colors cursor-pointer`}
        >
          {visible ? <EyeOff className="w-[18px] h-[18px]" /> : <Eye className="w-[18px] h-[18px]" />}
        </button>
      </div>
    );
  }
);

PasswordInput.displayName = 'PasswordInput';

export default PasswordInput;
