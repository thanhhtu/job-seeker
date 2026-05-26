export function renderInlineBold(text: string): React.ReactNode[] {
  const parts = text.split(/\*\*(.+?)\*\*/g);
  return parts.map((part, idx) =>
    idx % 2 === 1 ? (
      <span key={idx} className="font-semibold">{part}</span>
    ) : (
      part
    )
  );
}
