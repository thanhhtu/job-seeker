/** Plain text for UI that must not show raw `**bold**` markers (e.g. action chips). */
export function stripInlineBoldMarkers(text: string): string {
  return text.replace(/\*\*(.+?)\*\*/g, "$1").replace(/\*\*/g, "");
}

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
