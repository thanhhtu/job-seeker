import { SavedJobStatus } from "@/types/savedJob";

export const STATUS_LABELS: Record<SavedJobStatus, string> = {
  saved: "Đã lưu",
  applied: "Đã nộp",
  interviewing: "Phỏng vấn",
  offer: "Có offer",
  rejected: "Từ chối",
};

export const STATUS_ORDER: SavedJobStatus[] = [
  "saved",
  "applied",
  "interviewing",
  "offer",
  "rejected",
];

export const STATUS_RANK = Object.fromEntries(
  STATUS_ORDER.map((status, index) => [status, index]),
) as Record<SavedJobStatus, number>;
