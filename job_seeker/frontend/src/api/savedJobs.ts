import { SavedJob, SavedJobStatus } from "@/types/savedJob";
import { apiFetch } from "./client";

export async function listSavedJobs(
  status?: SavedJobStatus,
  token?: string | null
): Promise<SavedJob[]> {
  const qs = status ? `?status=${encodeURIComponent(status)}` : "";
  const res = await apiFetch(`/api/me/saved-jobs${qs}`, { token });
  return res.json() as Promise<SavedJob[]>;
}

export async function saveJob(params: {
  jobId: string;
  status?: SavedJobStatus;
  note?: string | null;
  token?: string | null;
}): Promise<SavedJob> {
  const res = await apiFetch("/api/me/saved-jobs", {
    method: "POST",
    token: params.token,
    body: JSON.stringify({
      job_id: params.jobId,
      ...(params.status ? { status: params.status } : {}),
      ...(params.note != null ? { note: params.note } : {}),
    }),
  });
  return res.json() as Promise<SavedJob>;
}

export async function updateSavedJob(
  jobId: string,
  patch: { status?: SavedJobStatus; note?: string | null },
  token?: string | null
): Promise<SavedJob> {
  const res = await apiFetch(`/api/me/saved-jobs/${jobId}`, {
    method: "PATCH",
    token,
    body: JSON.stringify(patch),
  });
  return res.json() as Promise<SavedJob>;
}

export async function removeSavedJob(
  jobId: string,
  token?: string | null
): Promise<void> {
  await apiFetch(`/api/me/saved-jobs/${jobId}`, { method: "DELETE", token });
}
