import {
  createContext,
  useCallback,
  useContext,
  useEffect,
  useMemo,
  useState,
  type ReactNode,
} from "react";
import { toast } from "react-hot-toast";
import { ApiError, listSavedJobs, removeSavedJob, saveJob } from "@/api";

type SavedJobsContextValue = {
  enabled: boolean;
  isSaved: (jobId: string) => boolean;
  isBusy: (jobId: string) => boolean;
  toggleSave: (jobId: string) => Promise<void>;
};

const SavedJobsContext = createContext<SavedJobsContextValue | null>(null);

export function SavedJobsProvider({
  enabled,
  getToken,
  onUnauthorized,
  children,
}: {
  enabled: boolean;
  getToken: () => Promise<string | null>;
  onUnauthorized: () => void;
  children: ReactNode;
}) {
  const [savedIds, setSavedIds] = useState<Set<string>>(new Set());
  const [busyIds, setBusyIds] = useState<Set<string>>(new Set());

  useEffect(() => {
    if (!enabled) {
      setSavedIds(new Set());
      return;
    }
    let cancelled = false;
    void (async () => {
      const token = await getToken();
      if (cancelled || !token) return;
      try {
        const jobs = await listSavedJobs(undefined, token);
        if (!cancelled) setSavedIds(new Set(jobs.map((j) => j.job_id)));
      } catch (err) {
        if (err instanceof ApiError && err.status === 401) onUnauthorized();
        // Non-fatal: bookmarks just won't be pre-marked.
      }
    })();
    return () => {
      cancelled = true;
    };
  }, [enabled, getToken, onUnauthorized]);

  const setBusy = useCallback((jobId: string, busy: boolean) => {
    setBusyIds((prev) => {
      const next = new Set(prev);
      if (busy) next.add(jobId);
      else next.delete(jobId);
      return next;
    });
  }, []);

  const toggleSave = useCallback(
    async (jobId: string) => {
      if (!enabled) {
        toast.error("Đăng nhập để lưu công việc.");
        return;
      }
      const token = await getToken();
      if (!token) {
        onUnauthorized();
        return;
      }
      const currentlySaved = savedIds.has(jobId);
      setBusy(jobId, true);
      try {
        if (currentlySaved) {
          await removeSavedJob(jobId, token);
          setSavedIds((prev) => {
            const next = new Set(prev);
            next.delete(jobId);
            return next;
          });
        } else {
          await saveJob({ jobId, token });
          setSavedIds((prev) => new Set(prev).add(jobId));
          toast.success("Đã lưu công việc.");
        }
      } catch (err) {
        if (err instanceof ApiError && err.status === 401) {
          onUnauthorized();
          return;
        }
        if (err instanceof ApiError) toast.error(err.message);
      } finally {
        setBusy(jobId, false);
      }
    },
    [enabled, getToken, savedIds, setBusy, onUnauthorized]
  );

  const value = useMemo<SavedJobsContextValue>(
    () => ({
      enabled,
      isSaved: (jobId: string) => savedIds.has(jobId),
      isBusy: (jobId: string) => busyIds.has(jobId),
      toggleSave,
    }),
    [enabled, savedIds, busyIds, toggleSave]
  );

  return <SavedJobsContext.Provider value={value}>{children}</SavedJobsContext.Provider>;
}

export function useSavedJobs(): SavedJobsContextValue | null {
  return useContext(SavedJobsContext);
}
