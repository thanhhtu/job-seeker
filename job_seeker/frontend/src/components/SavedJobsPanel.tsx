import { useCallback, useEffect, useMemo, useState } from "react";
import { toast } from "react-hot-toast";
import { ApiError, listSavedJobs, removeSavedJob, updateSavedJob } from "@/api";
import { Spinner } from "@/components/common";
import { SavedJobsTable } from "@/components/SavedJobsTable";
import { STATUS_LABELS, STATUS_ORDER } from "@/constant/savedJob";
import { useAuth } from "@/hooks/useAuth";
import { SavedJob, SavedJobStatus } from "@/types/savedJob";
import { colors } from "@/theme/colors";

type Filter = "all" | SavedJobStatus;

export function SavedJobsPanel() {
  const { accessToken, isBootstrapping, logout, getValidAccessToken } = useAuth();

  const [savedJobs, setSavedJobs] = useState<SavedJob[]>([]);
  const [loading, setLoading] = useState(false);
  const [filter, setFilter] = useState<Filter>("all");
  const [busyId, setBusyId] = useState<string | null>(null);

  const fetchSavedJobs = useCallback(async () => {
    if (!accessToken) {
      setSavedJobs([]);
      return;
    }
    setLoading(true);
    try {
      const token = await getValidAccessToken();
      if (!token) {
        logout();
        return;
      }
      const data = await listSavedJobs(undefined, token);
      setSavedJobs(data);
    } catch (err) {
      if (err instanceof ApiError && err.status === 401) {
        logout();
        return;
      }
      if (err instanceof ApiError) {
        toast.error(err.message);
      }
    } finally {
      setLoading(false);
    }
  }, [accessToken, logout, getValidAccessToken]);

  useEffect(() => {
    if (!isBootstrapping) {
      void fetchSavedJobs();
    }
  }, [fetchSavedJobs, isBootstrapping]);

  const handleChangeStatus = async (jobId: string, status: SavedJobStatus) => {
    setBusyId(jobId);
    try {
      const token = await getValidAccessToken();
      if (!token) {
        logout();
        return;
      }
      const updated = await updateSavedJob(jobId, { status }, token);
      setSavedJobs((prev) => prev.map((j) => (j.job_id === jobId ? updated : j)));
    } catch (err) {
      if (err instanceof ApiError && err.status === 401) {
        logout();
        return;
      }
      if (err instanceof ApiError) {
        toast.error(err.message);
      }
    } finally {
      setBusyId(null);
    }
  };

  const handleSaveNote = async (jobId: string, note: string) => {
    setBusyId(jobId);
    try {
      const token = await getValidAccessToken();
      if (!token) {
        logout();
        return;
      }
      const updated = await updateSavedJob(jobId, { note: note || null }, token);
      setSavedJobs((prev) => prev.map((j) => (j.job_id === jobId ? updated : j)));
      toast.success("Đã lưu ghi chú.");
    } catch (err) {
      if (err instanceof ApiError && err.status === 401) {
        logout();
        return;
      }
      if (err instanceof ApiError) {
        toast.error(err.message);
      }
    } finally {
      setBusyId(null);
    }
  };

  const handleRemove = async (jobId: string) => {
    setBusyId(jobId);
    try {
      const token = await getValidAccessToken();
      if (!token) {
        logout();
        return;
      }
      await removeSavedJob(jobId, token);
      setSavedJobs((prev) => prev.filter((j) => j.job_id !== jobId));
      toast.success("Đã bỏ lưu công việc.");
    } catch (err) {
      if (err instanceof ApiError && err.status === 401) {
        logout();
        return;
      }
      if (err instanceof ApiError) {
        toast.error(err.message);
      }
    } finally {
      setBusyId(null);
    }
  };

  const counts = useMemo(() => {
    const c: Record<string, number> = { all: savedJobs.length };
    for (const s of STATUS_ORDER) c[s] = 0;
    for (const j of savedJobs) c[j.status] = (c[j.status] ?? 0) + 1;
    return c;
  }, [savedJobs]);

  const visibleJobs = useMemo(
    () => (filter === "all" ? savedJobs : savedJobs.filter((j) => j.status === filter)),
    [savedJobs, filter]
  );

  const filterTabs: { key: Filter; label: string }[] = [
    { key: "all", label: "Tất cả" },
    ...STATUS_ORDER.map((s) => ({ key: s as Filter, label: STATUS_LABELS[s] })),
  ];

  return (
    <div className="flex-1 flex flex-col min-h-0">
      {/* Filter tabs */}
      <div className={`shrink-0 px-8 flex flex-wrap border-b ${colors.neutral.border100}`}>
        {filterTabs.map((tab) => {
          const active = filter === tab.key;
          return (
            <button
              key={tab.key}
              type="button"
              onClick={() => setFilter(tab.key)}
              className={`-mb-px flex items-center gap-2 py-3 px-5 text-[13px] font-medium border-b-2 transition-colors cursor-pointer ${
                active
                  ? `${colors.primary.border} ${colors.primary.text}`
                  : `border-transparent ${colors.neutral.text600} ${colors.neutral.hoverText700}`
              }`}
            >
              {tab.label}
              <span
                className={`inline-flex items-center justify-center min-w-[18px] h-[18px] px-1 rounded-full text-[11px] font-semibold ${
                  active
                    ? `${colors.primary.xLightBg} ${colors.primary.text}`
                    : `${colors.neutral.bg100} ${colors.neutral.text400}`
                }`}
              >
                {counts[tab.key] ?? 0}
              </span>
            </button>
          );
        })}
      </div>

      {/* List */}
      <div className="flex-1 min-h-0 flex flex-col p-8">
        {loading ? (
          <div className="flex items-center justify-center py-16">
            <Spinner size="md" colorVariant="muted" aria-label="Đang tải" />
          </div>
        ) : visibleJobs.length === 0 ? (
          <div className="flex flex-col items-center justify-center py-16 gap-2 text-center">
            <p className={`text-[14px] font-medium ${colors.neutral.text600}`}>
              {savedJobs.length === 0
                ? "Bạn chưa lưu công việc nào."
                : "Không có công việc nào ở trạng thái này."}
            </p>
            <p className={`text-[13px] ${colors.neutral.text400}`}>
              Lưu công việc từ kết quả trò chuyện để theo dõi tại đây.
            </p>
          </div>
        ) : (
          <SavedJobsTable
            jobs={visibleJobs}
            busyId={busyId}
            onChangeStatus={(id, s) => void handleChangeStatus(id, s)}
            onSaveNote={(id, note) => void handleSaveNote(id, note)}
            onRemove={(id) => void handleRemove(id)}
          />
        )}
      </div>
    </div>
  );
}
