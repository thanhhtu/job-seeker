export type SavedJobStatus =
  | "saved"
  | "applied"
  | "interviewing"
  | "offer"
  | "rejected";

export type SavedJob = {
  job_id: string;
  status: SavedJobStatus;
  note: string | null;
  applied_at: string | null;
  created_at: string;
  updated_at: string;

  title: string;
  company_name: string;
  url: string;
  locations: string[];
  salary_min: number | null;
  salary_max: number | null;
  salary_currency: string | null;
  salary_negotiable: boolean;
  work_mode: string | null;
  job_level: string | null;
  skills: string[];
  experience_years_min: number;
  posted_date: string | null;
};
