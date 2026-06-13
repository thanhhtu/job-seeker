export type ChatRole = "user" | "assistant";

export type JobRecommendation = {
  rank: number;
  title: string;
  company: string;
  reason: string;
};

export type JobCard = {
  id?: string | null;
  title: string;
  company_name: string;
  locations?: string[];
  salary_min?: number | null;
  salary_max?: number | null;
  salary_currency?: string | null;
  salary_negotiable?: boolean;
  work_mode?: string | null;
  job_level?: string | null;
  skills?: string[];
  experience_years_min?: number;
  url?: string;
};

export type AssistantData = {
  type: "jobs" | "clarification" | "no_results";
  message?: string | null;
  match_summary?: string | null;
  recommendations?: JobRecommendation[] | null;
  suggested_actions?: string[] | null;
  jobs?: JobCard[] | null;
};

export type ChatMessage = {
  role: ChatRole;
  content: string;
  createdAt?: string;
  data?: AssistantData | null;
};
