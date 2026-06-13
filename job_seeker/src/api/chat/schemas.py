from __future__ import annotations

from pydantic import BaseModel, Field


class ChatRequest(BaseModel):
    message: str = Field(..., min_length=1)
    user_id: str = Field(default="anonymous")
    session_id: str | None = None


# Structured response models 
class JobRecommendationOut(BaseModel):
    rank: int
    title: str
    company: str
    reason: str


class JobCardOut(BaseModel):
    id: str | None = None
    title: str
    company_name: str
    locations: list[str] = []
    salary_min: float | None = None
    salary_max: float | None = None
    salary_currency: str | None = None
    salary_negotiable: bool = False
    work_mode: str | None = None
    job_level: str | None = None
    skills: list[str] = []
    experience_years_min: int = 0
    url: str = ""


class AssistantData(BaseModel):
    type: str = Field(description="'jobs' | 'clarification' | 'no_results'")
    message: str | None = None
    match_summary: str | None = None
    recommendations: list[JobRecommendationOut] | None = None
    suggested_actions: list[str] | None = None
    jobs: list[JobCardOut] | None = None


class ChatResponse(BaseModel):
    session_id: str
    user_message: str
    assistant_message: str
    data: AssistantData | None = None
