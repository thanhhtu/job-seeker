from pydantic import field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    # Database
    database_url: str

    # Mistral LLM
    mistral_api_key: str

    # Embedding (Ollama)
    ollama_base_url: str

    # BGE Reranker (Docker service at port 8001)
    reranker_url: str = "http://localhost:8001"

    # LangSmith tracing
    langsmith_tracing: bool = False
    langsmith_api_key: str = ""
    langsmith_project: str = "job-seeker"

    # Auth (JWT)
    jwt_secret: str = "somethingsecret"
    jwt_algorithm: str = "HS256"
    jwt_expire_minutes: int = 30
    jwt_refresh_expire_days: int = 7

    @field_validator("jwt_secret", mode="after")
    @classmethod
    def jwt_secret_non_empty(cls, v: str) -> str:
        s = (v or "").strip()
        if not s:
            return "dev-only-change-me-use-a-long-random-secret"
        return s


settings = Settings()

