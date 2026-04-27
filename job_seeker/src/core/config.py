from pydantic_settings import BaseSettings, SettingsConfigDict

class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    # Database
    database_url: str

    # Mistral LLM
    mistral_api_key: str

    # Embedding (Ollama)
    ollama_base_url: str

    # BGE Reranker (Docker service at port 8000)
    reranker_url: str = "http://localhost:8000"

    # LangSmith tracing
    langsmith_tracing: bool = False
    langsmith_api_key: str = ""
    langsmith_project: str = "job-seeker"


settings = Settings()

