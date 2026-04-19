from dataclasses import dataclass
from os import getenv


@dataclass(frozen=True)
class DatabaseConfig:
    host: str
    port: int
    user: str
    password: str
    database: str
    min_size: int = 5
    max_size: int = 20

    @classmethod
    def from_url(cls, url: str) -> "DatabaseConfig":
        import re

        url = url.replace("postgresql+asyncpg://", "postgresql://")
        match = re.match(r"postgresql://(.+?):(.+?)@(.+?):(\d+)/(.+)", url)
        if not match:
            raise ValueError(f"Invalid DATABASE_URL: {url}")
        user, password, host, port, database = match.groups()
        return cls(
            host=host,
            port=int(port),
            user=user,
            password=password,
            database=database,
        )


def get_db_config() -> DatabaseConfig:
    url = getenv(
        "DATABASE_URL", "postgresql+asyncpg://postgres:postgres@localhost:5433/postgres"
    )
    return DatabaseConfig.from_url(url)
