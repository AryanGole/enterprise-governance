"""
Core Configuration — Environment-driven settings via Pydantic BaseSettings.
All secrets injected via environment variables or .env file.
"""

from pydantic_settings import BaseSettings
from typing import List
from functools import lru_cache


class Settings(BaseSettings):
    # ── Application ───────────────────────────────────────────────────────────
    APP_NAME: str = "Enterprise Data Governance Platform"
    APP_ENV: str = "production"
    DEBUG: bool = False
    SECRET_KEY: str = "change-me-in-production-use-256-bit-key"

    # ── PostgreSQL ─────────────────────────────────────────────────────────────
    POSTGRES_HOST: str = "localhost"
    POSTGRES_PORT: int = 5432
    POSTGRES_DB: str = "governance_db"
    POSTGRES_USER: str = "governance_user"
    POSTGRES_PASSWORD: str = "governance_pass"

    @property
    def DATABASE_URL(self) -> str:
        return (
            f"postgresql+asyncpg://{self.POSTGRES_USER}:{self.POSTGRES_PASSWORD}"
            f"@{self.POSTGRES_HOST}:{self.POSTGRES_PORT}/{self.POSTGRES_DB}"
        )

    @property
    def DATABASE_URL_SYNC(self) -> str:
        return (
            f"postgresql://{self.POSTGRES_USER}:{self.POSTGRES_PASSWORD}"
            f"@{self.POSTGRES_HOST}:{self.POSTGRES_PORT}/{self.POSTGRES_DB}"
        )

    # ── Neo4j ─────────────────────────────────────────────────────────────────
    NEO4J_URI: str = "bolt://localhost:7687"
    NEO4J_USER: str = "neo4j"
    NEO4J_PASSWORD: str = "neo4j_pass"

    # ── OpenLineage / Marquez ─────────────────────────────────────────────────
    MARQUEZ_URL: str = "http://localhost:5000"
    OPENLINEAGE_NAMESPACE: str = "enterprise-governance"

    # ── Redis (caching) ───────────────────────────────────────────────────────
    REDIS_URL: str = "redis://localhost:6379/0"
    CACHE_TTL_SECONDS: int = 300

    # ── CORS ──────────────────────────────────────────────────────────────────
    CORS_ORIGINS: List[str] = [
        "http://localhost:3000",
        "http://localhost:5173",
        "https://governance.enterprise.internal"
    ]

    # ── Governance Rules ──────────────────────────────────────────────────────
    SCHEMA_BREAKING_CHANGE_ALERT: bool = True
    REQUIRE_OWNER_FOR_PUBLISH: bool = True
    AUTO_GENERATE_DICTIONARY: bool = True
    MAX_LINEAGE_DEPTH: int = 10

    model_config = {"env_file": ".env", "case_sensitive": True}


@lru_cache()
def get_settings() -> Settings:
    return Settings()


settings = get_settings()
