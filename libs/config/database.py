"""Database configuration settings."""

import os

from pydantic import Field
from pydantic_settings import BaseSettings


class DatabaseSettings(BaseSettings):
    """Database configuration settings."""

    # Database connection
    database_url: str = Field(
        default="postgresql+asyncpg://postgres:password@localhost:5432/analytics",
        description="Database connection URL",
    )

    # Connection pool settings
    pool_size: int = Field(default=5, description="Database connection pool size")
    max_overflow: int = Field(default=10, description="Maximum connection overflow")
    pool_timeout: int = Field(
        default=30, description="Connection pool timeout in seconds"
    )
    pool_recycle: int = Field(
        default=3600, description="Connection recycle time in seconds"
    )

    # Migration settings
    migration_timeout: int = Field(
        default=300, description="Migration timeout in seconds"
    )
    migration_lock_timeout: int = Field(
        default=60, description="Migration lock timeout"
    )

    # Performance settings
    echo_sql: bool = Field(
        default=False, description="Echo SQL statements for debugging"
    )
    query_timeout: int = Field(default=30, description="Query timeout in seconds")

    # Health check settings
    health_check_interval: int = Field(
        default=30, description="Health check interval in seconds"
    )
    health_check_timeout: int = Field(
        default=5, description="Health check timeout in seconds"
    )

    class Config:
        env_prefix = "DB_"
        case_sensitive = False

    @property
    def sync_database_url(self) -> str:
        """Get synchronous database URL for migrations."""
        url = self.database_url
        if url.startswith("postgresql+asyncpg://"):
            return url.replace("postgresql+asyncpg://", "postgresql://")
        elif url.startswith("sqlite+aiosqlite://"):
            return url.replace("sqlite+aiosqlite://", "sqlite://")
        return url

    @property
    def is_sqlite(self) -> bool:
        """Check if using SQLite database."""
        return "sqlite" in self.database_url.lower()

    @property
    def is_postgresql(self) -> bool:
        """Check if using PostgreSQL database."""
        return "postgresql" in self.database_url.lower()

    def get_alembic_url(self) -> str:
        """Get database URL for Alembic migrations."""
        return self.sync_database_url


def get_database_settings() -> DatabaseSettings:
    """Get database settings instance."""
    return DatabaseSettings()


def create_database_url(
    driver: str = "postgresql+asyncpg",
    username: str = "postgres",
    password: str = "password",
    host: str = "localhost",
    port: int = 5432,
    database: str = "analytics",
) -> str:
    """Create database URL from components."""
    return f"{driver}://{username}:{password}@{host}:{port}/{database}"


def create_test_database_url() -> str:
    """Create test database URL."""
    test_db = os.getenv("TEST_DATABASE_URL")
    if test_db:
        return test_db

    # Default to in-memory SQLite for tests
    return "sqlite+aiosqlite:///:memory:"
