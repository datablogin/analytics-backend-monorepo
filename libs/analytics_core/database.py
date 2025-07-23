"""Database configuration and connection management."""

from collections.abc import AsyncGenerator
from typing import Any

from sqlalchemy import MetaData, create_engine, event, text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.orm import declarative_base
from sqlalchemy.pool import StaticPool

# SQLAlchemy 2.0 style declarative base
Base: Any = declarative_base()

# Naming convention for constraints (required for Alembic)
NAMING_CONVENTION = {
    "ix": "ix_%(column_0_label)s",
    "uq": "uq_%(table_name)s_%(column_0_name)s",
    "ck": "ck_%(table_name)s_%(constraint_name)s",
    "fk": "fk_%(table_name)s_%(column_0_name)s_%(referred_table_name)s",
    "pk": "pk_%(table_name)s",
}

Base.metadata = MetaData(naming_convention=NAMING_CONVENTION)


def _enable_sqlite_foreign_keys(dbapi_connection, connection_record):
    """Enable foreign key enforcement for SQLite connections."""
    if "sqlite" in str(dbapi_connection):
        cursor = dbapi_connection.cursor()
        cursor.execute("PRAGMA foreign_keys=ON")
        cursor.close()


class DatabaseManager:
    """Database connection and session management."""

    def __init__(self, database_url: str, echo: bool = False):
        """Initialize database manager.

        Args:
            database_url: Database connection URL
            echo: Whether to echo SQL statements (for debugging)
        """
        self.database_url = database_url
        self.echo = echo

        # Create async engine
        connect_args = {}
        if "sqlite" in database_url:
            connect_args = {
                "check_same_thread": False,
                "isolation_level": None,  # Enable autocommit mode for SQLite
            }

        self.async_engine = create_async_engine(
            database_url,
            echo=echo,
            poolclass=StaticPool if "sqlite" in database_url else None,
            connect_args=connect_args,
        )

        # Enable foreign key enforcement for SQLite
        if "sqlite" in database_url:
            event.listen(self.async_engine.sync_engine, "connect", _enable_sqlite_foreign_keys)

        # Create async session factory
        self.async_session_factory = async_sessionmaker(
            self.async_engine,
            class_=AsyncSession,
            expire_on_commit=False,
        )

        # Create sync engine for migrations
        sync_url = database_url.replace("postgresql+asyncpg://", "postgresql://")
        sync_url = sync_url.replace("sqlite+aiosqlite://", "sqlite://")

        self.sync_engine = create_engine(
            sync_url,
            echo=echo,
            poolclass=StaticPool if "sqlite" in sync_url else None,
            connect_args={"check_same_thread": False} if "sqlite" in sync_url else {},
        )

        # Enable foreign key enforcement for SQLite
        if "sqlite" in sync_url:
            event.listen(self.sync_engine, "connect", _enable_sqlite_foreign_keys)

    async def get_session(self) -> AsyncGenerator[AsyncSession, None]:
        """Get async database session."""
        async with self.async_session_factory() as session:
            try:
                yield session
                await session.commit()
            except Exception:
                await session.rollback()
                raise
            finally:
                await session.close()

    async def create_all_tables(self) -> None:
        """Create all tables (for testing purposes)."""
        async with self.async_engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)

    async def drop_all_tables(self) -> None:
        """Drop all tables (for testing purposes)."""
        async with self.async_engine.begin() as conn:
            await conn.run_sync(Base.metadata.drop_all)

    async def close(self) -> None:
        """Close database connections."""
        await self.async_engine.dispose()
        self.sync_engine.dispose()

    async def health_check(self) -> bool:
        """Check database connectivity."""
        try:
            async with self.async_engine.begin() as conn:
                await conn.execute(text("SELECT 1"))
            return True
        except Exception:
            return False


# Global database manager instance
_db_manager: DatabaseManager | None = None


def get_database_manager() -> DatabaseManager | None:
    """Get the global database manager instance."""
    return _db_manager


def initialize_database(database_url: str, echo: bool = False) -> DatabaseManager:
    """Initialize the global database manager."""
    global _db_manager
    _db_manager = DatabaseManager(database_url, echo)
    return _db_manager


async def get_db_session() -> AsyncGenerator[AsyncSession, None]:
    """FastAPI dependency for database sessions."""
    if _db_manager is None:
        raise RuntimeError(
            "Database not initialized. Call initialize_database() first."
        )

    async for session in _db_manager.get_session():
        yield session
