import os
from logging.config import fileConfig

from sqlalchemy import engine_from_config, pool

from alembic import context

# Import our models for autogenerate support
from libs.analytics_core.database import Base
from libs.analytics_core.models import (
    AuditLog,  # noqa: F401
    Experiment,  # noqa: F401
    ExperimentRun,  # noqa: F401
    Metric,  # noqa: F401
    Param,  # noqa: F401
    Role,  # noqa: F401
    RunTag,  # noqa: F401
    User,  # noqa: F401
    UserRole,  # noqa: F401
)

# this is the Alembic Config object, which provides
# access to the values within the .ini file in use.
config = context.config

# Interpret the config file for Python logging.
# This line sets up loggers basically.
if config.config_file_name is not None:
    fileConfig(config.config_file_name)

# Set database URL from environment variable
database_url = os.getenv("DATABASE_URL")
if database_url:
    # Convert async URL to sync for Alembic
    if database_url.startswith("postgresql+asyncpg://"):
        database_url = database_url.replace("postgresql+asyncpg://", "postgresql://")
    elif database_url.startswith("sqlite+aiosqlite://"):
        database_url = database_url.replace("sqlite+aiosqlite://", "sqlite://")
    config.set_main_option("sqlalchemy.url", database_url)

# add your model's MetaData object here
# for 'autogenerate' support
target_metadata = Base.metadata

# other values from the config, defined by the needs of env.py,
# can be acquired:
# my_important_option = config.get_main_option("my_important_option")
# ... etc.


def run_migrations_offline() -> None:
    """Run migrations in 'offline' mode.

    This configures the context with just a URL
    and not an Engine, though an Engine is acceptable
    here as well.  By skipping the Engine creation
    we don't even need a DBAPI to be available.

    Calls to context.execute() here emit the given string to the
    script output.

    """
    url = config.get_main_option("sqlalchemy.url")
    context.configure(
        url=url,
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
    )

    with context.begin_transaction():
        context.run_migrations()


def run_migrations_online() -> None:
    """Run migrations in 'online' mode.

    In this scenario we need to create an Engine
    and associate a connection with the context.

    """
    connectable = engine_from_config(
        config.get_section(config.config_ini_section, {}),
        prefix="sqlalchemy.",
        poolclass=pool.NullPool,
    )

    with connectable.connect() as connection:
        context.configure(connection=connection, target_metadata=target_metadata)

        with context.begin_transaction():
            context.run_migrations()


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
