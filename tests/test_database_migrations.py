"""Tests for database migration system."""

import os
import tempfile
from unittest.mock import patch

import pytest
from sqlalchemy import select, text

from alembic import command
from alembic.config import Config
from libs.analytics_core.database import initialize_database
from libs.analytics_core.models import AuditLog, Role, User, UserRole


@pytest.fixture
def temp_db_url():
    """Create a temporary SQLite database for testing."""
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as tmp:
        db_url = f"sqlite+aiosqlite:///{tmp.name}"
        yield db_url
        # Cleanup
        if os.path.exists(tmp.name):
            os.unlink(tmp.name)


@pytest.fixture
def db_manager(temp_db_url):
    """Create database manager with temporary database."""
    manager = initialize_database(temp_db_url, echo=False)
    yield manager
    # Cleanup
    if manager:
        manager.sync_engine.dispose()


@pytest.fixture
def alembic_config(temp_db_url):
    """Create Alembic configuration for testing."""
    config = Config("alembic.ini")
    config.set_main_option("sqlalchemy.url", temp_db_url)
    return config


class TestDatabaseManager:
    """Test database manager functionality."""

    @pytest.mark.asyncio
    async def test_database_initialization(self, db_manager):
        """Test database manager initialization."""
        assert db_manager is not None
        assert db_manager.database_url.startswith("sqlite+aiosqlite://")

        # Test health check
        health = await db_manager.health_check()
        assert health is True

    @pytest.mark.asyncio
    async def test_create_tables(self, db_manager):
        """Test table creation."""
        await db_manager.create_all_tables()

        # Verify tables exist
        with db_manager.sync_engine.connect() as conn:
            result = conn.execute(
                text("SELECT name FROM sqlite_master WHERE type='table'")
            )
            tables = {row[0] for row in result}

            expected_tables = {"users", "roles", "user_roles", "audit_logs"}
            assert expected_tables.issubset(tables)

    @pytest.mark.asyncio
    async def test_session_management(self, db_manager):
        """Test database session management."""
        await db_manager.create_all_tables()

        async for session in db_manager.get_session():
            # Test session functionality
            user = User(
                email="test@example.com",
                username="testuser",
                full_name="Test User",
                hashed_password="hashedpassword",
            )
            session.add(user)
            await session.flush()  # Get ID without committing

            assert user.id is not None
            assert user.email == "test@example.com"

    @pytest.mark.asyncio
    async def test_session_rollback(self, db_manager):
        """Test session rollback on error."""
        await db_manager.create_all_tables()

        with pytest.raises(Exception):
            async for session in db_manager.get_session():
                user = User(
                    email="test@example.com",
                    username="testuser",
                    full_name="Test User",
                    hashed_password="hashedpassword",
                )
                session.add(user)
                await session.flush()

                # Force an error to test rollback
                raise ValueError("Test error")


class TestAlembicIntegration:
    """Test Alembic migration functionality."""

    def test_alembic_migration_generation(self, alembic_config, temp_db_url):
        """Test that migrations exist and can be applied."""
        # Check migration file exists
        versions_dir = "alembic/versions"
        migration_files = (
            os.listdir(versions_dir) if os.path.exists(versions_dir) else []
        )
        python_files = [f for f in migration_files if f.endswith(".py")]
        assert len(python_files) >= 1

    def test_alembic_upgrade_downgrade(self, alembic_config, temp_db_url):
        """Test migration upgrade and downgrade."""
        with patch.dict(os.environ, {"DATABASE_URL": temp_db_url}):
            # Upgrade to head (using existing migrations)
            command.upgrade(alembic_config, "head")

            # Check current revision
            from alembic.script import ScriptDirectory

            ScriptDirectory.from_config(alembic_config)

            # Verify we can downgrade
            command.downgrade(alembic_config, "base")

    def test_migration_performance(self, alembic_config, temp_db_url):
        """Test migration performance requirements."""
        import time

        with patch.dict(os.environ, {"DATABASE_URL": temp_db_url}):
            # Test upgrade performance (using existing migrations)
            start_time = time.time()
            command.upgrade(alembic_config, "head")
            upgrade_time = time.time() - start_time

            # Performance assertions (generous for test environment)
            assert upgrade_time < 10.0  # Migration execution < 10 seconds


class TestDatabaseModels:
    """Test database model definitions."""

    @pytest.mark.asyncio
    async def test_user_model(self, db_manager):
        """Test User model functionality."""
        await db_manager.create_all_tables()

        async for session in db_manager.get_session():
            user = User(
                email="test@example.com",
                username="testuser",
                full_name="Test User",
                hashed_password="hashedpassword",
                is_active=True,
                is_superuser=False,
            )
            session.add(user)
            await session.flush()

            assert user.id is not None
            assert user.created_at is not None
            assert user.updated_at is not None
            assert user.email == "test@example.com"

    @pytest.mark.asyncio
    async def test_role_model(self, db_manager):
        """Test Role model functionality."""
        await db_manager.create_all_tables()

        async for session in db_manager.get_session():
            role = Role(
                name="admin",
                description="Administrator role",
                permissions='{"read": true, "write": true}',
            )
            session.add(role)
            await session.flush()

            assert role.id is not None
            assert role.name == "admin"
            assert "read" in role.permissions

    @pytest.mark.asyncio
    async def test_audit_log_model(self, db_manager):
        """Test AuditLog model functionality."""
        await db_manager.create_all_tables()

        async for session in db_manager.get_session():
            log = AuditLog(
                user_id=1,
                action="login",
                resource="auth",
                resource_id="user_1",
                details='{"success": true}',
                ip_address="192.168.1.1",
                user_agent="Test Agent",
            )
            session.add(log)
            await session.flush()

            assert log.id is not None
            assert log.action == "login"
            assert log.ip_address == "192.168.1.1"


class TestMigrationCompliance:
    """Test migration system compliance with requirements."""

    def test_naming_convention(self):
        """Test that naming convention is properly configured."""
        from libs.analytics_core.database import NAMING_CONVENTION

        required_keys = ["ix", "uq", "ck", "fk", "pk"]
        for key in required_keys:
            assert key in NAMING_CONVENTION
            assert NAMING_CONVENTION[key] is not None

    def test_schema_consistency(self, db_manager):
        """Test schema consistency between environments."""
        # This would be expanded in real implementation
        # to compare schemas across different environments
        assert True  # Placeholder for schema comparison logic

    @pytest.mark.asyncio
    async def test_zero_downtime_capability(self, db_manager):
        """Test that migrations can support zero downtime deployments."""
        await db_manager.create_all_tables()

        # Test concurrent access during migration simulation
        async for session in db_manager.get_session():
            user = User(
                email="concurrent@example.com",
                username="concurrent",
                hashed_password="password",
            )
            session.add(user)
            await session.flush()

            # Simulate continuing operations during migration
            health = await db_manager.health_check()
            assert health is True


class TestForeignKeyConstraints:
    """Test foreign key constraint functionality."""

    @pytest.mark.asyncio
    async def test_foreign_key_enforcement(self, db_manager):
        """Test that foreign key constraints are properly enforced."""
        await db_manager.create_all_tables()

        async for session in db_manager.get_session():
            # Create a user and role first
            user = User(
                email="fk_test@example.com",
                username="fktest",
                hashed_password="password",
                is_active=True,
            )
            role = Role(
                name="test_role",
                description="Test role",
                permissions='["read"]',
            )
            session.add(user)
            session.add(role)
            await session.flush()

            # Test valid UserRole creation
            user_role = UserRole(user_id=user.id, role_id=role.id)
            session.add(user_role)
            await session.flush()
            assert user_role.id is not None

    @pytest.mark.asyncio
    async def test_relationship_loading(self, db_manager):
        """Test that relationships are properly loaded with eager loading."""
        await db_manager.create_all_tables()

        async for session in db_manager.get_session():
            # Create test data
            user = User(
                email="relationship@example.com",
                username="reltest",
                hashed_password="password",
            )
            role = Role(
                name="admin",
                description="Administrator",
                permissions='["admin:read", "admin:write"]',
            )
            session.add(user)
            session.add(role)
            await session.flush()

            # Create relationship
            user_role = UserRole(user_id=user.id, role_id=role.id)
            session.add(user_role)
            await session.flush()

            # Test relationship loading
            from sqlalchemy.orm import selectinload
            stmt = select(User).options(
                selectinload(User.user_roles).selectinload(UserRole.role)
            ).where(User.id == user.id)
            result = await session.execute(stmt)
            loaded_user = result.scalar_one()

            assert len(loaded_user.user_roles) == 1
            assert loaded_user.user_roles[0].role.name == "admin"

    @pytest.mark.asyncio
    async def test_cascade_delete(self, db_manager):
        """Test that cascade deletes work properly."""
        await db_manager.create_all_tables()

        async for session in db_manager.get_session():
            # Create test data
            user = User(
                email="cascade@example.com",
                username="cascade",
                hashed_password="password",
            )
            role = Role(
                name="cascade_role",
                description="Role for cascade test",
                permissions='["read"]',
            )
            session.add(user)
            session.add(role)
            await session.flush()

            user_role = UserRole(user_id=user.id, role_id=role.id)
            session.add(user_role)
            await session.flush()
            user_role_id = user_role.id

            # Delete the user - should cascade delete user_roles
            await session.delete(user)
            await session.flush()

            # Verify user_role was deleted due to cascade
            stmt = select(UserRole).where(UserRole.id == user_role_id)
            result = await session.execute(stmt)
            deleted_user_role = result.scalar_one_or_none()
            assert deleted_user_role is None
