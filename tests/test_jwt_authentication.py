from datetime import timedelta

import jwt
import pytest
from fastapi.testclient import TestClient
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from libs.analytics_core.auth import ALGORITHM, SECRET_KEY, AuthService
from libs.analytics_core.database import Base, get_db_session
from libs.analytics_core.models import Role, User, UserRole
from services.analytics_api.main import app

# Test database setup
SQLALCHEMY_DATABASE_URL = "sqlite+aiosqlite:///./test_auth.db"
engine = create_async_engine(
    SQLALCHEMY_DATABASE_URL, connect_args={"check_same_thread": False}
)
TestingSessionLocal = async_sessionmaker(
    engine, class_=AsyncSession, expire_on_commit=False
)


async def override_get_db_session():
    async with TestingSessionLocal() as session:
        try:
            yield session
            await session.commit()
        except Exception:
            await session.rollback()
            raise
        finally:
            await session.close()


app.dependency_overrides[get_db_session] = override_get_db_session

client = TestClient(app)


@pytest.fixture(scope="function")
async def setup_test_db():
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    yield
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.drop_all)


@pytest.fixture
async def test_user(setup_test_db):
    async with TestingSessionLocal() as db:
        hashed_password = AuthService.get_password_hash("testpassword123")
        user = User(
            username="testuser",
            email="test@example.com",
            hashed_password=hashed_password,
            full_name="Test User",
            is_active=True,
        )
        db.add(user)
        await db.commit()
        await db.refresh(user)
        yield user


@pytest.fixture
async def test_role(setup_test_db):
    async with TestingSessionLocal() as db:
        role = Role(
            name="test_role",
            description="Test role for testing",
            permissions='["read:data", "write:data"]',
            is_active=True,
        )
        db.add(role)
        await db.commit()
        await db.refresh(role)
        yield role


@pytest.fixture
async def admin_user(setup_test_db):
    async with TestingSessionLocal() as db:
        # Create admin role
        admin_role = Role(
            name="admin",
            description="Administrator role",
            permissions="["
            + '"admin:roles:create",'
            + '"admin:roles:read",'
            + '"admin:roles:update",'
            + '"admin:roles:delete",'
            + '"admin:users:read",'
            + '"admin:users:assign_roles",'
            + '"admin:users:remove_roles"'
            + "]",
            is_active=True,
        )
        db.add(admin_role)
        await db.commit()

        # Create admin user
        hashed_password = AuthService.get_password_hash("adminpassword123")
        admin = User(
            username="admin",
            email="admin@example.com",
            hashed_password=hashed_password,
            full_name="Admin User",
            is_active=True,
        )
        db.add(admin)
        await db.commit()
        await db.refresh(admin)

        # Assign admin role to user
        user_role = UserRole(user_id=admin.id, role_id=admin_role.id)
        db.add(user_role)
        await db.commit()

        yield admin


class TestPasswordHashing:
    def test_password_hashing(self):
        password = "testpassword123"
        hashed = AuthService.get_password_hash(password)

        assert hashed != password
        assert AuthService.verify_password(password, hashed)
        assert not AuthService.verify_password("wrongpassword", hashed)


class TestJWTTokens:
    async def test_create_access_token(self):
        data = {"sub": "123", "permissions": ["read:data"]}
        token = await AuthService.create_access_token(data)

        # Decode token to verify contents
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        assert payload["sub"] == "123"
        assert payload["permissions"] == ["read:data"]
        assert "exp" in payload

    async def test_verify_valid_token(self):
        data = {"sub": "123", "permissions": ["read:data"]}
        token = await AuthService.create_access_token(data)

        token_data = await AuthService.verify_token(token)
        assert token_data.user_id == 123
        assert token_data.permissions == ["read:data"]

    async def test_verify_expired_token(self):
        data = {"sub": "123", "permissions": ["read:data"]}
        # Create expired token
        expired_token = await AuthService.create_access_token(
            data, expires_delta=timedelta(seconds=-1)
        )

        with pytest.raises(Exception) as exc_info:
            await AuthService.verify_token(expired_token)
        assert "Token expired" in str(exc_info.value)

    async def test_verify_invalid_token(self):
        invalid_token = "invalid.token.here"

        with pytest.raises(Exception) as exc_info:
            await AuthService.verify_token(invalid_token)
        assert "Invalid authentication credentials" in str(exc_info.value)


class TestAuthenticationRoutes:
    def test_register_user(self, setup_test_db):
        response = client.post(
            "/v1/auth/register",
            json={
                "username": "newuser",
                "email": "newuser@example.com",
                "password": "newpassword123",
                "full_name": "New User",
            },
        )

        assert response.status_code == 200
        data = response.json()
        assert data["username"] == "newuser"
        assert data["email"] == "newuser@example.com"
        assert data["full_name"] == "New User"
        assert data["is_active"] is True
        assert "id" in data

    def test_register_duplicate_user(self, test_user):
        response = client.post(
            "/v1/auth/register",
            json={
                "username": "testuser",  # Same as test_user
                "email": "different@example.com",
                "password": "password123",
                "full_name": "Different User",
            },
        )

        assert response.status_code == 400
        assert "Username or email already registered" in response.json()["detail"]

    def test_login_valid_credentials(self, test_user):
        response = client.post(
            "/v1/auth/token",
            data={"username": "testuser", "password": "testpassword123"},
        )

        assert response.status_code == 200
        data = response.json()
        assert "access_token" in data
        assert data["token_type"] == "bearer"
        assert data["expires_in"] == 1800

    def test_login_invalid_credentials(self, test_user):
        response = client.post(
            "/v1/auth/token",
            data={"username": "testuser", "password": "wrongpassword"},
        )

        assert response.status_code == 401
        assert "Incorrect username or password" in response.json()["detail"]

    def test_get_current_user_info(self, test_user):
        # First login to get token
        login_response = client.post(
            "/v1/auth/token",
            data={"username": "testuser", "password": "testpassword123"},
        )
        token = login_response.json()["access_token"]

        # Then access protected endpoint
        response = client.get(
            "/v1/auth/me",
            headers={"Authorization": f"Bearer {token}"},
        )

        assert response.status_code == 200
        data = response.json()
        assert data["username"] == "testuser"
        assert data["email"] == "test@example.com"

    def test_protected_endpoint_without_token(self):
        response = client.get("/protected")
        assert response.status_code == 403  # No token provided

    def test_protected_endpoint_with_token(self, test_user):
        # Login to get token
        login_response = client.post(
            "/v1/auth/token",
            data={"username": "testuser", "password": "testpassword123"},
        )
        token = login_response.json()["access_token"]

        # Access protected endpoint
        response = client.get(
            "/protected",
            headers={"Authorization": f"Bearer {token}"},
        )

        assert response.status_code == 200
        data = response.json()
        assert data["success"] is True
        assert "Hello testuser" in data["data"]["message"]

    def test_change_password(self, test_user):
        # Login to get token
        login_response = client.post(
            "/v1/auth/token",
            data={"username": "testuser", "password": "testpassword123"},
        )
        token = login_response.json()["access_token"]

        # Change password
        response = client.post(
            "/v1/auth/change-password",
            json={
                "current_password": "testpassword123",
                "new_password": "newpassword123",
            },
            headers={"Authorization": f"Bearer {token}"},
        )

        assert response.status_code == 200
        assert "Password updated successfully" in response.json()["message"]

        # Test old password no longer works
        old_login = client.post(
            "/v1/auth/token",
            data={"username": "testuser", "password": "testpassword123"},
        )
        assert old_login.status_code == 401

        # Test new password works
        new_login = client.post(
            "/v1/auth/token",
            data={"username": "testuser", "password": "newpassword123"},
        )
        assert new_login.status_code == 200


class TestRoleBasedAccessControl:
    def test_create_role_as_admin(self, admin_user):
        # Login as admin
        login_response = client.post(
            "/v1/auth/token",
            data={"username": "admin", "password": "adminpassword123"},
        )
        token = login_response.json()["access_token"]

        # Create role
        response = client.post(
            "/v1/admin/roles",
            json={
                "name": "data_analyst",
                "description": "Data analyst role",
                "permissions": ["read:data", "analyze:data"],
            },
            headers={"Authorization": f"Bearer {token}"},
        )

        assert response.status_code == 200
        data = response.json()
        assert data["name"] == "data_analyst"
        assert "read:data" in data["permissions"]

    def test_create_role_as_regular_user(self, test_user):
        # Login as regular user
        login_response = client.post(
            "/v1/auth/token",
            data={"username": "testuser", "password": "testpassword123"},
        )
        token = login_response.json()["access_token"]

        # Try to create role (should fail)
        response = client.post(
            "/v1/admin/roles",
            json={
                "name": "unauthorized_role",
                "description": "Should not be created",
                "permissions": ["read:data"],
            },
            headers={"Authorization": f"Bearer {token}"},
        )

        assert response.status_code == 403
        assert "Insufficient permissions" in response.json()["detail"]

    def test_assign_role_to_user(self, admin_user, test_user, test_role):
        # Login as admin
        login_response = client.post(
            "/v1/auth/token",
            data={"username": "admin", "password": "adminpassword123"},
        )
        token = login_response.json()["access_token"]

        # Assign role to user
        response = client.post(
            f"/v1/admin/users/{test_user.id}/roles",
            json={"user_id": test_user.id, "role_id": test_role.id},
            headers={"Authorization": f"Bearer {token}"},
        )

        assert response.status_code == 200
        assert "Role assigned successfully" in response.json()["message"]

    def test_get_users_with_roles(self, admin_user):
        # Login as admin
        login_response = client.post(
            "/v1/auth/token",
            data={"username": "admin", "password": "adminpassword123"},
        )
        token = login_response.json()["access_token"]

        # Get users with roles
        response = client.get(
            "/v1/admin/users",
            headers={"Authorization": f"Bearer {token}"},
        )

        assert response.status_code == 200
        users = response.json()
        assert len(users) >= 1  # At least the admin user
        assert any(user["username"] == "admin" for user in users)
