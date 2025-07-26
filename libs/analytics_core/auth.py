import json
import os
from datetime import datetime, timedelta, timezone
from typing import Any

import jwt
from fastapi import Depends, HTTPException, status
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from passlib.context import CryptContext
from pydantic import BaseModel, Field
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from libs.analytics_core.database import get_db_session
from libs.analytics_core.models import Role, User, UserRole

# Note: Import moved to function level to avoid circular import

security = HTTPBearer()
pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")

# Use secrets manager for sensitive configuration
SECRET_KEY = os.getenv(
    "JWT_SECRET_KEY", "your-secret-key-change-in-production"
)  # Will be replaced by secrets manager
ALGORITHM = "HS256"
ACCESS_TOKEN_EXPIRE_MINUTES = 30


async def get_jwt_secret_key() -> str:
    """Get JWT secret key from secrets manager."""
    try:
        from libs.streaming_analytics.secrets_manager import get_secret

        secret_key = await get_secret("jwt_secret_key")
        return secret_key or SECRET_KEY
    except ImportError:
        # Fallback if secrets manager is not available
        return SECRET_KEY


class TokenData(BaseModel):
    user_id: int | None = None
    permissions: list[str] = Field(default_factory=list)


class TokenResponse(BaseModel):
    access_token: str
    token_type: str = "bearer"
    expires_in: int


class AuthService:
    @staticmethod
    def verify_password(plain_password: str, hashed_password: str) -> bool:
        return pwd_context.verify(plain_password, hashed_password)

    @staticmethod
    def get_password_hash(password: str) -> str:
        return pwd_context.hash(password)

    @staticmethod
    async def create_access_token(
        data: dict[str, Any], expires_delta: timedelta | None = None
    ) -> str:
        to_encode = data.copy()
        if expires_delta:
            expire = datetime.now(timezone.utc) + expires_delta  # noqa: UP017
        else:
            expire = datetime.now(timezone.utc) + timedelta(  # noqa: UP017
                minutes=ACCESS_TOKEN_EXPIRE_MINUTES
            )
        to_encode.update({"exp": expire})
        secret_key = await get_jwt_secret_key()
        encoded_jwt = jwt.encode(to_encode, secret_key, algorithm=ALGORITHM)
        return encoded_jwt

    @staticmethod
    async def verify_token(token: str) -> TokenData:
        import time
        # Add consistent timing to prevent timing attacks
        start_time = time.time()

        try:
            secret_key = await get_jwt_secret_key()
            payload = jwt.decode(token, secret_key, algorithms=[ALGORITHM])

            # Validate user_id
            user_id_raw = payload.get("sub")
            if user_id_raw is None:
                # Ensure consistent timing for all error cases
                elapsed = time.time() - start_time
                if elapsed < 0.001:  # Minimum 1ms delay
                    time.sleep(0.001 - elapsed)
                raise HTTPException(
                    status_code=status.HTTP_401_UNAUTHORIZED,
                    detail="Invalid authentication credentials",
                    headers={"WWW-Authenticate": "Bearer"},
                )

            # Ensure user_id is an integer
            try:
                user_id = int(user_id_raw)
                if user_id <= 0:
                    raise ValueError("User ID must be positive")
            except (ValueError, TypeError):
                # Ensure consistent timing for all error cases
                elapsed = time.time() - start_time
                if elapsed < 0.001:  # Minimum 1ms delay
                    time.sleep(0.001 - elapsed)
                raise HTTPException(
                    status_code=status.HTTP_401_UNAUTHORIZED,
                    detail="Invalid authentication credentials",
                    headers={"WWW-Authenticate": "Bearer"},
                )

            # Validate permissions
            permissions_raw = payload.get("permissions", [])
            if not isinstance(permissions_raw, list):
                permissions = []
            else:
                # Ensure all permissions are strings
                permissions = [str(p) for p in permissions_raw if p]

            return TokenData(user_id=user_id, permissions=permissions)
        except jwt.ExpiredSignatureError:
            # Ensure consistent timing for all error cases
            elapsed = time.time() - start_time
            if elapsed < 0.001:  # Minimum 1ms delay
                time.sleep(0.001 - elapsed)
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Token expired",
                headers={"WWW-Authenticate": "Bearer"},
            )
        except (jwt.InvalidTokenError, jwt.DecodeError, ValueError, AttributeError):
            # Ensure consistent timing for all error cases
            elapsed = time.time() - start_time
            if elapsed < 0.001:  # Minimum 1ms delay
                time.sleep(0.001 - elapsed)
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Invalid authentication credentials",
                headers={"WWW-Authenticate": "Bearer"},
            )

    @staticmethod
    async def authenticate_user(
        db: AsyncSession, username: str, password: str
    ) -> User | None:
        stmt = select(User).where(User.username == username, User.is_active)
        result = await db.execute(stmt)
        user = result.scalar_one_or_none()

        if not user:
            return None
        if not AuthService.verify_password(password, user.hashed_password):
            return None
        return user

    @staticmethod
    async def get_user_permissions(db: AsyncSession, user_id: int) -> list[str]:
        stmt = (
            select(Role.permissions)
            .join(UserRole, UserRole.role_id == Role.id)
            .where(UserRole.user_id == user_id)
        )
        result = await db.execute(stmt)
        permissions = []
        for row in result.scalars():
            if row:
                try:
                    # Parse JSON string to list
                    role_permissions = json.loads(row) if isinstance(row, str) else row
                    if isinstance(role_permissions, list):
                        permissions.extend(role_permissions)
                except (json.JSONDecodeError, TypeError):
                    # Skip invalid JSON
                    continue
        return list(set(permissions))


async def get_current_user(
    credentials: HTTPAuthorizationCredentials = Depends(security),
    db: AsyncSession = Depends(get_db_session),
) -> User:
    token_data = await AuthService.verify_token(credentials.credentials)

    stmt = select(User).where(User.id == token_data.user_id, User.is_active)
    result = await db.execute(stmt)
    user = result.scalar_one_or_none()

    if user is None:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="User not found",
            headers={"WWW-Authenticate": "Bearer"},
        )
    return user


def require_permissions(*required_permissions: str) -> Any:
    async def permission_checker(
        current_user: User = Depends(get_current_user),
        db: AsyncSession = Depends(get_db_session),
    ) -> User:
        user_permissions = await AuthService.get_user_permissions(db, current_user.id)

        for permission in required_permissions:
            if permission not in user_permissions:
                raise HTTPException(
                    status_code=status.HTTP_403_FORBIDDEN,
                    detail=f"Insufficient permissions. Required: {permission}",
                )
        return current_user

    return permission_checker


def require_roles(*required_roles: str) -> Any:
    async def role_checker(
        current_user: User = Depends(get_current_user),
        db: AsyncSession = Depends(get_db_session),
    ) -> User:
        stmt = (
            select(Role.name)
            .join(UserRole, UserRole.role_id == Role.id)
            .where(UserRole.user_id == current_user.id)
        )
        result = await db.execute(stmt)
        user_roles = [row for row in result.scalars()]

        for role in required_roles:
            if role not in user_roles:
                raise HTTPException(
                    status_code=status.HTTP_403_FORBIDDEN,
                    detail=f"Insufficient role. Required: {role}",
                )
        return current_user

    return role_checker
