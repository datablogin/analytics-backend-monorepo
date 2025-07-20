
import json
from fastapi import APIRouter, Depends, HTTPException, status
from pydantic import BaseModel
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from libs.analytics_core.auth import require_permissions
from libs.analytics_core.database import get_db_session
from libs.analytics_core.models import Role, User, UserRole

router = APIRouter(prefix="/admin", tags=["admin"])


class RoleCreate(BaseModel):
    name: str
    description: str
    permissions: list[str]


class RoleResponse(BaseModel):
    model_config = {"from_attributes": True}
    
    id: int
    name: str
    description: str
    permissions: list[str]
    is_active: bool


class UserRoleAssignment(BaseModel):
    user_id: int
    role_id: int


class UserWithRoles(BaseModel):
    model_config = {"from_attributes": True}
    
    id: int
    username: str
    email: str
    full_name: str
    is_active: bool
    roles: list[RoleResponse]


@router.post("/roles", response_model=RoleResponse)
async def create_role(
    role_data: RoleCreate,
    db: AsyncSession = Depends(get_db_session),
    current_user: User = Depends(require_permissions("admin:roles:create")),
):
    # Check if role already exists
    stmt = select(Role).where(Role.name == role_data.name)
    result = await db.execute(stmt)
    existing_role = result.scalar_one_or_none()

    if existing_role:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Role already exists",
        )

    new_role = Role(
        name=role_data.name,
        description=role_data.description,
        permissions=json.dumps(role_data.permissions),
    )

    db.add(new_role)
    await db.commit()
    await db.refresh(new_role)

    return RoleResponse.model_validate(new_role)


@router.get("/roles", response_model=list[RoleResponse])
async def get_roles(
    db: Session = Depends(get_db),
    current_user: User = Depends(require_permissions("admin:roles:read")),
):
    stmt = select(Role).where(Role.is_active)
    result = await db.execute(stmt)
    roles = result.scalars().all()

    return [RoleResponse.model_validate(role) for role in roles]


@router.put("/roles/{role_id}", response_model=RoleResponse)
async def update_role(
    role_id: int,
    role_data: RoleCreate,
    db: Session = Depends(get_db),
    current_user: User = Depends(require_permissions("admin:roles:update")),
):
    stmt = select(Role).where(Role.id == role_id, Role.is_active)
    result = await db.execute(stmt)
    role = result.scalar_one_or_none()

    if not role:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Role not found",
        )

    role.name = role_data.name
    role.description = role_data.description
    role.permissions = role_data.permissions

    await db.commit()
    await db.refresh(role)

    return RoleResponse.model_validate(role)


@router.delete("/roles/{role_id}")
async def delete_role(
    role_id: int,
    db: Session = Depends(get_db),
    current_user: User = Depends(require_permissions("admin:roles:delete")),
):
    stmt = select(Role).where(Role.id == role_id, Role.is_active)
    result = await db.execute(stmt)
    role = result.scalar_one_or_none()

    if not role:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Role not found",
        )

    # Soft delete
    role.is_active = False
    await db.commit()

    return {"message": "Role deleted successfully"}


@router.post("/users/{user_id}/roles")
async def assign_role_to_user(
    user_id: int,
    role_assignment: UserRoleAssignment,
    db: Session = Depends(get_db),
    current_user: User = Depends(require_permissions("admin:users:assign_roles")),
):
    # Check if user exists
    stmt = select(User).where(User.id == user_id, User.is_active)
    result = await db.execute(stmt)
    user = result.scalar_one_or_none()

    if not user:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="User not found",
        )

    # Check if role exists
    stmt = select(Role).where(
        Role.id == role_assignment.role_id, Role.is_active
    )
    result = await db.execute(stmt)
    role = result.scalar_one_or_none()

    if not role:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Role not found",
        )

    # Check if assignment already exists
    stmt = select(UserRole).where(
        UserRole.user_id == user_id,
        UserRole.role_id == role_assignment.role_id,
    )
    result = await db.execute(stmt)
    existing_assignment = result.scalar_one_or_none()

    if existing_assignment:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="User already has this role",
        )

    user_role = UserRole(
        user_id=user_id,
        role_id=role_assignment.role_id,
    )

    db.add(user_role)
    await db.commit()

    return {"message": "Role assigned successfully"}


@router.delete("/users/{user_id}/roles/{role_id}")
async def remove_role_from_user(
    user_id: int,
    role_id: int,
    db: Session = Depends(get_db),
    current_user: User = Depends(require_permissions("admin:users:remove_roles")),
):
    stmt = select(UserRole).where(
        UserRole.user_id == user_id,
        UserRole.role_id == role_id,
    )
    result = await db.execute(stmt)
    user_role = result.scalar_one_or_none()

    if not user_role:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Role assignment not found",
        )

    await db.delete(user_role)
    await db.commit()

    return {"message": "Role removed successfully"}


@router.get("/users", response_model=list[UserWithRoles])
async def get_users_with_roles(
    db: Session = Depends(get_db),
    current_user: User = Depends(require_permissions("admin:users:read")),
):
    stmt = select(User).where(User.is_active)
    result = await db.execute(stmt)
    users = result.scalars().all()

    users_with_roles = []
    for user in users:
        # Get user roles
        stmt = (
            select(Role)
            .join(UserRole, UserRole.role_id == Role.id)
            .where(UserRole.user_id == user.id, Role.is_active)
        )
        result = await db.execute(stmt)
        roles = result.scalars().all()

        user_data = UserWithRoles(
            id=user.id,
            username=user.username,
            email=user.email,
            full_name=user.full_name,
            is_active=user.is_active,
            roles=[RoleResponse.model_validate(role) for role in roles],
        )
        users_with_roles.append(user_data)

    return users_with_roles
