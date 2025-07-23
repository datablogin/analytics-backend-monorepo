"""Core database models for analytics platform."""

from datetime import datetime
from typing import Any

from sqlalchemy import (
    JSON,
    Boolean,
    DateTime,
    Float,
    ForeignKey,
    Index,
    Integer,
    String,
    Text,
    func,
)
from sqlalchemy.orm import Mapped, mapped_column, relationship

from .database import Base


class BaseModel(Base):  # type: ignore[misc]
    """Base model with common fields."""

    __abstract__ = True

    id: Mapped[int] = mapped_column(Integer, primary_key=True, index=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        onupdate=func.now(),
        nullable=False,
    )


class User(BaseModel):
    """User model for authentication and authorization."""

    __tablename__ = "users"

    email: Mapped[str] = mapped_column(
        String(255), unique=True, index=True, nullable=False
    )
    username: Mapped[str] = mapped_column(
        String(50), unique=True, index=True, nullable=False
    )
    full_name: Mapped[str | None] = mapped_column(String(255), nullable=True)
    hashed_password: Mapped[str] = mapped_column(String(255), nullable=False)
    is_active: Mapped[bool] = mapped_column(Boolean, default=True, nullable=False)
    is_superuser: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False)

    # Relationships
    user_roles: Mapped[list["UserRole"]] = relationship(
        back_populates="user", cascade="all, delete-orphan"
    )
    audit_logs: Mapped[list["AuditLog"]] = relationship(back_populates="user")


class Role(BaseModel):
    """Role model for RBAC."""

    __tablename__ = "roles"

    name: Mapped[str] = mapped_column(
        String(50), unique=True, index=True, nullable=False
    )
    description: Mapped[str | None] = mapped_column(Text, nullable=True)
    permissions: Mapped[str | None] = mapped_column(Text, nullable=True)  # JSON string
    is_active: Mapped[bool] = mapped_column(Boolean, default=True, nullable=False)

    # Relationships
    user_roles: Mapped[list["UserRole"]] = relationship(
        back_populates="role", cascade="all, delete-orphan"
    )


class UserRole(BaseModel):
    """Many-to-many relationship between users and roles."""

    __tablename__ = "user_roles"
    __table_args__ = (
        Index("ix_user_roles_user_id_role_id", "user_id", "role_id"),
        Index("ix_user_roles_role_id_user_id", "role_id", "user_id"),
    )

    user_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("users.id"), nullable=False, index=True
    )
    role_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("roles.id"), nullable=False, index=True
    )

    # Relationships
    user: Mapped["User"] = relationship(back_populates="user_roles")
    role: Mapped["Role"] = relationship(back_populates="user_roles")


class AuditLog(BaseModel):
    """Audit log for tracking user actions."""

    __tablename__ = "audit_logs"

    user_id: Mapped[int | None] = mapped_column(
        Integer, ForeignKey("users.id"), nullable=True, index=True
    )
    action: Mapped[str] = mapped_column(String(100), nullable=False, index=True)
    resource: Mapped[str | None] = mapped_column(String(100), nullable=True, index=True)
    resource_id: Mapped[str | None] = mapped_column(String(100), nullable=True)
    details: Mapped[str | None] = mapped_column(Text, nullable=True)  # JSON string
    ip_address: Mapped[str | None] = mapped_column(String(45), nullable=True)
    user_agent: Mapped[str | None] = mapped_column(Text, nullable=True)

    # Relationships
    user: Mapped["User | None"] = relationship(back_populates="audit_logs")


class Experiment(BaseModel):
    """ML Experiment tracking model."""

    __tablename__ = "experiments"

    name: Mapped[str] = mapped_column(
        String(255), unique=True, index=True, nullable=False
    )
    description: Mapped[str | None] = mapped_column(Text, nullable=True)
    tags: Mapped[dict[str, Any] | None] = mapped_column(JSON, nullable=True)
    artifact_location: Mapped[str | None] = mapped_column(String(500), nullable=True)
    lifecycle_stage: Mapped[str] = mapped_column(
        String(50), default="active", nullable=False, index=True
    )
    owner_id: Mapped[int | None] = mapped_column(
        Integer, ForeignKey("users.id"), nullable=True, index=True
    )

    # Relationships
    owner: Mapped["User | None"] = relationship()
    runs: Mapped[list["ExperimentRun"]] = relationship(
        back_populates="experiment", cascade="all, delete-orphan"
    )


class ExperimentRun(BaseModel):
    """ML Experiment run tracking model."""

    __tablename__ = "experiment_runs"

    experiment_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("experiments.id"), nullable=False, index=True
    )
    run_uuid: Mapped[str] = mapped_column(
        String(32), unique=True, index=True, nullable=False
    )
    name: Mapped[str | None] = mapped_column(String(250), nullable=True)
    source_type: Mapped[str | None] = mapped_column(String(20), nullable=True)
    source_name: Mapped[str | None] = mapped_column(String(500), nullable=True)
    entry_point_name: Mapped[str | None] = mapped_column(String(50), nullable=True)
    user_id: Mapped[str | None] = mapped_column(String(256), nullable=True)
    status: Mapped[str] = mapped_column(
        String(20), default="RUNNING", nullable=False, index=True
    )
    start_time: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    end_time: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    lifecycle_stage: Mapped[str] = mapped_column(
        String(20), default="active", nullable=False
    )
    artifact_uri: Mapped[str | None] = mapped_column(String(200), nullable=True)

    # Relationships
    experiment: Mapped["Experiment"] = relationship(back_populates="runs")
    metrics: Mapped[list["Metric"]] = relationship(
        back_populates="run", cascade="all, delete-orphan"
    )
    params: Mapped[list["Param"]] = relationship(
        back_populates="run", cascade="all, delete-orphan"
    )
    tags: Mapped[list["RunTag"]] = relationship(
        back_populates="run", cascade="all, delete-orphan"
    )


class Metric(BaseModel):
    """ML Experiment metric tracking model."""

    __tablename__ = "metrics"
    __table_args__ = (Index("idx_metrics_run_key", "run_uuid", "key"),)

    run_uuid: Mapped[str] = mapped_column(
        String(32), ForeignKey("experiment_runs.run_uuid"), nullable=False, index=True
    )
    key: Mapped[str] = mapped_column(String(250), nullable=False, index=True)
    value: Mapped[float] = mapped_column(Float, nullable=False)
    timestamp: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, index=True
    )
    step: Mapped[int] = mapped_column(Integer, default=0, nullable=False)

    # Relationships
    run: Mapped["ExperimentRun"] = relationship(back_populates="metrics")


class Param(BaseModel):
    """ML Experiment parameter tracking model."""

    __tablename__ = "params"
    __table_args__ = (Index("idx_params_run_key", "run_uuid", "key"),)

    run_uuid: Mapped[str] = mapped_column(
        String(32), ForeignKey("experiment_runs.run_uuid"), nullable=False, index=True
    )
    key: Mapped[str] = mapped_column(String(250), nullable=False, index=True)
    value: Mapped[str] = mapped_column(String(6000), nullable=False)

    # Relationships
    run: Mapped["ExperimentRun"] = relationship(back_populates="params")


class RunTag(BaseModel):
    """ML Experiment run tag tracking model."""

    __tablename__ = "run_tags"
    __table_args__ = (Index("idx_run_tags_run_key", "run_uuid", "key"),)

    run_uuid: Mapped[str] = mapped_column(
        String(32), ForeignKey("experiment_runs.run_uuid"), nullable=False, index=True
    )
    key: Mapped[str] = mapped_column(String(250), nullable=False, index=True)
    value: Mapped[str] = mapped_column(String(5000), nullable=False)

    # Relationships
    run: Mapped["ExperimentRun"] = relationship(back_populates="tags")
