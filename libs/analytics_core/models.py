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


class ABTestExperiment(BaseModel):
    """A/B Test Experiment model for database persistence."""

    __tablename__ = "ab_test_experiments"

    # Basic information
    experiment_uuid: Mapped[str] = mapped_column(
        String(36), unique=True, index=True, nullable=False
    )
    name: Mapped[str] = mapped_column(String(255), nullable=False, index=True)
    description: Mapped[str | None] = mapped_column(Text, nullable=True)

    # Experiment setup
    feature_flag_key: Mapped[str] = mapped_column(
        String(255), nullable=False, index=True
    )
    objective: Mapped[str] = mapped_column(String(50), nullable=False)
    hypothesis: Mapped[str] = mapped_column(Text, nullable=False)
    variants: Mapped[dict[str, Any]] = mapped_column(JSON, nullable=False)

    # Targeting and segmentation
    target_audience: Mapped[dict[str, Any] | None] = mapped_column(JSON, nullable=True)
    exclusion_rules: Mapped[dict[str, Any] | None] = mapped_column(JSON, nullable=True)

    # Duration and stopping
    start_date: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    end_date: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    stopping_rules: Mapped[dict[str, Any] | None] = mapped_column(JSON, nullable=True)

    # Metrics and analysis
    primary_metric: Mapped[str] = mapped_column(String(100), nullable=False)
    secondary_metrics: Mapped[list[str] | None] = mapped_column(JSON, nullable=True)
    guardrail_metrics: Mapped[list[str] | None] = mapped_column(JSON, nullable=True)

    # Statistical settings
    significance_level: Mapped[float] = mapped_column(
        Float, default=0.05, nullable=False
    )
    power: Mapped[float] = mapped_column(Float, default=0.8, nullable=False)
    minimum_detectable_effect: Mapped[float | None] = mapped_column(
        Float, nullable=True
    )

    # Status and metadata
    status: Mapped[str] = mapped_column(
        String(50), default="draft", nullable=False, index=True
    )
    created_by: Mapped[int | None] = mapped_column(
        Integer, ForeignKey("users.id"), nullable=True, index=True
    )

    # MLflow integration
    mlflow_experiment_id: Mapped[str | None] = mapped_column(String(100), nullable=True)

    # Results
    results: Mapped[dict[str, Any] | None] = mapped_column(JSON, nullable=True)
    winner: Mapped[str | None] = mapped_column(String(100), nullable=True)
    confidence: Mapped[float | None] = mapped_column(Float, nullable=True)

    # Relationships
    creator: Mapped["User | None"] = relationship()
    assignments: Mapped[list["ABTestAssignment"]] = relationship(
        back_populates="experiment", cascade="all, delete-orphan"
    )
    events: Mapped[list["ABTestEvent"]] = relationship(
        back_populates="experiment", cascade="all, delete-orphan"
    )


class ABTestAssignment(BaseModel):
    """A/B Test user assignment model."""

    __tablename__ = "ab_test_assignments"
    __table_args__ = (
        Index("ix_ab_assignments_exp_user", "experiment_id", "user_id"),
        Index("ix_ab_assignments_user_exp", "user_id", "experiment_id"),
    )

    experiment_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("ab_test_experiments.id"), nullable=False, index=True
    )
    user_id: Mapped[str] = mapped_column(String(255), nullable=False, index=True)
    variant: Mapped[str] = mapped_column(String(100), nullable=False, index=True)
    assigned_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )
    context: Mapped[dict[str, Any] | None] = mapped_column(JSON, nullable=True)

    # Relationships
    experiment: Mapped["ABTestExperiment"] = relationship(back_populates="assignments")


class ABTestEvent(BaseModel):
    """A/B Test event tracking model."""

    __tablename__ = "ab_test_events"
    __table_args__ = (
        Index("ix_ab_events_exp_user", "experiment_id", "user_id"),
        Index("ix_ab_events_type_timestamp", "event_type", "timestamp"),
        Index("ix_ab_events_variant_type", "variant", "event_type"),
    )

    experiment_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("ab_test_experiments.id"), nullable=False, index=True
    )
    user_id: Mapped[str] = mapped_column(String(255), nullable=False, index=True)
    variant: Mapped[str] = mapped_column(String(100), nullable=False, index=True)
    event_type: Mapped[str] = mapped_column(String(100), nullable=False, index=True)
    event_value: Mapped[float | None] = mapped_column(Float, nullable=True)
    properties: Mapped[dict[str, Any] | None] = mapped_column(JSON, nullable=True)
    timestamp: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False, index=True
    )

    # Relationships
    experiment: Mapped["ABTestExperiment"] = relationship(back_populates="events")
