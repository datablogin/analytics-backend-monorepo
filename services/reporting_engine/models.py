"""Data models for reporting engine."""

import uuid
from datetime import datetime
from enum import Enum
from typing import Any

from pydantic import BaseModel, Field
from sqlalchemy import JSON, Column, DateTime, String, Text
from sqlalchemy import Enum as SQLEnum
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.sql import func

from libs.analytics_core.database import Base


class ReportType(str, Enum):
    """Types of reports that can be generated."""

    DATA_QUALITY = "data_quality"
    ANALYTICS_DASHBOARD = "analytics_dashboard"
    AB_TEST_RESULTS = "ab_test_results"
    ML_MODEL_PERFORMANCE = "ml_model_performance"
    STREAMING_METRICS = "streaming_metrics"
    CUSTOM = "custom"


class ReportFormat(str, Enum):
    """Supported export formats."""

    PDF = "pdf"
    EXCEL = "excel"
    CSV = "csv"
    JSON = "json"
    HTML = "html"


class ReportStatus(str, Enum):
    """Report generation status."""

    PENDING = "pending"
    IN_PROGRESS = "in_progress"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


class ReportFrequency(str, Enum):
    """Scheduled report frequency."""

    ONCE = "once"
    DAILY = "daily"
    WEEKLY = "weekly"
    MONTHLY = "monthly"
    QUARTERLY = "quarterly"


# SQLAlchemy Models
class Report(Base):  # type: ignore[misc]
    """Report database model."""

    __tablename__ = "reports"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    name = Column(String(255), nullable=False)
    description = Column(Text)
    report_type: ReportType = Column(SQLEnum(ReportType), nullable=False)
    status: ReportStatus = Column(SQLEnum(ReportStatus), default=ReportStatus.PENDING)

    # Configuration
    config = Column(JSON, nullable=False)
    filters = Column(JSON, default={})

    # Scheduling
    frequency: ReportFrequency = Column(
        SQLEnum(ReportFrequency), default=ReportFrequency.ONCE
    )
    next_run = Column(DateTime(timezone=True))

    # Metadata
    created_by = Column(String(255), nullable=False)
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), onupdate=func.now())

    # Results
    file_path = Column(String(512))
    error_message = Column(Text)
    processing_time_ms = Column(String(50))


class ReportExecution(Base):  # type: ignore[misc]
    """Report execution history."""

    __tablename__ = "report_executions"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    report_id = Column(UUID(as_uuid=True), nullable=False)

    status: ReportStatus = Column(SQLEnum(ReportStatus), default=ReportStatus.PENDING)
    format: ReportFormat = Column(SQLEnum(ReportFormat), nullable=False)

    started_at = Column(DateTime(timezone=True), server_default=func.now())
    completed_at = Column(DateTime(timezone=True))

    file_path = Column(String(512))
    file_size_bytes = Column(String(50))
    error_message = Column(Text)

    # Metadata
    task_id = Column(String(255))  # Celery task ID
    processing_time_ms = Column(String(50))


# Pydantic Models
class ReportConfigBase(BaseModel):
    """Base report configuration."""

    title: str
    description: str | None = None
    date_range: dict[str, Any] | None = None
    filters: dict[str, Any] = Field(default_factory=dict)


class DataQualityReportConfig(ReportConfigBase):
    """Data quality report configuration."""

    datasets: list[str]
    quality_checks: list[str]
    threshold_alerts: bool = True


class AnalyticsDashboardConfig(ReportConfigBase):
    """Analytics dashboard configuration."""

    metrics: list[str]
    visualizations: list[str]
    time_granularity: str = "daily"
    include_trends: bool = True


class ABTestReportConfig(ReportConfigBase):
    """A/B test results report configuration."""

    experiment_ids: list[str]
    metrics: list[str]
    confidence_level: float = 0.95
    include_statistical_analysis: bool = True


class MLModelPerformanceConfig(ReportConfigBase):
    """ML model performance report configuration."""

    model_names: list[str]
    performance_metrics: list[str]
    time_period: str = "30d"
    include_drift_analysis: bool = True


class StreamingMetricsConfig(ReportConfigBase):
    """Streaming metrics report configuration."""

    services: list[str]
    metrics: list[str]
    aggregation_level: str = "hourly"
    include_alerts: bool = True


class ReportCreateRequest(BaseModel):
    """Request to create a new report."""

    name: str
    description: str | None = None
    report_type: ReportType
    config: dict[str, Any]
    filters: dict[str, Any] = Field(default_factory=dict)
    frequency: ReportFrequency = ReportFrequency.ONCE
    next_run: datetime | None = None


class ReportUpdateRequest(BaseModel):
    """Request to update an existing report."""

    name: str | None = None
    description: str | None = None
    config: dict[str, Any] | None = None
    filters: dict[str, Any] | None = None
    frequency: ReportFrequency | None = None
    next_run: datetime | None = None


class ReportResponse(BaseModel):
    """Report response model."""

    id: str
    name: str
    description: str | None
    report_type: ReportType
    status: ReportStatus
    config: dict[str, Any]
    filters: dict[str, Any]
    frequency: ReportFrequency
    next_run: datetime | None
    created_by: str
    created_at: datetime
    updated_at: datetime | None
    file_path: str | None
    error_message: str | None
    processing_time_ms: str | None

    class Config:
        from_attributes = True


class ReportExecutionRequest(BaseModel):
    """Request to execute a report."""

    format: ReportFormat = ReportFormat.PDF
    async_execution: bool = True


class ReportExecutionResponse(BaseModel):
    """Report execution response."""

    id: str
    report_id: str
    status: ReportStatus
    format: ReportFormat
    started_at: datetime
    completed_at: datetime | None
    file_path: str | None
    file_size_bytes: str | None
    error_message: str | None
    task_id: str | None
    processing_time_ms: str | None

    class Config:
        from_attributes = True


class DashboardConfig(BaseModel):
    """Dashboard configuration."""

    name: str
    description: str | None = None
    layout: dict[str, Any]
    widgets: list[dict[str, Any]]
    refresh_interval: int = 300  # seconds
    filters: dict[str, Any] = Field(default_factory=dict)


class DashboardCreateRequest(BaseModel):
    """Request to create a dashboard."""

    name: str
    description: str | None = None
    layout: dict[str, Any]
    widgets: list[dict[str, Any]]
    refresh_interval: int = 300
    filters: dict[str, Any] = Field(default_factory=dict)


class WidgetConfig(BaseModel):
    """Widget configuration for dashboards."""

    type: str  # chart, table, metric, etc.
    title: str
    data_source: str
    query: dict[str, Any]
    visualization: dict[str, Any]
    size: dict[str, int] = {"width": 6, "height": 4}
    position: dict[str, int] = {"x": 0, "y": 0}


class ExportRequest(BaseModel):
    """Export request model."""

    format: ReportFormat
    data_source: str
    query: dict[str, Any] | None = None
    filters: dict[str, Any] = Field(default_factory=dict)
    filename: str | None = None
