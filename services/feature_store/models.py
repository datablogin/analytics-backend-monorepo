"""Feature store data models."""

from datetime import datetime
from enum import Enum
from typing import Any

from libs.analytics_core.models import BaseModel as SQLBaseModel
from pydantic import BaseModel, Field
from sqlalchemy import (
    DateTime,
    Index,
    Integer,
    String,
    Text,
)
from sqlalchemy import (
    Enum as SQLEnum,
)
from sqlalchemy.orm import Mapped, mapped_column


class FeatureType(str, Enum):
    """Supported feature data types."""

    STRING = "string"
    INTEGER = "integer"
    FLOAT = "float"
    BOOLEAN = "boolean"
    TIMESTAMP = "timestamp"
    JSON = "json"


class FeatureStatus(str, Enum):
    """Feature definition status."""

    ACTIVE = "active"
    INACTIVE = "inactive"
    DEPRECATED = "deprecated"


# SQLAlchemy Models
class Feature(SQLBaseModel):
    """Feature definition model."""

    __tablename__ = "features"
    __table_args__ = (
        Index("ix_features_name", "name"),
        Index("ix_features_feature_group", "feature_group"),
        Index("ix_features_status", "status"),
    )

    name: Mapped[str] = mapped_column(
        String(255), unique=True, nullable=False, index=True
    )
    feature_group: Mapped[str] = mapped_column(String(255), nullable=False, index=True)
    description: Mapped[str | None] = mapped_column(Text, nullable=True)
    feature_type: Mapped[FeatureType] = mapped_column(
        SQLEnum(FeatureType), nullable=False
    )
    status: Mapped[FeatureStatus] = mapped_column(
        SQLEnum(FeatureStatus), default=FeatureStatus.ACTIVE, nullable=False
    )
    default_value: Mapped[str | None] = mapped_column(
        Text, nullable=True
    )  # JSON string
    validation_rules: Mapped[str | None] = mapped_column(
        Text, nullable=True
    )  # JSON string
    tags: Mapped[str | None] = mapped_column(Text, nullable=True)  # JSON string
    owner: Mapped[str | None] = mapped_column(String(255), nullable=True)
    version: Mapped[int] = mapped_column(Integer, default=1, nullable=False)


class FeatureValue(SQLBaseModel):
    """Feature value storage model."""

    __tablename__ = "feature_values"
    __table_args__ = (
        Index("ix_feature_values_feature_name", "feature_name"),
        Index("ix_feature_values_entity_id", "entity_id"),
        Index("ix_feature_values_timestamp", "timestamp"),
        Index("ix_feature_values_composite", "feature_name", "entity_id", "timestamp"),
    )

    feature_name: Mapped[str] = mapped_column(String(255), nullable=False, index=True)
    entity_id: Mapped[str] = mapped_column(String(255), nullable=False, index=True)
    value: Mapped[str] = mapped_column(Text, nullable=False)  # JSON string
    timestamp: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, index=True
    )
    version: Mapped[int] = mapped_column(Integer, default=1, nullable=False)


# Pydantic Models for API
class FeatureDefinitionCreate(BaseModel):
    """Model for creating a feature definition."""

    name: str = Field(
        ..., description="Unique feature name", min_length=1, max_length=255
    )
    feature_group: str = Field(
        ..., description="Feature group name", min_length=1, max_length=255
    )
    description: str | None = Field(None, description="Feature description")
    feature_type: FeatureType = Field(..., description="Feature data type")
    default_value: Any | None = Field(None, description="Default value for the feature")
    validation_rules: dict[str, Any] | None = Field(
        None, description="Validation rules"
    )
    tags: list[str] | None = Field(None, description="Feature tags")
    owner: str | None = Field(None, description="Feature owner", max_length=255)


class FeatureDefinitionUpdate(BaseModel):
    """Model for updating a feature definition."""

    description: str | None = Field(None, description="Feature description")
    status: FeatureStatus | None = Field(None, description="Feature status")
    default_value: Any | None = Field(None, description="Default value for the feature")
    validation_rules: dict[str, Any] | None = Field(
        None, description="Validation rules"
    )
    tags: list[str] | None = Field(None, description="Feature tags")
    owner: str | None = Field(None, description="Feature owner", max_length=255)


class FeatureDefinitionResponse(BaseModel):
    """Model for feature definition response."""

    id: int = Field(..., description="Feature ID")
    name: str = Field(..., description="Feature name")
    feature_group: str = Field(..., description="Feature group name")
    description: str | None = Field(None, description="Feature description")
    feature_type: FeatureType = Field(..., description="Feature data type")
    status: FeatureStatus = Field(..., description="Feature status")
    default_value: Any | None = Field(None, description="Default value")
    validation_rules: dict[str, Any] | None = Field(
        None, description="Validation rules"
    )
    tags: list[str] | None = Field(None, description="Feature tags")
    owner: str | None = Field(None, description="Feature owner")
    version: int = Field(..., description="Feature version")
    created_at: datetime = Field(..., description="Creation timestamp")
    updated_at: datetime = Field(..., description="Last update timestamp")

    class Config:
        from_attributes = True


class FeatureValueWrite(BaseModel):
    """Model for writing feature values."""

    feature_name: str = Field(
        ..., description="Feature name", min_length=1, max_length=255
    )
    entity_id: str = Field(
        ..., description="Entity identifier", min_length=1, max_length=255
    )
    value: Any = Field(..., description="Feature value")
    timestamp: datetime | None = Field(
        None, description="Value timestamp (defaults to now)"
    )


class FeatureValueBatchWrite(BaseModel):
    """Model for batch writing feature values."""

    values: list[FeatureValueWrite] = Field(
        description="Feature values to write", min_length=1
    )


class FeatureValueRead(BaseModel):
    """Model for reading feature values."""

    feature_names: list[str] = Field(description="Feature names to read", min_length=1)
    entity_ids: list[str] = Field(description="Entity identifiers", min_length=1)
    timestamp: datetime | None = Field(None, description="Point-in-time timestamp")


class FeatureValueResponse(BaseModel):
    """Model for feature value response."""

    id: int = Field(..., description="Value ID")
    feature_name: str = Field(..., description="Feature name")
    entity_id: str = Field(..., description="Entity identifier")
    value: Any = Field(..., description="Feature value")
    timestamp: datetime = Field(..., description="Value timestamp")
    version: int = Field(..., description="Value version")
    created_at: datetime = Field(..., description="Creation timestamp")

    class Config:
        from_attributes = True


class FeatureServingResponse(BaseModel):
    """Model for feature serving response."""

    entity_id: str = Field(..., description="Entity identifier")
    features: dict[str, Any] = Field(..., description="Feature name to value mapping")
    timestamp: datetime = Field(..., description="Serving timestamp")


class FeatureDiscoveryResponse(BaseModel):
    """Model for feature discovery response."""

    feature_groups: list[str] = Field(..., description="Available feature groups")
    total_features: int = Field(..., description="Total number of features")
    features_by_type: dict[FeatureType, int] = Field(
        ..., description="Feature count by type"
    )
    features_by_status: dict[FeatureStatus, int] = Field(
        ..., description="Feature count by status"
    )


class FeatureValidationResult(BaseModel):
    """Model for feature validation result."""

    feature_name: str = Field(..., description="Feature name")
    is_valid: bool = Field(..., description="Whether the value is valid")
    errors: list[str] = Field(..., description="Validation errors")
    warnings: list[str] = Field(..., description="Validation warnings")
