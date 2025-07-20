"""Standardized response models for all API endpoints."""

from datetime import datetime
from typing import Any, Generic, TypeVar
from uuid import uuid4

from pydantic import BaseModel, Field

T = TypeVar("T")


class APIMetadata(BaseModel):
    """Metadata included in all API responses."""

    request_id: str = Field(
        default_factory=lambda: str(uuid4()), description="Unique request identifier"
    )
    timestamp: datetime = Field(
        default_factory=datetime.utcnow, description="Response timestamp (UTC)"
    )
    version: str = Field(default="v1", description="API version")
    environment: str = Field(default="development", description="Environment name")


class StandardResponse(BaseModel, Generic[T]):
    """Standard response format for all API endpoints."""

    success: bool = Field(description="Whether the request was successful")
    data: T | None = Field(default=None, description="Response data")
    message: str | None = Field(default=None, description="Human-readable message")
    metadata: APIMetadata = Field(
        default_factory=APIMetadata, description="Response metadata"
    )


class ErrorDetail(BaseModel):
    """Detailed error information."""

    code: str = Field(description="Error code")
    message: str = Field(description="Error message")
    field: str | None = Field(
        default=None, description="Field that caused the error (for validation errors)"
    )


class ErrorResponse(BaseModel):
    """Standard error response format."""

    success: bool = Field(default=False, description="Always false for errors")
    error: ErrorDetail = Field(description="Error details")
    metadata: APIMetadata = Field(
        default_factory=APIMetadata, description="Response metadata"
    )


class PaginationMeta(BaseModel):
    """Pagination metadata for list responses."""

    page: int = Field(description="Current page number (1-based)")
    per_page: int = Field(description="Number of items per page")
    total_items: int = Field(description="Total number of items")
    total_pages: int = Field(description="Total number of pages")
    has_next: bool = Field(description="Whether there is a next page")
    has_prev: bool = Field(description="Whether there is a previous page")


class PaginatedResponse(BaseModel, Generic[T]):
    """Standard paginated response format."""

    success: bool = Field(
        default=True, description="Whether the request was successful"
    )
    data: list[T] = Field(description="List of items")
    pagination: PaginationMeta = Field(description="Pagination metadata")
    metadata: APIMetadata = Field(
        default_factory=APIMetadata, description="Response metadata"
    )


class HealthStatus(BaseModel):
    """Health check response model."""

    status: str = Field(
        description="Overall health status", examples=["healthy", "unhealthy"]
    )
    checks: dict[str, Any] = Field(
        default_factory=dict, description="Individual health check results"
    )
    version: str = Field(description="Application version")
    uptime: float | None = Field(
        default=None, description="Application uptime in seconds"
    )


class ValidationErrorDetail(BaseModel):
    """Validation error detail."""

    field: str = Field(description="Field name that failed validation")
    message: str = Field(description="Validation error message")
    invalid_value: Any = Field(description="The invalid value that was provided")


class ValidationErrorResponse(BaseModel):
    """Response for validation errors."""

    success: bool = Field(
        default=False, description="Always false for validation errors"
    )
    error: ErrorDetail = Field(description="Main error information")
    validation_errors: list[ValidationErrorDetail] = Field(
        description="Detailed validation errors"
    )
    metadata: APIMetadata = Field(
        default_factory=APIMetadata, description="Response metadata"
    )
