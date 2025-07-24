"""Event schema registry and validation for streaming analytics."""

import json
from datetime import datetime
from enum import Enum
from typing import Any
from uuid import uuid4

import structlog
from pydantic import BaseModel, Field, ValidationError, validator

logger = structlog.get_logger(__name__)


class EventType(str, Enum):
    """Standard event types for analytics."""

    USER_ACTION = "user_action"
    SYSTEM_EVENT = "system_event"
    BUSINESS_EVENT = "business_event"
    ML_PREDICTION = "ml_prediction"
    DATA_QUALITY = "data_quality"
    ALERT = "alert"
    METRIC = "metric"


class EventSchema(BaseModel):
    """Base event schema for all streaming events."""

    # Core event fields
    event_id: str = Field(default_factory=lambda: str(uuid4()))
    event_type: EventType = Field(..., description="Type of event")
    event_name: str = Field(..., description="Specific event name")
    timestamp: datetime = Field(default_factory=datetime.utcnow)

    # Source information
    source_service: str = Field(..., description="Service that generated the event")
    source_version: str = Field(default="1.0.0", description="Version of source service")

    # Trace information
    trace_id: str | None = Field(default=None, description="Distributed trace ID")
    span_id: str | None = Field(default=None, description="Span ID")
    parent_span_id: str | None = Field(default=None, description="Parent span ID")

    # Event payload
    payload: dict[str, Any] = Field(..., description="Event data")

    # Metadata
    metadata: dict[str, Any] = Field(default_factory=dict, description="Additional metadata")
    schema_version: str = Field(default="1.0", description="Schema version")

    @validator('timestamp', pre=True)
    def validate_timestamp(cls, v):
        """Validate and convert timestamp."""
        if isinstance(v, int | float):
            return datetime.fromtimestamp(v)
        elif isinstance(v, str):
            return datetime.fromisoformat(v.replace('Z', '+00:00'))
        return v

    @validator('payload')
    def validate_payload(cls, v):
        """Validate payload is not empty."""
        if not v:
            raise ValueError("Event payload cannot be empty")
        return v

    def to_json(self) -> str:
        """Convert event to JSON string."""
        return self.model_dump_json()

    def to_dict(self) -> dict[str, Any]:
        """Convert event to dictionary."""
        return self.model_dump()

    @classmethod
    def from_json(cls, json_str: str) -> "EventSchema":
        """Create event from JSON string."""
        return cls.model_validate_json(json_str)

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "EventSchema":
        """Create event from dictionary."""
        return cls.model_validate(data)


class UserActionEvent(EventSchema):
    """User action event schema."""

    event_type: EventType = Field(default=EventType.USER_ACTION)

    # User action specific fields
    user_id: str = Field(..., description="User identifier")
    session_id: str = Field(..., description="Session identifier")
    action: str = Field(..., description="Action performed")

    # Context
    ip_address: str | None = Field(default=None)
    user_agent: str | None = Field(default=None)
    referrer: str | None = Field(default=None)

    @validator('payload')
    def validate_user_action_payload(cls, v):
        """Validate user action payload structure."""
        required_fields = ['page', 'element']
        for field in required_fields:
            if field not in v:
                raise ValueError(f"User action payload must contain '{field}'")
        return v


class SystemEvent(EventSchema):
    """System event schema."""

    event_type: EventType = Field(default=EventType.SYSTEM_EVENT)

    # System specific fields
    component: str = Field(..., description="System component")
    severity: str = Field(default="INFO", description="Event severity")

    @validator('severity')
    def validate_severity(cls, v):
        """Validate severity level."""
        valid_levels = ['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL']
        if v.upper() not in valid_levels:
            raise ValueError(f"Severity must be one of: {valid_levels}")
        return v.upper()


class MLPredictionEvent(EventSchema):
    """ML prediction event schema."""

    event_type: EventType = Field(default=EventType.ML_PREDICTION)

    # ML specific fields
    model_name: str = Field(..., description="Model name")
    model_version: str = Field(..., description="Model version")
    prediction_id: str = Field(default_factory=lambda: str(uuid4()))
    confidence_score: float | None = Field(default=None)
    features_used: list[str] = Field(default_factory=list)

    @validator('confidence_score')
    def validate_confidence(cls, v):
        """Validate confidence score range."""
        if v is not None and not (0.0 <= v <= 1.0):
            raise ValueError("Confidence score must be between 0.0 and 1.0")
        return v


class BusinessEvent(EventSchema):
    """Business event schema."""

    event_type: EventType = Field(default=EventType.BUSINESS_EVENT)

    # Business specific fields
    entity_type: str = Field(..., description="Business entity type")
    entity_id: str = Field(..., description="Business entity ID")
    value: float | None = Field(default=None, description="Monetary or numerical value")
    currency: str | None = Field(default="USD", description="Currency code")


class MetricEvent(EventSchema):
    """Metric event schema for streaming metrics."""

    event_type: EventType = Field(default=EventType.METRIC)

    # Metric specific fields
    metric_name: str = Field(..., description="Metric name")
    metric_value: int | float = Field(..., description="Metric value")
    metric_type: str = Field(..., description="Metric type (counter, gauge, histogram)")
    labels: dict[str, str] = Field(default_factory=dict, description="Metric labels")
    unit: str | None = Field(default=None, description="Metric unit")

    @validator('metric_type')
    def validate_metric_type(cls, v):
        """Validate metric type."""
        valid_types = ['counter', 'gauge', 'histogram', 'summary']
        if v.lower() not in valid_types:
            raise ValueError(f"Metric type must be one of: {valid_types}")
        return v.lower()


class EventStore:
    """Event schema registry and validation store."""

    def __init__(self):
        self.schemas: dict[str, type[EventSchema]] = {}
        self.logger = logger.bind(component="event_store")

        # Register built-in schemas
        self._register_builtin_schemas()

    def _register_builtin_schemas(self):
        """Register built-in event schemas."""
        builtin_schemas = {
            "base_event": EventSchema,
            "user_action": UserActionEvent,
            "system_event": SystemEvent,
            "ml_prediction": MLPredictionEvent,
            "business_event": BusinessEvent,
            "metric_event": MetricEvent,
        }

        for name, schema_class in builtin_schemas.items():
            self.schemas[name] = schema_class
            self.logger.debug("Registered built-in schema", schema_name=name)

    def register_schema(self, name: str, schema_class: type[EventSchema]) -> None:
        """Register a custom event schema."""
        if not issubclass(schema_class, EventSchema):
            raise ValueError("Schema class must inherit from EventSchema")

        self.schemas[name] = schema_class
        self.logger.info("Registered custom schema", schema_name=name)

    def get_schema(self, name: str) -> type[EventSchema] | None:
        """Get registered schema by name."""
        return self.schemas.get(name)

    def list_schemas(self) -> list[str]:
        """List all registered schema names."""
        return list(self.schemas.keys())

    def validate_event(self, event_data: dict[str, Any], schema_name: str = "base_event") -> EventSchema:
        """Validate event data against schema."""
        schema_class = self.get_schema(schema_name)
        if not schema_class:
            raise ValueError(f"Unknown schema: {schema_name}")

        try:
            event = schema_class.model_validate(event_data)
            self.logger.debug("Event validation successful",
                            event_id=event.event_id,
                            schema_name=schema_name)
            return event
        except ValidationError as e:
            self.logger.error("Event validation failed",
                            schema_name=schema_name,
                            errors=e.errors())
            raise

    def create_event(self,
                    event_type: EventType,
                    event_name: str,
                    payload: dict[str, Any],
                    source_service: str,
                    schema_name: str = "base_event",
                    **kwargs) -> EventSchema:
        """Create and validate a new event."""
        event_data = {
            "event_type": event_type,
            "event_name": event_name,
            "payload": payload,
            "source_service": source_service,
            **kwargs
        }

        return self.validate_event(event_data, schema_name)

    def serialize_event(self, event: EventSchema, format: str = "json") -> str | bytes | dict[str, Any]:
        """Serialize event to specified format."""
        if format == "json":
            return event.to_json()
        elif format == "dict":
            return event.to_dict()
        else:
            raise ValueError(f"Unsupported serialization format: {format}")

    def deserialize_event(self, data: str | bytes | dict[str, Any],
                         schema_name: str = "base_event") -> EventSchema:
        """Deserialize event from data."""
        if isinstance(data, str | bytes):
            event_data = json.loads(data)
        else:
            event_data = data

        return self.validate_event(event_data, schema_name)

    def get_schema_info(self, schema_name: str) -> dict[str, Any]:
        """Get information about a schema."""
        schema_class = self.get_schema(schema_name)
        if not schema_class:
            raise ValueError(f"Unknown schema: {schema_name}")

        # Generate JSON schema
        json_schema = schema_class.model_json_schema()

        return {
            "name": schema_name,
            "class_name": schema_class.__name__,
            "json_schema": json_schema,
            "required_fields": json_schema.get("required", []),
            "properties": list(json_schema.get("properties", {}).keys())
        }


# Global event store instance
_event_store = EventStore()


def get_event_store() -> EventStore:
    """Get the global event store instance."""
    return _event_store


def create_event(event_type: EventType,
                event_name: str,
                payload: dict[str, Any],
                source_service: str,
                **kwargs) -> EventSchema:
    """Convenient function to create events."""
    return _event_store.create_event(
        event_type=event_type,
        event_name=event_name,
        payload=payload,
        source_service=source_service,
        **kwargs
    )


def validate_event_data(event_data: dict[str, Any], schema_name: str = "base_event") -> EventSchema:
    """Convenient function to validate event data."""
    return _event_store.validate_event(event_data, schema_name)

