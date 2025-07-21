"""Configuration for observability components."""

from typing import Any

from pydantic import BaseModel, Field
from pydantic_settings import BaseSettings


class TracingConfig(BaseModel):
    """Configuration for distributed tracing."""

    enabled: bool = True
    service_name: str = "analytics-backend"
    jaeger_endpoint: str | None = "http://localhost:14268/api/traces"
    sample_rate: float = 1.0  # Sample all traces in development
    max_tag_value_length: int = 1024
    max_export_batch_size: int = 512
    export_timeout_millis: int = 30000


class MetricsConfig(BaseModel):
    """Configuration for metrics collection."""

    enabled: bool = True
    prometheus_port: int = 8090
    prometheus_endpoint: str = "/metrics"
    collection_interval_seconds: int = 15
    histogram_buckets: list[float] = Field(
        default_factory=lambda: [
            0.005,
            0.01,
            0.025,
            0.05,
            0.1,
            0.25,
            0.5,
            1.0,
            2.5,
            5.0,
            10.0,
        ]
    )
    enable_runtime_metrics: bool = True


class LoggingConfig(BaseModel):
    """Configuration for structured logging."""

    level: str = "INFO"
    format: str = "json"  # json or console
    enable_correlation: bool = True
    enable_tracing_integration: bool = True
    processors: list[str] = Field(
        default_factory=lambda: [
            "structlog.contextvars.merge_contextvars",
            "structlog.processors.TimeStamper",
            "structlog.processors.add_log_level",
            "structlog.processors.StackInfoRenderer",
        ]
    )


class AlertingConfig(BaseModel):
    """Configuration for monitoring alerting."""

    enabled: bool = True
    webhook_url: str | None = None
    slack_webhook: str | None = None
    email_config: dict[str, Any] | None = None
    pagerduty_config: dict[str, Any] | None = None
    alert_rules: dict[str, Any] = Field(default_factory=dict)


class ObservabilityConfig(BaseSettings):
    """Main observability configuration."""

    enabled: bool = True
    environment: str = "development"
    service_name: str = "analytics-backend"
    service_version: str = "1.0.0"

    tracing: TracingConfig = Field(default_factory=TracingConfig)
    metrics: MetricsConfig = Field(default_factory=MetricsConfig)
    logging: LoggingConfig = Field(default_factory=LoggingConfig)
    alerting: AlertingConfig = Field(default_factory=AlertingConfig)

    # Resource attributes for OpenTelemetry
    resource_attributes: dict[str, str] = Field(default_factory=dict)

    class Config:
        env_prefix = "OBSERVABILITY_"
        env_nested_delimiter = "__"


_observability_config: ObservabilityConfig | None = None


def get_observability_config() -> ObservabilityConfig:
    """Get the observability configuration singleton."""
    global _observability_config

    if _observability_config is None:
        _observability_config = ObservabilityConfig()

    return _observability_config


def configure_observability_config(config: ObservabilityConfig) -> None:
    """Configure the observability settings (for testing)."""
    global _observability_config
    _observability_config = config
