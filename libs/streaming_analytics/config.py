"""Configuration for streaming analytics components."""

from typing import Any

from pydantic import BaseModel, Field
from pydantic_settings import BaseSettings


class KafkaConfig(BaseModel):
    """Kafka cluster configuration."""

    bootstrap_servers: list[str] = Field(default=["localhost:9092"])
    security_protocol: str = Field(default="PLAINTEXT")
    sasl_mechanism: str | None = Field(default=None)
    sasl_username: str | None = Field(default=None)
    sasl_password: str | None = Field(default=None)
    ssl_ca_location: str | None = Field(default=None)
    ssl_cert_location: str | None = Field(default=None)
    ssl_key_location: str | None = Field(default=None)

    # Producer config
    producer_config: dict[str, Any] = Field(default_factory=lambda: {
        "acks": "all",
        "retries": 3,
        "batch_size": 16384,
        "linger_ms": 5,
        "buffer_memory": 33554432,
        "compression_type": "gzip",
        "enable_idempotence": True,
    })

    # Consumer config
    consumer_config: dict[str, Any] = Field(default_factory=lambda: {
        "auto_offset_reset": "earliest",
        "enable_auto_commit": False,
        "max_poll_records": 500,
        "session_timeout_ms": 30000,
        "heartbeat_interval_ms": 3000,
        "fetch_min_bytes": 1024,
        "fetch_max_wait_ms": 500,
    })


class StreamProcessingConfig(BaseModel):
    """Stream processing configuration."""

    # Window settings
    default_window_size_ms: int = Field(default=60000)  # 1 minute
    allowed_lateness_ms: int = Field(default=10000)  # 10 seconds
    watermark_interval_ms: int = Field(default=5000)  # 5 seconds

    # Processing settings
    parallelism: int = Field(default=4)
    max_batch_size: int = Field(default=1000)
    processing_timeout_ms: int = Field(default=30000)

    # State management
    state_backend: str = Field(default="memory")  # memory, rocksdb, redis
    checkpoint_interval_ms: int = Field(default=60000)
    state_ttl_ms: int | None = Field(default=3600000)  # 1 hour

    # Error handling
    retry_attempts: int = Field(default=3)
    retry_backoff_ms: int = Field(default=1000)
    dead_letter_topic: str | None = Field(default="dead-letter-queue")


class WebSocketConfig(BaseModel):
    """WebSocket server configuration."""

    host: str = Field(default="0.0.0.0")
    port: int = Field(default=8765)
    max_connections: int = Field(default=1000)
    ping_interval: int = Field(default=20)
    ping_timeout: int = Field(default=10)
    close_timeout: int = Field(default=10)

    # Message settings
    max_message_size: int = Field(default=1024 * 1024)  # 1MB
    compression: str | None = Field(default="deflate")

    # Authentication
    require_auth: bool = Field(default=True)
    auth_timeout_seconds: int = Field(default=30)


class RealtimeMLConfig(BaseModel):
    """Real-time ML inference configuration."""

    # Model settings
    model_cache_size: int = Field(default=10)
    model_warm_up_samples: int = Field(default=100)
    model_refresh_interval_seconds: int = Field(default=300)

    # Inference settings
    batch_inference: bool = Field(default=True)
    max_batch_size: int = Field(default=32)
    batch_timeout_ms: int = Field(default=100)

    # Feature store integration
    feature_cache_ttl_seconds: int = Field(default=300)
    feature_freshness_threshold_ms: int = Field(default=60000)

    # Performance targets
    max_latency_ms: int = Field(default=50)
    throughput_target_per_second: int = Field(default=1000)


class MonitoringConfig(BaseModel):
    """Streaming monitoring configuration."""

    # Metrics collection
    metrics_interval_seconds: int = Field(default=10)
    detailed_metrics: bool = Field(default=True)

    # Performance thresholds
    latency_warning_ms: int = Field(default=100)
    latency_critical_ms: int = Field(default=1000)
    throughput_warning_threshold: float = Field(default=0.8)  # 80% of target
    error_rate_warning_threshold: float = Field(default=0.01)  # 1%
    error_rate_critical_threshold: float = Field(default=0.05)  # 5%

    # Auto-scaling
    enable_auto_scaling: bool = Field(default=True)
    scale_up_threshold: float = Field(default=0.8)
    scale_down_threshold: float = Field(default=0.3)
    min_instances: int = Field(default=1)
    max_instances: int = Field(default=10)
    cooldown_period_seconds: int = Field(default=300)


class StreamingConfig(BaseSettings):
    """Main streaming analytics configuration."""

    # Component configs
    kafka: KafkaConfig = Field(default_factory=KafkaConfig)
    stream_processing: StreamProcessingConfig = Field(default_factory=StreamProcessingConfig)
    websocket: WebSocketConfig = Field(default_factory=WebSocketConfig)
    realtime_ml: RealtimeMLConfig = Field(default_factory=RealtimeMLConfig)
    monitoring: MonitoringConfig = Field(default_factory=MonitoringConfig)

    # Global settings
    environment: str = Field(default="development")
    log_level: str = Field(default="INFO")
    enable_tracing: bool = Field(default=True)
    trace_sampling_rate: float = Field(default=0.1)

    # Topics configuration
    default_topic_partitions: int = Field(default=3)
    default_topic_replication_factor: int = Field(default=1)

    # Common topics
    events_topic: str = Field(default="analytics-events")
    metrics_topic: str = Field(default="streaming-metrics")
    alerts_topic: str = Field(default="streaming-alerts")
    ml_predictions_topic: str = Field(default="ml-predictions")

    class Config:
        env_prefix = "STREAMING_"
        env_nested_delimiter = "__"


def get_streaming_config() -> StreamingConfig:
    """Get streaming configuration instance."""
    return StreamingConfig()

