"""Real-time streaming analytics infrastructure.

This module provides comprehensive streaming analytics capabilities including:
- Apache Kafka integration for event streaming
- Stream processing with windowing and aggregations
- Real-time machine learning inference
- WebSocket API for live dashboard updates
- Event schema registry and validation
- Stream processing monitoring and alerting
- Auto-scaling based on stream volume
- Fault tolerance with exactly-once processing guarantees
"""

from .config import StreamingConfig
from .event_store import EventSchema, EventStore, EventType, UserActionEvent
from .kafka_manager import KafkaManager
from .monitoring import StreamingMetrics
from .processor import AggregationType, StreamProcessor, WindowedProcessor, WindowType
from .realtime_ml import RealtimeMLPipeline
from .websocket_server import WebSocketServer

__all__ = [
    "StreamingConfig",
    "EventStore",
    "EventSchema",
    "EventType",
    "UserActionEvent",
    "KafkaManager",
    "StreamProcessor",
    "WindowedProcessor",
    "WindowType",
    "AggregationType",
    "RealtimeMLPipeline",
    "WebSocketServer",
    "StreamingMetrics",
]

