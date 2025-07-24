"""Tests for streaming analytics infrastructure."""

import asyncio
from datetime import datetime

import pytest

from libs.streaming_analytics import (
    EventStore,
    EventType,
    StreamingConfig,
    UserActionEvent,
    WindowedProcessor,
    WindowType,
)


class TestEventStore:
    """Test event schema registry and validation."""

    def test_event_store_initialization(self):
        """Test event store initializes with built-in schemas."""
        store = EventStore()

        # Check built-in schemas are registered
        assert "base_event" in store.list_schemas()
        assert "user_action" in store.list_schemas()
        assert "system_event" in store.list_schemas()
        assert "ml_prediction" in store.list_schemas()

    def test_event_creation_and_validation(self):
        """Test creating and validating events."""
        store = EventStore()

        # Create a basic event
        event = store.create_event(
            event_type=EventType.USER_ACTION,
            event_name="page_view",
            payload={"page": "/home", "element": "navbar"},
            source_service="web_app"
        )

        assert event.event_type == EventType.USER_ACTION
        assert event.event_name == "page_view"
        assert event.source_service == "web_app"
        assert "page" in event.payload

    def test_user_action_event(self):
        """Test user action event validation."""
        event_data = {
            "event_type": "user_action",
            "event_name": "button_click",
            "user_id": "user123",
            "session_id": "session456",
            "action": "click",
            "source_service": "web_app",
            "payload": {
                "page": "/dashboard",
                "element": "submit_button"
            }
        }

        store = EventStore()
        event = store.validate_event(event_data, "user_action")

        assert isinstance(event, UserActionEvent)
        assert event.user_id == "user123"
        assert event.action == "click"

    def test_event_serialization(self):
        """Test event serialization and deserialization."""
        store = EventStore()

        original_event = store.create_event(
            event_type=EventType.SYSTEM_EVENT,
            event_name="service_started",
            payload={"service": "api", "port": 8080},
            source_service="analytics_api"
        )

        # Serialize to JSON
        json_data = store.serialize_event(original_event, "json")
        assert isinstance(json_data, str)

        # Deserialize from JSON
        deserialized_event = store.deserialize_event(json_data, "base_event")

        assert deserialized_event.event_type == original_event.event_type
        assert deserialized_event.event_name == original_event.event_name
        assert deserialized_event.payload == original_event.payload


class TestStreamProcessing:
    """Test stream processing with windowing."""

    @pytest.mark.asyncio
    async def test_windowed_processor_initialization(self):
        """Test windowed processor setup."""
        processor = WindowedProcessor()

        # Define a tumbling window
        processor.define_window(
            key_field="source_service",
            window_type=WindowType.TUMBLING,
            size_ms=60000  # 1 minute
        )

        await processor.start()

        # Check processor is running
        stats = processor.get_stats()
        assert stats['is_running'] is True

        await processor.stop()
        assert processor.get_stats()['is_running'] is False

    @pytest.mark.asyncio
    async def test_event_processing_and_aggregation(self):
        """Test processing events through windows."""
        processor = WindowedProcessor()

        # Define window
        processor.define_window(
            key_field="source_service",
            window_type=WindowType.TUMBLING,
            size_ms=1000  # 1 second for testing
        )

        # Track aggregation results
        results = []

        def capture_result(result):
            results.append(result)

        processor.add_aggregation_handler(capture_result)

        await processor.start()

        # Create test events
        store = EventStore()
        events = []

        for i in range(5):
            event = store.create_event(
                event_type=EventType.USER_ACTION,
                event_name=f"action_{i}",
                payload={"action_id": i, "value": i * 10},
                source_service="test_service"
            )
            events.append(event)

        # Process events
        for event in events:
            await processor.process_event(event)

        # Wait a bit for processing
        await asyncio.sleep(0.1)

        await processor.stop()

        # Verify processing stats
        stats = processor.get_stats()
        assert stats['processed_count'] == 5
        assert stats['error_count'] == 0


class TestStreamingConfig:
    """Test streaming configuration."""

    def test_streaming_config_defaults(self):
        """Test default configuration values."""
        config = StreamingConfig()

        # Check Kafka defaults
        assert config.kafka.bootstrap_servers == ["localhost:9092"]
        assert config.kafka.security_protocol == "PLAINTEXT"

        # Check stream processing defaults
        assert config.stream_processing.default_window_size_ms == 60000
        assert config.stream_processing.parallelism == 4

        # Check WebSocket defaults
        assert config.websocket.host == "0.0.0.0"
        assert config.websocket.port == 8765
        assert config.websocket.max_connections == 1000

        # Check ML defaults
        assert config.realtime_ml.model_cache_size == 10
        assert config.realtime_ml.max_latency_ms == 50

        # Check monitoring defaults
        assert config.monitoring.metrics_interval_seconds == 10
        assert config.monitoring.enable_auto_scaling is True

    def test_topic_configuration(self):
        """Test topic naming configuration."""
        config = StreamingConfig()

        assert config.events_topic == "analytics-events"
        assert config.metrics_topic == "streaming-metrics"
        assert config.alerts_topic == "streaming-alerts"
        assert config.ml_predictions_topic == "ml-predictions"


class TestEventTypes:
    """Test different event types and their validation."""

    def test_user_action_event_validation(self):
        """Test user action event specific validation."""
        # Valid user action event
        valid_data = {
            "event_type": "user_action",
            "event_name": "page_view",
            "user_id": "user123",
            "session_id": "sess456",
            "action": "view",
            "source_service": "web_app",
            "payload": {
                "page": "/dashboard",
                "element": "main_content"
            }
        }

        store = EventStore()
        event = store.validate_event(valid_data, "user_action")
        assert isinstance(event, UserActionEvent)

        # Invalid user action event (missing required payload fields)
        invalid_data = valid_data.copy()
        invalid_data["payload"] = {"other_field": "value"}

        with pytest.raises(Exception):  # ValidationError
            store.validate_event(invalid_data, "user_action")

    def test_event_timestamp_handling(self):
        """Test event timestamp validation and conversion."""
        store = EventStore()

        # Test with Unix timestamp
        event_data = {
            "event_type": "system_event",
            "event_name": "test",
            "component": "test_component",
            "source_service": "test",
            "timestamp": 1640995200,  # Unix timestamp
            "payload": {"test": "data"}
        }

        event = store.validate_event(event_data, "system_event")
        assert isinstance(event.timestamp, datetime)

        # Test with ISO string
        event_data["timestamp"] = "2022-01-01T00:00:00Z"
        event = store.validate_event(event_data, "system_event")
        assert isinstance(event.timestamp, datetime)


if __name__ == "__main__":
    pytest.main([__file__])
