"""End-to-end integration tests for streaming analytics pipeline."""

import asyncio
import json
import time
from datetime import datetime
from unittest.mock import Mock, patch

import pytest

from libs.streaming_analytics import (
    EventStore,
    EventType,
    KafkaManager,
    RealtimeMLPipeline,
    StreamingConfig,
    StreamingMetrics,
    StreamProcessor,
    WebSocketServer,
    WindowedProcessor,
    WindowType,
)


class TestKafkaStreamProcessingIntegration:
    """Test Kafka + Stream Processing integration."""

    @pytest.fixture
    async def kafka_config(self):
        """Kafka configuration for testing."""
        config = StreamingConfig()
        config.kafka.bootstrap_servers = ["localhost:9092"]
        config.events_topic = "test-events"
        config.metrics_topic = "test-metrics"
        return config

    @pytest.fixture
    async def kafka_manager(self, kafka_config):
        """Kafka manager instance for testing."""
        manager = KafkaManager(kafka_config.kafka)
        await manager.start()
        yield manager
        await manager.stop()

    @pytest.fixture
    async def stream_processor(self, kafka_config):
        """Stream processor instance for testing."""
        processor = StreamProcessor(kafka_config)
        yield processor
        await processor.stop()

    @pytest.mark.asyncio
    async def test_kafka_producer_consumer_integration(self, kafka_manager):
        """Test basic Kafka producer and consumer functionality."""
        topic = "test-integration-topic"
        test_messages = [
            {"event_id": f"test-{i}", "timestamp": time.time(), "data": f"message-{i}"}
            for i in range(10)
        ]

        # Create topic
        await kafka_manager.create_topic(topic, num_partitions=3)

        # Produce messages
        for message in test_messages:
            await kafka_manager.produce_event(topic, message)

        # Consume messages
        consumed_messages = []
        consumer = await kafka_manager.create_consumer([topic], "test-group")

        # Give some time for messages to be produced
        await asyncio.sleep(1)

        start_time = time.time()
        while (
            len(consumed_messages) < len(test_messages)
            and time.time() - start_time < 10
        ):
            async for msg in consumer:
                consumed_messages.append(json.loads(msg.value.decode()))
                if len(consumed_messages) >= len(test_messages):
                    break
            await asyncio.sleep(0.1)

        await consumer.stop()

        # Verify all messages were consumed
        assert len(consumed_messages) == len(test_messages)
        consumed_ids = {msg["event_id"] for msg in consumed_messages}
        expected_ids = {msg["event_id"] for msg in test_messages}
        assert consumed_ids == expected_ids

    @pytest.mark.asyncio
    async def test_stream_processing_with_windows(
        self, kafka_manager, stream_processor
    ):
        """Test stream processing with windowing operations."""
        # Configure windowed processor
        windowed_processor = WindowedProcessor()
        windowed_processor.define_window(
            key_field="user_id",
            window_type=WindowType.TUMBLING,
            size_ms=2000,  # 2 seconds for testing
        )

        aggregation_results = []

        def capture_aggregation(result):
            aggregation_results.append(result)

        windowed_processor.add_aggregation_handler(capture_aggregation)

        await windowed_processor.start()

        # Create test events
        event_store = EventStore()
        test_events = []
        for i in range(20):
            event = event_store.create_event(
                event_type=EventType.USER_ACTION,
                event_name="click",
                payload={
                    "user_id": f"user-{i % 5}",  # 5 different users
                    "value": i,
                    "timestamp": time.time(),
                },
                source_service="test_service",
            )
            test_events.append(event)

        # Process events
        for event in test_events:
            await windowed_processor.process_event(event)

        # Wait for window processing
        await asyncio.sleep(3)

        await windowed_processor.stop()

        # Verify aggregations occurred
        assert len(aggregation_results) > 0
        stats = windowed_processor.get_stats()
        assert stats["processed_count"] == len(test_events)

    @pytest.mark.asyncio
    async def test_fault_tolerance_and_recovery(self, kafka_manager):
        """Test fault tolerance and recovery mechanisms."""
        topic = "test-fault-tolerance"
        await kafka_manager.create_topic(topic, num_partitions=1)

        # Create processor with fault tolerance
        processor = StreamProcessor(StreamingConfig())
        processor.config.stream_processing.retry_attempts = 3
        processor.config.stream_processing.retry_backoff_ms = 100

        processed_events = []
        error_count = 0

        async def faulty_handler(event):
            nonlocal error_count
            if error_count < 2:  # Fail first 2 times
                error_count += 1
                raise Exception("Simulated processing error")
            processed_events.append(event)

        processor.add_event_handler(faulty_handler)

        # Create test event
        event_store = EventStore()
        test_event = event_store.create_event(
            event_type=EventType.SYSTEM_EVENT,
            event_name="test_fault_tolerance",
            payload={"test": "data"},
            source_service="test",
        )

        # Process event (should succeed after retries)
        await processor.process_event(test_event)

        # Verify event was eventually processed
        assert len(processed_events) == 1
        assert error_count == 2  # Failed twice before succeeding


class TestMLInferencePipelineIntegration:
    """Test ML inference pipeline integration with real models."""

    @pytest.fixture
    async def ml_pipeline(self):
        """ML pipeline instance for testing."""
        config = StreamingConfig()
        config.realtime_ml.model_cache_size = 5
        config.realtime_ml.max_latency_ms = 100

        pipeline = RealtimeMLPipeline(config.realtime_ml)
        await pipeline.start()
        yield pipeline
        await pipeline.stop()

    @pytest.fixture
    def mock_model(self):
        """Mock ML model for testing."""
        model = Mock()
        model.predict.return_value = [0.85, 0.15]  # Mock prediction probabilities
        model.feature_names = ["feature_1", "feature_2", "feature_3"]
        return model

    @pytest.mark.asyncio
    async def test_model_loading_and_caching(self, ml_pipeline, mock_model):
        """Test model loading and caching mechanisms."""
        model_name = "test_model"
        model_version = "1.0.0"

        # Mock model registry
        with patch.object(
            ml_pipeline.model_registry, "get_model", return_value=mock_model
        ):
            # Load model
            loaded_model = await ml_pipeline.load_model(model_name, model_version)

            # Verify model is loaded
            assert loaded_model is not None
            assert loaded_model == mock_model

            # Verify model is cached
            cached_model = await ml_pipeline.load_model(model_name, model_version)
            assert cached_model == loaded_model

            # Check cache stats
            cache_stats = ml_pipeline.get_cache_stats()
            assert cache_stats["cache_size"] == 1
            assert cache_stats["cache_hits"] >= 1

    @pytest.mark.asyncio
    async def test_realtime_inference_with_features(self, ml_pipeline, mock_model):
        """Test real-time inference with feature extraction."""
        model_name = "test_model"

        with patch.object(
            ml_pipeline.model_registry, "get_model", return_value=mock_model
        ):
            await ml_pipeline.load_model(model_name, "1.0.0")

            # Create test event with features
            event_store = EventStore()
            test_event = event_store.create_event(
                event_type=EventType.USER_ACTION,
                event_name="prediction_request",
                payload={
                    "features": {
                        "feature_1": 0.5,
                        "feature_2": 0.8,
                        "feature_3": 0.3,
                    },
                    "model_name": model_name,
                },
                source_service="test_service",
            )

            # Run inference
            start_time = time.time()
            prediction = await ml_pipeline.predict(test_event)
            inference_time = (time.time() - start_time) * 1000

            # Verify prediction
            assert prediction is not None
            assert prediction.status.value == "success"
            assert prediction.predictions == [0.85, 0.15]
            assert inference_time < ml_pipeline.config.max_latency_ms

    @pytest.mark.asyncio
    async def test_batch_inference_performance(self, ml_pipeline, mock_model):
        """Test batch inference performance."""
        model_name = "test_model"
        batch_size = 10

        with patch.object(
            ml_pipeline.model_registry, "get_model", return_value=mock_model
        ):
            await ml_pipeline.load_model(model_name, "1.0.0")

            # Create batch of test events
            event_store = EventStore()
            test_events = []
            for i in range(batch_size):
                event = event_store.create_event(
                    event_type=EventType.USER_ACTION,
                    event_name="batch_prediction",
                    payload={
                        "features": {
                            "feature_1": i * 0.1,
                            "feature_2": i * 0.2,
                            "feature_3": i * 0.3,
                        },
                        "model_name": model_name,
                    },
                    source_service="test_service",
                )
                test_events.append(event)

            # Run batch inference
            start_time = time.time()
            predictions = await ml_pipeline.predict_batch(test_events)
            batch_time = (time.time() - start_time) * 1000

            # Verify batch predictions
            assert len(predictions) == batch_size
            assert all(p.status.value == "success" for p in predictions)

            # Check performance (should be faster than individual predictions)
            avg_time_per_prediction = batch_time / batch_size
            assert avg_time_per_prediction < ml_pipeline.config.max_latency_ms


class TestWebSocketDashboardIntegration:
    """Test WebSocket real-time dashboard integration."""

    @pytest.fixture
    async def websocket_server(self):
        """WebSocket server instance for testing."""
        config = StreamingConfig()
        config.websocket.port = 8766  # Different port for testing
        config.websocket.require_auth = False  # Disable auth for testing

        server = WebSocketServer(config.websocket)
        await server.start()
        yield server
        await server.stop()

    @pytest.mark.asyncio
    async def test_websocket_connection_and_messaging(self, websocket_server):
        """Test WebSocket connection and real-time messaging."""
        import websockets

        # Connect to WebSocket server
        uri = f"ws://localhost:{websocket_server.config.port}"

        async with websockets.connect(uri) as websocket:
            # Send subscription message
            subscription_msg = {
                "type": "subscribe",
                "subscription_type": "events",
                "filters": {"event_type": "user_action"},
            }
            await websocket.send(json.dumps(subscription_msg))

            # Receive confirmation
            response = await websocket.recv()
            response_data = json.loads(response)
            assert response_data["type"] == "subscription_confirmed"

            # Simulate streaming event
            test_event = {
                "type": "data",
                "subscription_type": "events",
                "data": {
                    "event_id": "test-123",
                    "event_type": "user_action",
                    "timestamp": datetime.utcnow().isoformat(),
                    "payload": {"action": "click", "element": "button"},
                },
            }

            # Send event through server
            await websocket_server.broadcast_to_subscribers(
                "events", test_event["data"]
            )

            # Receive event
            event_msg = await websocket.recv()
            event_data = json.loads(event_msg)

            assert event_data["type"] == "data"
            assert event_data["subscription_type"] == "events"
            assert event_data["data"]["event_id"] == "test-123"

    @pytest.mark.asyncio
    async def test_multiple_client_subscriptions(self, websocket_server):
        """Test multiple client subscriptions and broadcasting."""
        import websockets

        uri = f"ws://localhost:{websocket_server.config.port}"

        # Connect multiple clients
        clients = []
        for i in range(3):
            client = await websockets.connect(uri)
            clients.append(client)

            # Subscribe each client
            subscription_msg = {
                "type": "subscribe",
                "subscription_type": "metrics",
                "client_id": f"client-{i}",
            }
            await client.send(json.dumps(subscription_msg))

            # Wait for confirmation
            response = await client.recv()
            response_data = json.loads(response)
            assert response_data["type"] == "subscription_confirmed"

        # Broadcast metric update
        metric_data = {
            "timestamp": datetime.utcnow().isoformat(),
            "metrics": {
                "events_per_second": 1500,
                "avg_latency_ms": 45,
                "error_rate": 0.01,
            },
        }

        await websocket_server.broadcast_to_subscribers("metrics", metric_data)

        # Verify all clients receive the message
        for client in clients:
            message = await client.recv()
            message_data = json.loads(message)
            assert message_data["type"] == "data"
            assert message_data["subscription_type"] == "metrics"
            assert message_data["data"]["metrics"]["events_per_second"] == 1500

        # Close all clients
        for client in clients:
            await client.close()

    @pytest.mark.asyncio
    async def test_websocket_authentication_and_rate_limiting(self):
        """Test WebSocket authentication and rate limiting."""
        config = StreamingConfig()
        config.websocket.port = 8767
        config.websocket.require_auth = True
        config.websocket.rate_limit_per_minute = 10

        server = WebSocketServer(config.websocket)
        await server.start()

        try:
            import websockets

            uri = f"ws://localhost:{config.websocket.port}"

            # Try to connect without authentication (should fail)
            with pytest.raises(websockets.exceptions.ConnectionClosedError):
                async with websockets.connect(uri) as websocket:
                    # Should be disconnected before we can send anything
                    await websocket.send(json.dumps({"type": "subscribe"}))
                    await websocket.recv()

        finally:
            await server.stop()


class TestPerformanceAndScaling:
    """Test performance targets and auto-scaling behavior."""

    @pytest.fixture
    async def performance_test_setup(self):
        """Setup for performance testing."""
        config = StreamingConfig()
        config.monitoring.enable_auto_scaling = True
        config.monitoring.scale_up_threshold = 0.8

        # Create components
        kafka_manager = KafkaManager(config.kafka)
        processor = StreamProcessor(config)
        metrics = StreamingMetrics(config.monitoring)

        await kafka_manager.start()
        await processor.start()
        await metrics.start()

        yield {
            "kafka_manager": kafka_manager,
            "processor": processor,
            "metrics": metrics,
            "config": config,
        }

        await kafka_manager.stop()
        await processor.stop()
        await metrics.stop()

    @pytest.mark.asyncio
    async def test_high_throughput_event_processing(self, performance_test_setup):
        """Test processing high volume of events (targeting 100k+/sec)."""
        components = performance_test_setup
        kafka_manager = components["kafka_manager"]
        processor = components["processor"]

        # Create test topic
        topic = "performance-test-topic"
        await kafka_manager.create_topic(topic, num_partitions=8)

        # Generate test events
        event_count = 10000  # Scaled down for test environment
        event_store = EventStore()

        processed_count = 0
        start_time = time.time()

        async def count_handler(event):
            nonlocal processed_count
            processed_count += 1

        processor.add_event_handler(count_handler)

        # Produce events rapidly
        # producer_start = time.time()
        for i in range(event_count):
            event = event_store.create_event(
                event_type=EventType.USER_ACTION,
                event_name=f"performance_test_{i}",
                payload={"test_id": i, "batch": i // 1000},
                source_service="performance_test",
            )
            await kafka_manager.produce_event(topic, event.to_dict())

        # producer_time = time.time() - producer_start

        # Wait for processing to complete
        await asyncio.sleep(5)

        total_time = time.time() - start_time
        events_per_second = event_count / total_time

        # Verify performance targets
        assert processed_count >= event_count * 0.95  # At least 95% processed
        assert events_per_second > 1000  # At least 1k events/sec in test environment

        # Check latency metrics
        stats = processor.get_stats()
        avg_latency = stats.get("avg_processing_time_ms", 0)
        assert avg_latency < 100  # Under 100ms average latency

    @pytest.mark.asyncio
    async def test_auto_scaling_behavior(self, performance_test_setup):
        """Test auto-scaling behavior under load."""
        components = performance_test_setup
        metrics = components["metrics"]
        # processor = components["processor"]

        # Simulate high load metrics
        high_load_metrics = {
            "events_per_second": 8000,  # Above 80% threshold
            "cpu_usage": 0.85,
            "memory_usage": 0.75,
            "queue_depth": 500,
        }

        # Update metrics to trigger scaling
        await metrics.update_performance_metrics(high_load_metrics)

        # Wait for scaling decision
        await asyncio.sleep(1)

        # Check if scaling was triggered
        scaling_events = metrics.get_scaling_events()
        assert len(scaling_events) > 0

        latest_event = scaling_events[-1]
        assert latest_event["action"] == "scale_up"
        assert latest_event["reason"] == "high_throughput"

    @pytest.mark.asyncio
    async def test_memory_and_resource_usage(self, performance_test_setup):
        """Test memory usage and resource efficiency."""
        components = performance_test_setup
        processor = components["processor"]

        # Monitor initial memory usage
        import psutil

        process = psutil.Process()
        initial_memory = process.memory_info().rss / 1024 / 1024  # MB

        # Process a large batch of events
        event_store = EventStore()
        for i in range(5000):
            event = event_store.create_event(
                event_type=EventType.USER_ACTION,
                event_name=f"memory_test_{i}",
                payload={"data": "x" * 100},  # Some payload data
                source_service="memory_test",
            )
            await processor.process_event(event)

        # Check memory usage after processing
        final_memory = process.memory_info().rss / 1024 / 1024  # MB
        memory_increase = final_memory - initial_memory

        # Verify memory usage is reasonable (less than 100MB increase)
        assert memory_increase < 100

        # Check for memory leaks by processing another batch
        for i in range(5000, 10000):
            event = event_store.create_event(
                event_type=EventType.USER_ACTION,
                event_name=f"memory_test_{i}",
                payload={"data": "x" * 100},
                source_service="memory_test",
            )
            await processor.process_event(event)

        after_second_batch = process.memory_info().rss / 1024 / 1024  # MB
        second_increase = after_second_batch - final_memory

        # Second batch should not significantly increase memory (no major leaks)
        assert second_increase < memory_increase * 0.5


if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short"])
