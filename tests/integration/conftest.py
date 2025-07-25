"""Integration test configuration and fixtures."""

import asyncio
import os
import time

import pytest
import pytest_asyncio
from testcontainers.kafka import KafkaContainer
from testcontainers.postgres import PostgresContainer
from testcontainers.redis import RedisContainer

from libs.streaming_analytics import StreamingConfig


@pytest_asyncio.fixture(scope="session")
async def kafka_container():
    """Start Kafka container for integration tests."""
    # Check if running in CI or local environment
    if os.getenv("GITHUB_ACTIONS") or os.getenv("CI"):
        # In CI, use localhost Kafka from docker-compose.test.yml
        yield {"bootstrap_servers": ["localhost:9092"]}
    else:
        # Local development - use testcontainers
        try:
            with KafkaContainer("confluentinc/cp-kafka:7.4.0") as kafka:
                kafka_config = {"bootstrap_servers": [kafka.get_bootstrap_server()]}
                # Wait for Kafka to be ready
                await asyncio.sleep(5)
                yield kafka_config
        except Exception:
            # Fallback to localhost if testcontainers fail
            yield {"bootstrap_servers": ["localhost:9092"]}


@pytest_asyncio.fixture(scope="session")
async def redis_container():
    """Start Redis container for integration tests."""
    if os.getenv("GITHUB_ACTIONS") or os.getenv("CI"):
        yield {"url": "redis://localhost:6379"}
    else:
        try:
            with RedisContainer("redis:7-alpine") as redis:
                redis_config = {
                    "url": f"redis://localhost:{redis.get_exposed_port(6379)}"
                }
                yield redis_config
        except Exception:
            yield {"url": "redis://localhost:6379"}


@pytest_asyncio.fixture(scope="session")
async def postgres_container():
    """Start PostgreSQL container for integration tests."""
    if os.getenv("GITHUB_ACTIONS") or os.getenv("CI"):
        yield {"url": "postgresql://postgres:password@localhost:5433/analytics_test"}
    else:
        try:
            with PostgresContainer("postgres:15") as postgres:
                postgres_config = {"url": postgres.get_connection_url()}
                yield postgres_config
        except Exception:
            yield {
                "url": "postgresql://postgres:password@localhost:5433/analytics_test"
            }


@pytest_asyncio.fixture
async def integration_config(kafka_container, redis_container, postgres_container):
    """Create integration test configuration."""
    config = StreamingConfig()

    # Update with container configurations
    config.kafka.bootstrap_servers = kafka_container["bootstrap_servers"]

    # Test-specific overrides
    config.kafka.consumer_config.update(
        {
            "auto_offset_reset": "earliest",
            "session_timeout_ms": 10000,
            "heartbeat_interval_ms": 3000,
        }
    )

    config.stream_processing.retry_attempts = 2
    config.stream_processing.processing_timeout_ms = 5000

    config.websocket.port = 8766  # Different port for testing
    config.websocket.require_auth = False

    config.realtime_ml.max_latency_ms = 200  # More lenient for testing
    config.realtime_ml.model_cache_size = 3

    config.monitoring.metrics_interval_seconds = 1  # Faster for testing

    return config


@pytest_asyncio.fixture
async def event_generator():
    """Generate test events for integration tests."""
    from libs.streaming_analytics import EventStore, EventType

    store = EventStore()

    def generate_events(count: int, event_type: EventType = EventType.USER_ACTION):
        events = []
        for i in range(count):
            event = store.create_event(
                event_type=event_type,
                event_name=f"test_event_{i}",
                payload={
                    "test_id": i,
                    "timestamp": time.time(),
                    "data": f"test_data_{i}",
                    "batch_id": i // 10,
                },
                source_service="integration_test",
            )
            events.append(event)
        return events

    return generate_events


@pytest_asyncio.fixture
async def performance_monitor():
    """Monitor performance metrics during tests."""
    import psutil

    class PerformanceMonitor:
        def __init__(self):
            self.process = psutil.Process()
            self.start_time = None
            self.start_memory = None
            self.metrics = []

        def start(self):
            self.start_time = time.time()
            self.start_memory = self.process.memory_info().rss / 1024 / 1024  # MB

        def record_metric(self, name: str, value: float):
            current_time = time.time()
            current_memory = self.process.memory_info().rss / 1024 / 1024  # MB

            self.metrics.append(
                {
                    "name": name,
                    "value": value,
                    "timestamp": current_time,
                    "elapsed_time": current_time - self.start_time
                    if self.start_time
                    else 0,
                    "memory_mb": current_memory,
                    "memory_delta_mb": current_memory - self.start_memory
                    if self.start_memory
                    else 0,
                }
            )

        def get_summary(self):
            if not self.metrics:
                return {}

            total_time = time.time() - self.start_time if self.start_time else 0
            final_memory = self.process.memory_info().rss / 1024 / 1024  # MB
            memory_increase = (
                final_memory - self.start_memory if self.start_memory else 0
            )

            return {
                "total_duration_seconds": total_time,
                "total_metrics_recorded": len(self.metrics),
                "memory_increase_mb": memory_increase,
                "final_memory_mb": final_memory,
                "metrics": self.metrics[-5:],  # Last 5 metrics
            }

    monitor = PerformanceMonitor()
    yield monitor


@pytest.fixture(scope="session")
def event_loop():
    """Create event loop for session-scoped async fixtures."""
    loop = asyncio.new_event_loop()
    yield loop
    loop.close()


# Pytest configuration
def pytest_configure(config):
    """Configure pytest for integration tests."""
    config.addinivalue_line("markers", "integration: mark test as integration test")
    config.addinivalue_line("markers", "performance: mark test as performance test")
    config.addinivalue_line("markers", "slow: mark test as slow running")


def pytest_collection_modifyitems(config, items):
    """Modify test items to add markers."""
    for item in items:
        # Add integration marker to all tests in integration directory
        if "integration" in str(item.fspath):
            item.add_marker(pytest.mark.integration)

        # Add performance marker to performance tests
        if "performance" in item.name.lower() or "throughput" in item.name.lower():
            item.add_marker(pytest.mark.performance)

        # Add slow marker to tests that might take longer
        if any(
            keyword in item.name.lower() for keyword in ["e2e", "scaling", "failover"]
        ):
            item.add_marker(pytest.mark.slow)


# Skip integration tests if infrastructure is not available
def pytest_runtest_setup(item):
    """Setup function to skip tests if required services are not available."""
    if "integration" in [marker.name for marker in item.iter_markers()]:
        # Integration tests only run when explicitly requested
        if not os.getenv("RUN_INTEGRATION_TESTS"):
            pytest.skip(
                "Integration tests require RUN_INTEGRATION_TESTS=1 environment variable"
            )


# Async test timeout
pytest_asyncio.asyncio_fixture = pytest_asyncio.fixture
