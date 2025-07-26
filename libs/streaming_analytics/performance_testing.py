"""Load testing and performance benchmarking framework for streaming analytics."""

import asyncio
import json
import statistics
import time
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

import aiofiles
import structlog

from .event_store import EventSchema, EventType

logger = structlog.get_logger(__name__)

# Performance metrics constants
QUANTILES_COUNT = 20
P95_QUANTILE_INDEX = 18  # 95th percentile index for n=20 quantiles
P99_PERCENTILE = 0.99


@dataclass
class PerformanceMetrics:
    """Performance metrics for load testing."""

    latency_ms: list[float] = field(default_factory=list)
    throughput_events_per_sec: float = 0.0
    error_count: int = 0
    success_count: int = 0
    memory_usage_mb: float = 0.0
    cpu_usage_percent: float = 0.0
    queue_depth: int = 0
    connection_count: int = 0
    timestamp: datetime = field(default_factory=datetime.utcnow)

    @property
    def total_events(self) -> int:
        """Total events processed."""
        return self.success_count + self.error_count

    @property
    def success_rate(self) -> float:
        """Success rate as percentage."""
        if self.total_events == 0:
            return 0.0
        return (self.success_count / self.total_events) * 100

    @property
    def avg_latency_ms(self) -> float:
        """Average latency in milliseconds."""
        return statistics.mean(self.latency_ms) if self.latency_ms else 0.0

    @property
    def p95_latency_ms(self) -> float:
        """95th percentile latency in milliseconds."""
        if not self.latency_ms:
            return 0.0
        return statistics.quantiles(self.latency_ms, n=QUANTILES_COUNT)[P95_QUANTILE_INDEX]

    @property
    def p99_latency_ms(self) -> float:
        """99th percentile latency in milliseconds."""
        if not self.latency_ms:
            return 0.0
        sorted_latencies = sorted(self.latency_ms)
        index = int(P99_PERCENTILE * len(sorted_latencies))
        return sorted_latencies[min(index, len(sorted_latencies) - 1)]

    def to_dict(self) -> dict[str, Any]:
        """Convert metrics to dictionary."""
        return {
            "timestamp": self.timestamp.isoformat(),
            "throughput_events_per_sec": self.throughput_events_per_sec,
            "success_count": self.success_count,
            "error_count": self.error_count,
            "total_events": self.total_events,
            "success_rate_percent": self.success_rate,
            "avg_latency_ms": self.avg_latency_ms,
            "p95_latency_ms": self.p95_latency_ms,
            "p99_latency_ms": self.p99_latency_ms,
            "memory_usage_mb": self.memory_usage_mb,
            "cpu_usage_percent": self.cpu_usage_percent,
            "queue_depth": self.queue_depth,
            "connection_count": self.connection_count,
        }


@dataclass
class LoadTestConfig:
    """Configuration for load testing."""

    test_duration_seconds: int = 60
    events_per_second: int = 1000
    concurrent_producers: int = 10
    concurrent_consumers: int = 5
    event_size_bytes: int = 1024
    batch_size: int = 100
    ramp_up_seconds: int = 10
    ramp_down_seconds: int = 10
    metrics_collection_interval_ms: int = 1000
    enable_memory_profiling: bool = True
    enable_cpu_profiling: bool = True
    output_file: str = "load_test_results.json"
    kafka_topic: str = "load_test_events"


class EventGenerator:
    """Generates synthetic events for load testing."""

    def __init__(self, config: LoadTestConfig):
        self.config = config
        self.logger = logger.bind(component="event_generator")
        self._event_templates = self._create_event_templates()

    def _create_event_templates(self) -> list[dict[str, Any]]:
        """Create templates for different event types."""
        return [
            {
                "event_type": EventType.USER_ACTION,
                "event_name": "user_login",
                "source_service": "auth_service",
                "payload": {
                    "user_id": "user_{id}",
                    "session_id": "session_{id}",
                    "ip_address": "192.168.1.{ip}",
                    "user_agent": "TestAgent/1.0",
                    "login_method": "password",
                },
            },
            {
                "event_type": EventType.BUSINESS_EVENT,
                "event_name": "purchase_completed",
                "source_service": "ecommerce_service",
                "payload": {
                    "order_id": "order_{id}",
                    "user_id": "user_{id}",
                    "amount": 99.99,
                    "currency": "USD",
                    "items": [{"id": "item_{id}", "quantity": 1, "price": 99.99}],
                },
            },
            {
                "event_type": EventType.SYSTEM_EVENT,
                "event_name": "api_request",
                "source_service": "api_gateway",
                "payload": {
                    "request_id": "req_{id}",
                    "endpoint": "/api/v1/users",
                    "method": "GET",
                    "status_code": 200,
                    "response_time_ms": 45,
                },
            },
            {
                "event_type": EventType.METRIC,
                "event_name": "metric_collected",
                "source_service": "monitoring_service",
                "payload": {
                    "metric_name": "cpu_usage",
                    "value": 75.5,
                    "host": "server_{id}",
                    "tags": {"environment": "production", "region": "us-east-1"},
                },
            },
        ]

    def generate_event(self, sequence_id: int) -> EventSchema:
        """Generate a synthetic event."""
        template = self._event_templates[sequence_id % len(self._event_templates)]

        # Create dynamic payload with sequence-based values
        payload = self._substitute_template_values(template["payload"], sequence_id)

        # Add padding to reach target size
        payload = self._add_payload_padding(payload, self.config.event_size_bytes)

        # Create event
        event_data = {
            "event_type": template["event_type"],
            "event_name": template["event_name"],
            "source_service": template["source_service"],
            "payload": payload,
            "correlation_id": f"load_test_{sequence_id}",
        }

        from .event_store import get_event_store

        event_store = get_event_store()
        return event_store.validate_event(event_data)

    def _substitute_template_values(
        self, payload: dict[str, Any], sequence_id: int
    ) -> dict[str, Any]:
        """Substitute template placeholders with actual values."""
        result = {}
        for key, value in payload.items():
            if isinstance(value, str) and "{id}" in value:
                result[key] = value.replace("{id}", str(sequence_id))
            elif isinstance(value, str) and "{ip}" in value:
                result[key] = value.replace("{ip}", str((sequence_id % 254) + 1))
            elif isinstance(value, list):
                result[key] = [
                    self._substitute_template_values(item, sequence_id)
                    if isinstance(item, dict)
                    else item
                    for item in value
                ]
            elif isinstance(value, dict):
                result[key] = self._substitute_template_values(value, sequence_id)
            else:
                result[key] = value
        return result

    def _add_payload_padding(
        self, payload: dict[str, Any], target_size_bytes: int
    ) -> dict[str, Any]:
        """Add padding to reach target event size."""
        current_size = len(json.dumps(payload).encode("utf-8"))

        if current_size < target_size_bytes:
            padding_size = target_size_bytes - current_size - 50  # Leave room for key
            if padding_size > 0:
                payload["_padding"] = "x" * padding_size

        return payload


class PerformanceMonitor:
    """Monitors system performance during load testing."""

    def __init__(self, config: LoadTestConfig):
        self.config = config
        self.logger = logger.bind(component="performance_monitor")
        self._metrics_history: list[PerformanceMetrics] = []
        self._is_monitoring = False

    async def start_monitoring(self) -> None:
        """Start performance monitoring."""
        self._is_monitoring = True
        self.logger.info("Performance monitoring started")

        # Start monitoring task
        asyncio.create_task(self._monitor_loop())

    async def stop_monitoring(self) -> None:
        """Stop performance monitoring."""
        self._is_monitoring = False
        self.logger.info("Performance monitoring stopped")

    async def _monitor_loop(self) -> None:
        """Main monitoring loop."""
        while self._is_monitoring:
            try:
                metrics = await self._collect_system_metrics()
                self._metrics_history.append(metrics)

                self.logger.debug(
                    "Performance metrics collected",
                    memory_mb=metrics.memory_usage_mb,
                    cpu_percent=metrics.cpu_usage_percent,
                )

                await asyncio.sleep(self.config.metrics_collection_interval_ms / 1000)

            except Exception as e:
                self.logger.error("Error collecting performance metrics", error=str(e))
                await asyncio.sleep(1)  # Backoff on error

    async def _collect_system_metrics(self) -> PerformanceMetrics:
        """Collect current system metrics."""
        metrics = PerformanceMetrics()

        try:
            if self.config.enable_memory_profiling:
                metrics.memory_usage_mb = await self._get_memory_usage()

            if self.config.enable_cpu_profiling:
                metrics.cpu_usage_percent = await self._get_cpu_usage()

        except Exception as e:
            self.logger.warning("Failed to collect some system metrics", error=str(e))

        return metrics

    async def _get_memory_usage(self) -> float:
        """Get current memory usage in MB."""
        try:
            import psutil

            process = psutil.Process()
            memory_info = process.memory_info()
            return memory_info.rss / (1024 * 1024)  # Convert to MB
        except ImportError:
            self.logger.warning("psutil not available, memory monitoring disabled")
            return 0.0
        except Exception:
            return 0.0

    async def _get_cpu_usage(self) -> float:
        """Get current CPU usage percentage."""
        try:
            import psutil

            return psutil.cpu_percent(interval=0.1)
        except ImportError:
            self.logger.warning("psutil not available, CPU monitoring disabled")
            return 0.0
        except Exception:
            return 0.0

    def get_metrics_summary(self) -> dict[str, Any]:
        """Get summary of collected metrics."""
        if not self._metrics_history:
            return {}

        memory_values = [
            m.memory_usage_mb for m in self._metrics_history if m.memory_usage_mb > 0
        ]
        cpu_values = [
            m.cpu_usage_percent
            for m in self._metrics_history
            if m.cpu_usage_percent > 0
        ]

        return {
            "memory_usage_mb": {
                "avg": statistics.mean(memory_values) if memory_values else 0,
                "max": max(memory_values) if memory_values else 0,
                "min": min(memory_values) if memory_values else 0,
            },
            "cpu_usage_percent": {
                "avg": statistics.mean(cpu_values) if cpu_values else 0,
                "max": max(cpu_values) if cpu_values else 0,
                "min": min(cpu_values) if cpu_values else 0,
            },
            "sample_count": len(self._metrics_history),
        }


class LoadTestRunner:
    """Main load testing runner."""

    def __init__(self, config: LoadTestConfig | None = None):
        self.config = config or LoadTestConfig()
        self.logger = logger.bind(component="load_test_runner")
        self.event_generator = EventGenerator(self.config)
        self.performance_monitor = PerformanceMonitor(self.config)

        # Test state
        self._is_running = False
        self._start_time: datetime | None = None
        self._end_time: datetime | None = None

        # Metrics tracking
        self._producer_metrics: PerformanceMetrics = PerformanceMetrics()
        self._consumer_metrics: PerformanceMetrics = PerformanceMetrics()
        self._event_latencies: dict[str, float] = {}  # event_id -> latency_ms

        # Producer and consumer tasks
        self._producer_tasks: list[asyncio.Task] = []
        self._consumer_tasks: list[asyncio.Task] = []

        # Rate limiting
        self._target_interval = 1.0 / self.config.events_per_second
        self._event_sequence = 0

    async def run_load_test(self) -> dict[str, Any]:
        """Run the complete load test."""
        self.logger.info(
            "Starting load test",
            duration_seconds=self.config.test_duration_seconds,
            events_per_second=self.config.events_per_second,
            concurrent_producers=self.config.concurrent_producers,
            concurrent_consumers=self.config.concurrent_consumers,
        )

        try:
            # Initialize streaming infrastructure
            await self._setup_streaming_infrastructure()

            # Start performance monitoring
            await self.performance_monitor.start_monitoring()

            # Start the test
            self._start_time = datetime.utcnow()
            self._is_running = True

            # Start producers and consumers
            await self._start_producers()
            await self._start_consumers()

            # Run test for specified duration
            await asyncio.sleep(self.config.test_duration_seconds)

            # Stop the test
            await self._stop_test()

            # Generate results
            results = await self._generate_results()

            # Save results
            await self._save_results(results)

            return results

        except Exception as e:
            self.logger.error("Load test failed", error=str(e))
            raise
        finally:
            await self._cleanup()

    async def _setup_streaming_infrastructure(self) -> None:
        """Setup streaming infrastructure for load testing."""
        from .kafka_manager import get_kafka_manager

        self.kafka_manager = await get_kafka_manager()

        # Create test topic
        await self.kafka_manager.create_topic(
            topic_name=self.config.kafka_topic,
            num_partitions=self.config.concurrent_producers * 2,
            replication_factor=1,
        )

        self.logger.info("Streaming infrastructure setup completed")

    async def _start_producers(self) -> None:
        """Start producer tasks."""
        events_per_producer = (
            self.config.events_per_second // self.config.concurrent_producers
        )

        for i in range(self.config.concurrent_producers):
            task = asyncio.create_task(self._producer_worker(i, events_per_producer))
            self._producer_tasks.append(task)

        self.logger.info(
            "Started producers",
            count=self.config.concurrent_producers,
            events_per_producer=events_per_producer,
        )

    async def _start_consumers(self) -> None:
        """Start consumer tasks."""
        for i in range(self.config.concurrent_consumers):
            task = asyncio.create_task(self._consumer_worker(i))
            self._consumer_tasks.append(task)

        self.logger.info("Started consumers", count=self.config.concurrent_consumers)

    async def _producer_worker(self, worker_id: int, events_per_second: int) -> None:
        """Producer worker that generates and sends events."""
        producer = await self.kafka_manager.get_producer(
            f"load_test_producer_{worker_id}"
        )

        target_interval = 1.0 / events_per_second if events_per_second > 0 else 0.001
        next_send_time = time.time()

        try:
            while self._is_running:
                current_time = time.time()

                # Rate limiting
                if current_time < next_send_time:
                    await asyncio.sleep(next_send_time - current_time)

                # Generate and send event
                event = self.event_generator.generate_event(self._event_sequence)
                self._event_sequence += 1

                send_start = time.time()
                success = await producer.send_event(self.config.kafka_topic, event)
                send_duration = (time.time() - send_start) * 1000

                # Track latency for end-to-end measurement
                self._event_latencies[event.event_id] = time.time()

                if success:
                    self._producer_metrics.success_count += 1
                    self._producer_metrics.latency_ms.append(send_duration)
                else:
                    self._producer_metrics.error_count += 1

                next_send_time = current_time + target_interval

        except Exception as e:
            self.logger.error(
                "Producer worker error",
                worker_id=worker_id,
                error=str(e),
            )

    async def _consumer_worker(self, worker_id: int) -> None:
        """Consumer worker that processes events."""
        consumer = await self.kafka_manager.get_consumer(
            group_id=f"load_test_consumer_group_{worker_id}",
            topics=[self.config.kafka_topic],
        )

        try:
            await consumer.consume_messages()

        except Exception as e:
            self.logger.error(
                "Consumer worker error",
                worker_id=worker_id,
                error=str(e),
            )

    async def _stop_test(self) -> None:
        """Stop the load test."""
        self._is_running = False
        self._end_time = datetime.utcnow()

        # Cancel all tasks
        all_tasks = self._producer_tasks + self._consumer_tasks
        for task in all_tasks:
            task.cancel()

        # Wait for tasks to complete
        if all_tasks:
            await asyncio.gather(*all_tasks, return_exceptions=True)

        # Stop monitoring
        await self.performance_monitor.stop_monitoring()

        self.logger.info("Load test stopped")

    async def _generate_results(self) -> dict[str, Any]:
        """Generate comprehensive test results."""
        test_duration = (self._end_time - self._start_time).total_seconds()

        # Calculate throughput
        total_events = self._producer_metrics.success_count
        throughput = total_events / test_duration if test_duration > 0 else 0

        # Performance targets validation
        targets_met = self._validate_performance_targets(throughput)

        results = {
            "test_config": {
                "duration_seconds": self.config.test_duration_seconds,
                "target_events_per_second": self.config.events_per_second,
                "concurrent_producers": self.config.concurrent_producers,
                "concurrent_consumers": self.config.concurrent_consumers,
                "event_size_bytes": self.config.event_size_bytes,
            },
            "test_execution": {
                "actual_duration_seconds": test_duration,
                "start_time": self._start_time.isoformat(),
                "end_time": self._end_time.isoformat(),
            },
            "producer_metrics": {
                "total_events_sent": self._producer_metrics.success_count,
                "failed_events": self._producer_metrics.error_count,
                "success_rate_percent": self._producer_metrics.success_rate,
                "achieved_throughput_eps": throughput,
                "avg_send_latency_ms": self._producer_metrics.avg_latency_ms,
                "p95_send_latency_ms": self._producer_metrics.p95_latency_ms,
                "p99_send_latency_ms": self._producer_metrics.p99_latency_ms,
            },
            "system_metrics": self.performance_monitor.get_metrics_summary(),
            "performance_targets": targets_met,
            "bottlenecks_identified": self._identify_bottlenecks(),
            "recommendations": self._generate_recommendations(targets_met),
        }

        return results

    def _validate_performance_targets(
        self, achieved_throughput: float
    ) -> dict[str, bool]:
        """Validate against performance targets from issue requirements."""
        targets = {
            "throughput_100k_eps": achieved_throughput >= 100000,
            "latency_under_100ms": self._producer_metrics.avg_latency_ms < 100,
            "success_rate_above_99_percent": self._producer_metrics.success_rate
            >= 99.0,
        }

        return targets

    def _identify_bottlenecks(self) -> list[str]:
        """Identify performance bottlenecks."""
        bottlenecks = []

        # Check latency bottlenecks
        if self._producer_metrics.avg_latency_ms > 100:
            bottlenecks.append("High average latency detected in event production")

        if self._producer_metrics.p99_latency_ms > 500:
            bottlenecks.append("High tail latency (P99) detected")

        # Check throughput bottlenecks
        actual_throughput = (
            self._producer_metrics.success_count / self.config.test_duration_seconds
        )
        if actual_throughput < (self.config.events_per_second * 0.8):
            bottlenecks.append("Achieved throughput significantly below target")

        # Check error rates
        if self._producer_metrics.success_rate < 99.0:
            bottlenecks.append("High error rate detected")

        # Check system resource utilization
        system_metrics = self.performance_monitor.get_metrics_summary()
        if system_metrics.get("memory_usage_mb", {}).get("max", 0) > 2048:
            bottlenecks.append("High memory usage detected")

        if system_metrics.get("cpu_usage_percent", {}).get("avg", 0) > 80:
            bottlenecks.append("High CPU utilization detected")

        return bottlenecks

    def _generate_recommendations(self, targets_met: dict[str, bool]) -> list[str]:
        """Generate performance optimization recommendations."""
        recommendations = []

        if not targets_met.get("throughput_100k_eps"):
            recommendations.extend(
                [
                    "Consider increasing Kafka partition count for better parallelism",
                    "Optimize producer batch size and compression settings",
                    "Enable batch processing in ML inference pipeline",
                    "Review network configuration and bandwidth",
                ]
            )

        if not targets_met.get("latency_under_100ms"):
            recommendations.extend(
                [
                    "Implement connection pooling for database operations",
                    "Add model caching to reduce ML inference latency",
                    "Optimize serialization/deserialization performance",
                    "Consider using faster network protocols",
                ]
            )

        if not targets_met.get("success_rate_above_99_percent"):
            recommendations.extend(
                [
                    "Implement circuit breaker pattern for fault tolerance",
                    "Add retry mechanisms with exponential backoff",
                    "Improve error handling and recovery procedures",
                    "Monitor and tune Kafka broker configuration",
                ]
            )

        system_metrics = self.performance_monitor.get_metrics_summary()
        if system_metrics.get("memory_usage_mb", {}).get("max", 0) > 1024:
            recommendations.append(
                "Optimize memory usage and implement proper garbage collection"
            )

        return recommendations

    async def _save_results(self, results: dict[str, Any]) -> None:
        """Save test results to file."""
        try:
            async with aiofiles.open(self.config.output_file, "w") as f:
                await f.write(json.dumps(results, indent=2, default=str))

            self.logger.info("Test results saved", output_file=self.config.output_file)

        except Exception as e:
            self.logger.error("Failed to save test results", error=str(e))

    async def _cleanup(self) -> None:
        """Cleanup resources after test."""
        try:
            if hasattr(self, "kafka_manager"):
                await self.kafka_manager.shutdown()

        except Exception as e:
            self.logger.error("Error during cleanup", error=str(e))


# Convenience functions for running different test scenarios
async def run_throughput_test(
    events_per_second: int = 100000, duration_seconds: int = 60
) -> dict[str, Any]:
    """Run a throughput-focused load test."""
    config = LoadTestConfig(
        test_duration_seconds=duration_seconds,
        events_per_second=events_per_second,
        concurrent_producers=20,
        concurrent_consumers=10,
        batch_size=500,
        event_size_bytes=512,
    )

    runner = LoadTestRunner(config)
    return await runner.run_load_test()


async def run_latency_test(
    events_per_second: int = 1000, duration_seconds: int = 300
) -> dict[str, Any]:
    """Run a latency-focused load test."""
    config = LoadTestConfig(
        test_duration_seconds=duration_seconds,
        events_per_second=events_per_second,
        concurrent_producers=5,
        concurrent_consumers=5,
        batch_size=1,  # Single event processing for latency measurement
        event_size_bytes=256,
        metrics_collection_interval_ms=100,  # Higher frequency for latency tracking
    )

    runner = LoadTestRunner(config)
    return await runner.run_load_test()


async def run_stress_test(duration_seconds: int = 1800) -> dict[str, Any]:
    """Run a stress test with gradually increasing load."""
    # Start with base load and ramp up
    peak_eps = 150000

    config = LoadTestConfig(
        test_duration_seconds=duration_seconds,
        events_per_second=peak_eps,
        concurrent_producers=30,
        concurrent_consumers=15,
        batch_size=1000,
        event_size_bytes=1024,
        ramp_up_seconds=300,  # 5 minute ramp up
        ramp_down_seconds=300,  # 5 minute ramp down
    )

    runner = LoadTestRunner(config)
    return await runner.run_load_test()
