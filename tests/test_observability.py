"""Tests for the observability stack."""

import time
from unittest.mock import Mock, patch

import pytest

from libs.observability import (
    MetricsCollector,
    ObservabilityConfig,
    ObservabilityManager,
    configure_observability,
    get_correlation_id,
    get_logger_with_correlation,
    set_correlation_id,
    trace_function,
)
from libs.observability.config import LoggingConfig, MetricsConfig, TracingConfig
from libs.observability.metrics import BusinessMetrics, SystemMetrics
from libs.observability.middleware import ObservabilityMiddleware
from libs.observability.tracing import TracingContext


class TestObservabilityConfig:
    """Test observability configuration."""

    def test_default_config(self):
        """Test default configuration values."""
        config = ObservabilityConfig()

        assert config.enabled is True
        assert config.environment == "development"
        assert config.service_name == "analytics-backend"
        assert config.service_version == "1.0.0"

        assert config.tracing.enabled is True
        assert config.metrics.enabled is True
        assert config.logging.level == "INFO"

    def test_config_from_environment(self):
        """Test configuration from environment variables."""
        with patch.dict(
            "os.environ",
            {
                "OBSERVABILITY_ENABLED": "false",
                "OBSERVABILITY_SERVICE_NAME": "test-service",
                "OBSERVABILITY_TRACING__SAMPLE_RATE": "0.5",
                "OBSERVABILITY_METRICS__PROMETHEUS_PORT": "9090",
            },
        ):
            config = ObservabilityConfig()

            assert config.enabled is False
            assert config.service_name == "test-service"
            assert config.tracing.sample_rate == 0.5
            assert config.metrics.prometheus_port == 9090


class TestObservabilityManager:
    """Test the main observability manager."""

    @pytest.fixture
    def config(self):
        """Create test configuration."""
        return ObservabilityConfig(
            service_name="test-service",
            tracing=TracingConfig(enabled=True, jaeger_endpoint=None),
            metrics=MetricsConfig(enabled=True, prometheus_port=0),
        )

    def test_initialization(self, config):
        """Test observability manager initialization."""
        manager = ObservabilityManager(config)

        with patch("libs.observability.instrumentation.start_http_server"):
            manager.initialize()

        assert manager._initialized is True
        assert manager.tracing_manager is not None
        assert manager.metrics_collector is not None

    def test_disabled_observability(self):
        """Test when observability is disabled."""
        config = ObservabilityConfig(enabled=False)
        manager = ObservabilityManager(config)

        manager.initialize()

        assert manager._initialized is False
        assert manager.tracing_manager is None
        assert manager.metrics_collector is None

    def test_shutdown(self, config):
        """Test observability shutdown."""
        manager = ObservabilityManager(config)

        with patch("libs.observability.instrumentation.start_http_server"):
            manager.initialize()

        # Mock the shutdown methods
        manager.tracer_provider = Mock()
        manager.metrics_collector = Mock()

        manager.shutdown()

        manager.tracer_provider.shutdown.assert_called_once()
        manager.metrics_collector.shutdown.assert_called_once()


class TestMetricsCollector:
    """Test metrics collection functionality."""

    @pytest.fixture
    def metrics_config(self):
        """Create test metrics configuration."""
        return MetricsConfig(enabled=True, prometheus_port=0)

    @pytest.fixture
    def collector(self, metrics_config):
        """Create metrics collector instance."""
        # Use separate registry for each test to avoid metric collisions
        from prometheus_client import CollectorRegistry

        registry = CollectorRegistry()
        return MetricsCollector(metrics_config, registry=registry)

    def test_http_request_metrics(self, collector):
        """Test HTTP request metrics recording."""
        collector.record_http_request(
            method="GET", endpoint="/test", status_code=200, duration=0.1
        )

        # Verify metrics were recorded by checking the metric samples
        metric_samples = list(collector.system.http_requests_total.collect())
        assert len(metric_samples) > 0
        assert any(
            sample.value > 0 for metric in metric_samples for sample in metric.samples
        )

    def test_database_metrics(self, collector):
        """Test database metrics recording."""
        collector.record_db_query(operation="SELECT", table="users", duration=0.05)

        metric_samples = list(collector.system.db_queries_total.collect())
        assert len(metric_samples) > 0
        assert any(
            sample.value > 0 for metric in metric_samples for sample in metric.samples
        )

    def test_business_metrics(self, collector):
        """Test business metrics recording."""
        collector.record_user_login("success")
        collector.record_data_processing("test_dataset", 100, "success")
        collector.update_data_quality_score("test_dataset", 95.5)

        login_samples = list(collector.business.user_logins_total.collect())
        assert len(login_samples) > 0
        assert any(
            sample.value > 0 for metric in login_samples for sample in metric.samples
        )

        processing_samples = list(collector.business.data_records_processed.collect())
        assert len(processing_samples) > 0
        assert any(
            sample.value >= 100
            for metric in processing_samples
            for sample in metric.samples
        )

    def test_metrics_summary(self, collector):
        """Test metrics summary generation."""
        summary = collector.get_metrics_summary()

        assert "uptime_seconds" in summary
        assert "timestamp" in summary
        assert summary["collector_status"] == "active"


class TestTracingFunctionality:
    """Test distributed tracing functionality."""

    def test_trace_function_decorator(self):
        """Test the trace_function decorator."""

        @trace_function(name="test_function", attributes={"test": "value"})
        def test_sync_function(x, y):
            return x + y

        with patch("libs.observability.tracing.get_tracer") as mock_tracer:
            mock_span = Mock()
            mock_tracer.return_value.start_as_current_span.return_value.__enter__.return_value = mock_span
            mock_tracer.return_value.start_as_current_span.return_value.__exit__.return_value = None

            result = test_sync_function(1, 2)

            assert result == 3
            mock_span.set_attributes.assert_called()
            mock_span.set_status.assert_called()

    @pytest.mark.asyncio
    async def test_trace_async_function(self):
        """Test tracing async functions."""

        @trace_function(name="test_async_function")
        async def test_async_function(value):
            return value * 2

        with patch("libs.observability.tracing.get_tracer") as mock_tracer:
            mock_span = Mock()
            mock_tracer.return_value.start_as_current_span.return_value.__enter__.return_value = mock_span
            mock_tracer.return_value.start_as_current_span.return_value.__exit__.return_value = None

            result = await test_async_function(5)

            assert result == 10
            mock_span.set_attributes.assert_called()

    def test_tracing_context_manager(self):
        """Test the TracingContext context manager."""
        with patch("libs.observability.tracing.get_tracer") as mock_tracer:
            mock_span = Mock()
            mock_tracer.return_value.start_span.return_value = mock_span

            with TracingContext("test_operation", attributes={"key": "value"}):
                pass

            mock_span.set_attributes.assert_called_with({"key": "value"})
            mock_span.end.assert_called_once()


class TestStructuredLogging:
    """Test structured logging with correlation."""

    def test_correlation_id_context(self):
        """Test correlation ID context management."""
        correlation_id = "test-correlation-123"

        set_correlation_id(correlation_id)
        assert get_correlation_id() == correlation_id

    def test_logger_with_correlation(self):
        """Test logger with correlation context."""
        correlation_id = "test-correlation-456"
        set_correlation_id(correlation_id)

        logger = get_logger_with_correlation("test.module", component="test")

        # Verify logger is bound with context
        assert logger is not None
        # In a real test, we'd check the log output contains correlation_id


class TestObservabilityMiddleware:
    """Test observability middleware functionality."""

    @pytest.fixture
    def mock_app(self):
        """Create mock FastAPI app."""
        return Mock()

    @pytest.fixture
    def middleware(self, mock_app):
        """Create observability middleware instance."""
        return ObservabilityMiddleware(mock_app, service_name="test-service")

    @pytest.fixture
    def mock_request(self):
        """Create mock request."""
        request = Mock()
        request.method = "GET"
        request.url.path = "/test"
        request.url.scheme = "http"
        request.url.hostname = "localhost"
        request.headers = {"user-agent": "test-agent"}
        request.state = Mock()
        # Add scope attribute as a dictionary to prevent "argument of type 'Mock' is not iterable" error
        request.scope = {"type": "http", "path": "/test"}
        return request

    @pytest.fixture
    def mock_response(self):
        """Create mock response."""
        response = Mock()
        response.status_code = 200
        response.headers = {}
        return response

    @pytest.mark.asyncio
    async def test_successful_request_processing(
        self, middleware, mock_request, mock_response
    ):
        """Test successful request processing through middleware."""

        async def mock_call_next(request):
            return mock_response

        with patch(
            "libs.observability.middleware.get_observability_manager"
        ) as mock_obs:
            mock_obs.return_value = Mock()
            mock_obs.return_value.metrics_collector = Mock()

            result = await middleware.dispatch(mock_request, mock_call_next)

            assert result == mock_response
            assert hasattr(mock_request.state, "correlation_id")
            assert "X-Correlation-ID" in result.headers

            # Verify metrics were recorded
            mock_obs.return_value.metrics_collector.record_http_request.assert_called()

    @pytest.mark.asyncio
    async def test_error_handling(self, middleware, mock_request):
        """Test error handling in middleware."""

        async def mock_call_next(request):
            raise ValueError("Test error")

        with patch(
            "libs.observability.middleware.get_observability_manager"
        ) as mock_obs:
            mock_obs.return_value = Mock()
            mock_obs.return_value.metrics_collector = Mock()

            with pytest.raises(ValueError, match="Test error"):
                await middleware.dispatch(mock_request, mock_call_next)

            # Verify error metrics were recorded
            mock_obs.return_value.metrics_collector.record_error.assert_called()


class TestSystemMetrics:
    """Test system-level metrics."""

    def test_system_metrics_initialization(self):
        """Test system metrics are properly initialized."""
        from prometheus_client import CollectorRegistry

        registry = CollectorRegistry()
        metrics = SystemMetrics(prefix="test", registry=registry)

        assert metrics.http_requests_total is not None
        assert metrics.http_request_duration is not None
        assert metrics.db_connections_active is not None
        assert metrics.app_errors_total is not None
        assert metrics.cache_hits_total is not None


class TestBusinessMetrics:
    """Test business-level metrics."""

    def test_business_metrics_initialization(self):
        """Test business metrics are properly initialized."""
        from prometheus_client import CollectorRegistry

        registry = CollectorRegistry()
        metrics = BusinessMetrics(prefix="test", registry=registry)

        assert metrics.user_logins_total is not None
        assert metrics.data_records_processed is not None
        assert metrics.data_quality_score is not None
        assert metrics.api_calls_total is not None
        assert metrics.reports_generated is not None


@pytest.mark.integration
class TestObservabilityIntegration:
    """Integration tests for the complete observability stack."""

    @pytest.fixture
    def full_config(self):
        """Create full observability configuration for integration testing."""
        return ObservabilityConfig(
            service_name="integration-test",
            tracing=TracingConfig(enabled=True, jaeger_endpoint=None, sample_rate=1.0),
            metrics=MetricsConfig(enabled=True, prometheus_port=0),
            logging=LoggingConfig(level="DEBUG", format="json"),
        )

    def test_full_stack_initialization(self, full_config):
        """Test complete observability stack initialization."""
        with patch("libs.observability.instrumentation.start_http_server"):
            manager = configure_observability(full_config)

            assert manager is not None
            assert manager._initialized is True
            assert manager.metrics_collector is not None
            assert manager.tracing_manager is not None

    def test_end_to_end_request_flow(self, full_config):
        """Test end-to-end request flow with observability."""
        with patch("libs.observability.instrumentation.start_http_server"):
            manager = configure_observability(full_config)

            # Simulate a traced operation
            @trace_function(name="test_operation")
            def test_operation():
                # Record some metrics
                if manager.metrics_collector:
                    manager.metrics_collector.record_http_request(
                        method="GET", endpoint="/test", status_code=200, duration=0.1
                    )
                    manager.metrics_collector.record_user_login("success")

                return "success"

            result = test_operation()
            assert result == "success"

            # Verify metrics were collected
            summary = manager.metrics_collector.get_metrics_summary()
            assert summary["collector_status"] == "active"

    def test_performance_impact(self, full_config):
        """Test performance impact of observability stack."""
        with patch("libs.observability.instrumentation.start_http_server"):
            manager = configure_observability(full_config)

            # Measure performance with observability
            @trace_function(name="performance_test")
            def performance_test():
                time.sleep(0.001)  # Simulate some work
                if manager.metrics_collector:
                    manager.metrics_collector.record_http_request(
                        method="GET", endpoint="/perf", status_code=200, duration=0.001
                    )

            start_time = time.time()
            for _ in range(100):
                performance_test()
            end_time = time.time()

            total_time = end_time - start_time
            assert total_time < 1.0, "Observability overhead should be minimal"
