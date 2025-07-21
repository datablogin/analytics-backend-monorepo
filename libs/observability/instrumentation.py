"""Core OpenTelemetry instrumentation setup and management."""

from typing import Any

import structlog
from opentelemetry import trace
from opentelemetry.exporter.jaeger.thrift import JaegerExporter
from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor
from opentelemetry.instrumentation.redis import RedisInstrumentor
from opentelemetry.instrumentation.requests import RequestsInstrumentor
from opentelemetry.instrumentation.sqlalchemy import SQLAlchemyInstrumentor
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from prometheus_client import start_http_server

from .config import ObservabilityConfig, get_observability_config
from .logging import configure_structured_logging
from .metrics import MetricsCollector, create_metrics_collector
from .tracing import TracingManager

logger = structlog.get_logger(__name__)


class ObservabilityManager:
    """Central manager for all observability components."""

    def __init__(self, config: ObservabilityConfig | None = None):
        self.config = config or get_observability_config()
        self.tracer_provider: TracerProvider | None = None
        self.tracing_manager: TracingManager | None = None
        self.metrics_collector: MetricsCollector | None = None
        self._initialized = False

    def initialize(self) -> None:
        """Initialize all observability components."""
        if self._initialized:
            logger.warning("Observability already initialized")
            return

        if not self.config.enabled:
            logger.info("Observability disabled by configuration")
            return

        try:
            logger.info(
                "Initializing observability stack",
                service_name=self.config.service_name,
                environment=self.config.environment,
            )

            # Configure structured logging first
            self._setup_logging()

            # Initialize tracing
            if self.config.tracing.enabled:
                self._setup_tracing()

            # Initialize metrics
            if self.config.metrics.enabled:
                self._setup_metrics()

            # Auto-instrument libraries
            self._setup_auto_instrumentation()

            self._initialized = True
            logger.info("Observability stack initialized successfully")

        except Exception as e:
            logger.error("Failed to initialize observability", error=str(e))
            raise

    def _setup_logging(self) -> None:
        """Set up structured logging with correlation."""
        configure_structured_logging(self.config.logging)
        logger.debug("Structured logging configured")

    def _setup_tracing(self) -> None:
        """Set up distributed tracing with Jaeger."""
        # Create resource with service information
        resource_attrs = {
            "service.name": self.config.service_name,
            "service.version": self.config.service_version,
            "deployment.environment": self.config.environment,
            **self.config.resource_attributes,
        }

        resource = Resource.create(resource_attrs)

        # Create tracer provider
        self.tracer_provider = TracerProvider(resource=resource)
        trace.set_tracer_provider(self.tracer_provider)

        # Configure Jaeger exporter
        if self.config.tracing.jaeger_endpoint:
            jaeger_exporter = JaegerExporter(
                endpoint=self.config.tracing.jaeger_endpoint,
            )

            span_processor = BatchSpanProcessor(
                jaeger_exporter,
                max_export_batch_size=self.config.tracing.max_export_batch_size,
                export_timeout_millis=self.config.tracing.export_timeout_millis,
            )

            self.tracer_provider.add_span_processor(span_processor)

        # Initialize tracing manager
        self.tracing_manager = TracingManager(self.config.tracing)

        logger.debug(
            "Distributed tracing configured",
            jaeger_endpoint=self.config.tracing.jaeger_endpoint,
            sample_rate=self.config.tracing.sample_rate,
        )

    def _setup_metrics(self) -> None:
        """Set up metrics collection with Prometheus."""
        # Create metrics collector
        self.metrics_collector = create_metrics_collector(self.config.metrics)

        # Start Prometheus HTTP server
        if self.config.metrics.prometheus_port:
            try:
                start_http_server(self.config.metrics.prometheus_port)
                logger.info(
                    "Prometheus metrics server started",
                    port=self.config.metrics.prometheus_port,
                    endpoint=self.config.metrics.prometheus_endpoint,
                )
            except OSError as e:
                if "Address already in use" in str(e):
                    logger.warning(
                        "Prometheus port already in use, continuing without dedicated server",
                        port=self.config.metrics.prometheus_port,
                        error=str(e),
                    )
                else:
                    logger.error(
                        "Critical: Failed to start Prometheus server",
                        port=self.config.metrics.prometheus_port,
                        error=str(e),
                    )
                    # Raise for critical errors that aren't port conflicts
                    raise RuntimeError(
                        f"Unable to start Prometheus server on port {self.config.metrics.prometheus_port}: {e}"
                    ) from e

        logger.debug("Metrics collection configured")

    def _setup_auto_instrumentation(self) -> None:
        """Set up automatic instrumentation for common libraries."""
        try:
            # Instrument requests library
            RequestsInstrumentor().instrument()

            # Instrument Redis
            RedisInstrumentor().instrument()

            # Instrument SQLAlchemy
            SQLAlchemyInstrumentor().instrument()

            logger.debug(
                "Auto-instrumentation configured for requests, Redis, SQLAlchemy"
            )

        except Exception as e:
            logger.warning("Some auto-instrumentation failed", error=str(e))

    def instrument_fastapi(self, app: Any) -> None:
        """Instrument a FastAPI application."""
        if not self._initialized:
            logger.warning(
                "Observability not initialized, skipping FastAPI instrumentation"
            )
            return

        if not self.config.tracing.enabled:
            return

        try:
            FastAPIInstrumentor.instrument_app(app)
            logger.info("FastAPI instrumentation enabled")
        except Exception as e:
            logger.error("Failed to instrument FastAPI", error=str(e))

    def shutdown(self) -> None:
        """Shutdown all observability components."""
        try:
            if self.tracer_provider:
                self.tracer_provider.shutdown()

            if self.metrics_collector:
                self.metrics_collector.shutdown()

            logger.info("Observability stack shutdown completed")

        except Exception as e:
            logger.error("Error during observability shutdown", error=str(e))

    def get_tracer(self, name: str) -> trace.Tracer:
        """Get a tracer instance."""
        if not self.tracer_provider:
            return trace.NoOpTracer()
        return trace.get_tracer(name)

    def get_metrics_collector(self) -> MetricsCollector | None:
        """Get the metrics collector instance."""
        return self.metrics_collector


# Global observability manager instance
_observability_manager: ObservabilityManager | None = None


def configure_observability(
    config: ObservabilityConfig | None = None, app: Any = None
) -> ObservabilityManager:
    """Configure and initialize the observability stack."""
    global _observability_manager

    if _observability_manager is None:
        _observability_manager = ObservabilityManager(config)

    _observability_manager.initialize()

    # Instrument FastAPI app if provided
    if app is not None:
        _observability_manager.instrument_fastapi(app)

    return _observability_manager


def get_observability_manager() -> ObservabilityManager | None:
    """Get the global observability manager instance."""
    return _observability_manager
