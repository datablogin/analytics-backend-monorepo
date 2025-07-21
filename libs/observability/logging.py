"""Enhanced structured logging with correlation and tracing integration."""

import contextvars
import logging
import sys
from typing import Any

import structlog
from opentelemetry import trace

from .config import LoggingConfig

# Module-level ContextVar for correlation IDs
correlation_id_var: contextvars.ContextVar[str | None] = contextvars.ContextVar(
    "correlation_id", default=None
)


def configure_structured_logging(config: LoggingConfig) -> None:
    """Configure structured logging with OpenTelemetry integration."""

    # Configure standard library logging
    logging.basicConfig(
        format="%(message)s",
        stream=sys.stdout,
        level=getattr(logging, config.level.upper()),
    )

    # Build processors list
    processors = []

    # Add configured processors
    for processor_name in config.processors:
        if hasattr(structlog.processors, processor_name.split(".")[-1]):
            processor = getattr(structlog.processors, processor_name.split(".")[-1])
            if callable(processor):
                processors.append(processor())
            else:
                processors.append(processor)

    # Add tracing integration if enabled
    if config.enable_tracing_integration:
        processors.append(add_trace_context)

    # Add correlation ID if enabled
    if config.enable_correlation:
        processors.append(add_correlation_context)

    # Add final processor based on format
    if config.format == "json":
        processors.append(structlog.processors.JSONRenderer())
    else:
        processors.append(structlog.dev.ConsoleRenderer(colors=True))

    # Configure structlog
    structlog.configure(
        processors=processors,
        wrapper_class=structlog.make_filtering_bound_logger(
            getattr(logging, config.level.upper())
        ),
        logger_factory=structlog.WriteLoggerFactory(),
        cache_logger_on_first_use=True,
    )


def add_trace_context(
    logger: Any, method_name: str, event_dict: dict[str, Any]
) -> dict[str, Any]:
    """Add OpenTelemetry trace context to log entries."""
    span = trace.get_current_span()

    if span and span.is_recording():
        span_context = span.get_span_context()

        event_dict.update(
            {
                "trace_id": format(span_context.trace_id, "032x"),
                "span_id": format(span_context.span_id, "016x"),
                "trace_flags": span_context.trace_flags,
            }
        )

        # Add span name if available
        if hasattr(span, "_name"):
            event_dict["span_name"] = span._name

    return event_dict


def add_correlation_context(
    logger: Any, method_name: str, event_dict: dict[str, Any]
) -> dict[str, Any]:
    """Add correlation context to log entries."""
    # Use module-level correlation_id_var
    if "correlation_id" not in event_dict:
        correlation_id = correlation_id_var.get()
        if correlation_id:
            event_dict["correlation_id"] = correlation_id

    return event_dict


def get_logger_with_correlation(name: str, **kwargs: Any) -> structlog.BoundLogger:
    """Get a logger instance with correlation context."""
    logger = structlog.get_logger(name)

    # Add any additional context
    if kwargs:
        logger = logger.bind(**kwargs)

    return logger


def set_correlation_id(correlation_id: str) -> None:
    """Set the correlation ID for the current context."""
    correlation_id_var.set(correlation_id)


def get_correlation_id() -> str | None:
    """Get the current correlation ID."""
    return correlation_id_var.get()


class CorrelationFilter(logging.Filter):
    """Logging filter to add correlation IDs to all log records."""

    def filter(self, record: logging.LogRecord) -> bool:
        correlation_id = get_correlation_id()
        if correlation_id:
            record.correlation_id = correlation_id
        else:
            record.correlation_id = "unknown"

        return True


def configure_correlation_logging() -> None:
    """Configure correlation ID logging for all loggers."""
    # Add correlation filter to root logger
    correlation_filter = CorrelationFilter()
    logging.getLogger().addFilter(correlation_filter)

    # Also add to structlog if needed
    structlog.configure(
        processors=[
            add_correlation_context,
            structlog.processors.TimeStamper(fmt="ISO"),
            structlog.processors.add_log_level,
            structlog.processors.JSONRenderer(),
        ]
    )


class LoggingContext:
    """Context manager for adding structured context to logs."""

    def __init__(self, **context: Any):
        self.context = context
        self.logger = structlog.get_logger()
        self._bound_logger = None

    def __enter__(self) -> structlog.BoundLogger:
        self._bound_logger = self.logger.bind(**self.context)
        return self._bound_logger

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        if exc_type is not None and self._bound_logger:
            self._bound_logger.error(
                "Exception occurred in logging context",
                exc_info=True,
                exception_type=exc_type.__name__,
                exception_message=str(exc_val),
            )
