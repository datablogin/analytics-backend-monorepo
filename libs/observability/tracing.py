"""Distributed tracing utilities and decorators."""

import functools
import inspect
from collections.abc import Callable
from typing import Any, TypeVar

import structlog
from opentelemetry import trace
from opentelemetry.trace import Status, StatusCode

from .config import TracingConfig

F = TypeVar("F", bound=Callable[..., Any])

logger = structlog.get_logger(__name__)


class TracingManager:
    """Manager for distributed tracing operations."""

    def __init__(self, config: TracingConfig):
        self.config = config
        self.tracer = trace.get_tracer(__name__)

    def create_span(
        self,
        name: str,
        kind: trace.SpanKind = trace.SpanKind.INTERNAL,
        attributes: dict[str, Any] | None = None,
    ) -> trace.Span:
        """Create a new span with optional attributes."""
        span = self.tracer.start_span(name, kind=kind)

        if attributes:
            # Filter attribute values to respect max length
            filtered_attrs = {}
            for key, value in attributes.items():
                str_value = str(value)
                if len(str_value) > self.config.max_tag_value_length:
                    str_value = (
                        str_value[: self.config.max_tag_value_length - 3] + "..."
                    )
                filtered_attrs[key] = str_value

            span.set_attributes(filtered_attrs)

        return span

    def add_span_attributes(self, span: trace.Span, attributes: dict[str, Any]) -> None:
        """Add attributes to an existing span."""
        filtered_attrs = {}
        for key, value in attributes.items():
            str_value = str(value)
            if len(str_value) > self.config.max_tag_value_length:
                str_value = str_value[: self.config.max_tag_value_length - 3] + "..."
            filtered_attrs[key] = str_value

        span.set_attributes(filtered_attrs)

    def record_exception(self, span: trace.Span, exception: Exception) -> None:
        """Record an exception in a span."""
        span.record_exception(exception)
        span.set_status(Status(StatusCode.ERROR, str(exception)))

    def set_span_error(self, span: trace.Span, error_message: str) -> None:
        """Set a span as failed with an error message."""
        span.set_status(Status(StatusCode.ERROR, error_message))


def get_tracer(name: str) -> trace.Tracer:
    """Get a tracer instance for the given name."""
    return trace.get_tracer(name)


def get_current_span() -> trace.Span:
    """Get the currently active span."""
    return trace.get_current_span()


def trace_function(
    name: str | None = None,
    kind: trace.SpanKind = trace.SpanKind.INTERNAL,
    attributes: dict[str, Any] | None = None,
    record_exception: bool = True,
) -> Callable[[F], F]:
    """Decorator to automatically trace function calls.

    Args:
        name: Custom span name. If None, uses the function name.
        kind: The span kind (INTERNAL, CLIENT, SERVER, etc.)
        attributes: Static attributes to add to the span
        record_exception: Whether to record exceptions in the span

    Returns:
        Decorated function
    """

    def decorator(func: F) -> F:
        @functools.wraps(func)
        def sync_wrapper(*args: Any, **kwargs: Any) -> Any:
            span_name = name or f"{func.__module__}.{func.__qualname__}"
            tracer = get_tracer(func.__module__)

            with tracer.start_as_current_span(span_name, kind=kind) as span:
                # Add static attributes
                if attributes:
                    span.set_attributes(attributes)

                # Add function metadata
                span.set_attributes(
                    {
                        "function.name": func.__name__,
                        "function.module": func.__module__,
                    }
                )

                try:
                    result = func(*args, **kwargs)
                    span.set_status(Status(StatusCode.OK))
                    return result
                except Exception as e:
                    if record_exception:
                        span.record_exception(e)
                        span.set_status(Status(StatusCode.ERROR, str(e)))
                    raise

        @functools.wraps(func)
        async def async_wrapper(*args: Any, **kwargs: Any) -> Any:
            span_name = name or f"{func.__module__}.{func.__qualname__}"
            tracer = get_tracer(func.__module__)

            with tracer.start_as_current_span(span_name, kind=kind) as span:
                # Add static attributes
                if attributes:
                    span.set_attributes(attributes)

                # Add function metadata
                span.set_attributes(
                    {
                        "function.name": func.__name__,
                        "function.module": func.__module__,
                    }
                )

                try:
                    result = await func(*args, **kwargs)
                    span.set_status(Status(StatusCode.OK))
                    return result
                except Exception as e:
                    if record_exception:
                        span.record_exception(e)
                        span.set_status(Status(StatusCode.ERROR, str(e)))
                    raise

        # Return the appropriate wrapper based on whether the function is async
        if inspect.iscoroutinefunction(func):
            return async_wrapper  # type: ignore
        else:
            return sync_wrapper  # type: ignore

    return decorator


def add_span_attributes(**attributes: Any) -> None:
    """Add attributes to the current active span."""
    span = get_current_span()
    if span and span.is_recording():
        span.set_attributes(attributes)


def add_span_event(name: str, attributes: dict[str, Any] | None = None) -> None:
    """Add an event to the current active span."""
    span = get_current_span()
    if span and span.is_recording():
        span.add_event(name, attributes)


def set_span_tag(key: str, value: Any) -> None:
    """Set a tag on the current active span."""
    span = get_current_span()
    if span and span.is_recording():
        span.set_attribute(key, value)


class TracingContext:
    """Context manager for creating traced operations."""

    def __init__(
        self,
        name: str,
        tracer: trace.Tracer | None = None,
        kind: trace.SpanKind = trace.SpanKind.INTERNAL,
        attributes: dict[str, Any] | None = None,
    ):
        self.name = name
        self.tracer = tracer or get_tracer(__name__)
        self.kind = kind
        self.attributes = attributes or {}
        self.span: trace.Span | None = None

    def __enter__(self) -> trace.Span:
        self.span = self.tracer.start_span(self.name, kind=self.kind)
        if self.attributes:
            self.span.set_attributes(self.attributes)
        return self.span

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        if self.span:
            if exc_type is not None:
                self.span.record_exception(exc_val)
                self.span.set_status(Status(StatusCode.ERROR, str(exc_val)))
            else:
                self.span.set_status(Status(StatusCode.OK))
            self.span.end()
