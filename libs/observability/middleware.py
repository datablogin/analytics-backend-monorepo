"""FastAPI middleware for observability integration."""

import time
import uuid
from typing import Any

import structlog
from fastapi import Request, Response
from starlette.middleware.base import BaseHTTPMiddleware

from .instrumentation import get_observability_manager
from .logging import set_correlation_id
from .tracing import add_span_attributes, get_current_span

logger = structlog.get_logger(__name__)


class ObservabilityMiddleware(BaseHTTPMiddleware):
    """Comprehensive observability middleware combining metrics, tracing, and logging."""

    def __init__(self, app: Any, service_name: str = "analytics-api"):
        super().__init__(app)
        self.service_name = service_name

    async def dispatch(self, request: Request, call_next: Any) -> Response:
        # Generate correlation ID
        correlation_id = str(uuid.uuid4())
        set_correlation_id(correlation_id)

        # Add correlation ID to request state
        request.state.correlation_id = correlation_id

        # Start timing
        start_time = time.time()

        # Add request attributes to current span
        span = get_current_span()
        if span and span.is_recording():
            add_span_attributes(
                **{
                    "http.method": request.method,
                    "http.url": str(request.url),
                    "http.scheme": request.url.scheme,
                    "http.host": request.url.hostname,
                    "http.user_agent": request.headers.get("user-agent", ""),
                    "correlation_id": correlation_id,
                }
            )

        # Get observability manager
        obs_manager = get_observability_manager()

        try:
            # Process request
            response = await call_next(request)

            # Calculate duration
            duration = time.time() - start_time

            # Record metrics
            if obs_manager and obs_manager.metrics_collector:
                obs_manager.metrics_collector.record_http_request(
                    method=request.method,
                    endpoint=self._get_route_pattern(request),
                    status_code=response.status_code,
                    duration=duration,
                )

            # Add response attributes to span
            if span and span.is_recording():
                add_span_attributes(
                    **{
                        "http.status_code": response.status_code,
                        "http.response_size": len(response.body)
                        if hasattr(response, "body")
                        else 0,
                        "request_duration_ms": duration * 1000,
                    }
                )

            # Add correlation ID to response headers
            response.headers["X-Correlation-ID"] = correlation_id
            response.headers["X-Request-Duration-Ms"] = str(round(duration * 1000, 2))

            # Log request completion
            logger.info(
                "Request completed",
                method=request.method,
                path=request.url.path,
                status_code=response.status_code,
                duration_ms=round(duration * 1000, 2),
                correlation_id=correlation_id,
            )

            return response

        except Exception as e:
            # Calculate duration for failed requests
            duration = time.time() - start_time

            # Record error metrics
            if obs_manager and obs_manager.metrics_collector:
                obs_manager.metrics_collector.record_error(
                    error_type=type(e).__name__, component="api"
                )

                obs_manager.metrics_collector.record_http_request(
                    method=request.method,
                    endpoint=self._get_route_pattern(request),
                    status_code=500,
                    duration=duration,
                )

            # Add error attributes to span
            if span and span.is_recording():
                add_span_attributes(
                    **{
                        "error": True,
                        "error.type": type(e).__name__,
                        "error.message": str(e),
                        "http.status_code": 500,
                        "request_duration_ms": duration * 1000,
                    }
                )

            # Log error
            logger.error(
                "Request failed",
                method=request.method,
                path=request.url.path,
                error=str(e),
                error_type=type(e).__name__,
                duration_ms=round(duration * 1000, 2),
                correlation_id=correlation_id,
                exc_info=True,
            )

            raise

    def _get_route_pattern(self, request: Request) -> str:
        """Extract the route pattern from the request."""
        if hasattr(request, "scope") and "route" in request.scope:
            route = request.scope["route"]
            if hasattr(route, "path"):
                return route.path

        # Fallback to path
        return request.url.path


class TracingMiddleware(BaseHTTPMiddleware):
    """Dedicated tracing middleware."""

    async def dispatch(self, request: Request, call_next: Any) -> Response:
        # Tracing is handled by OpenTelemetry FastAPI instrumentation
        # This middleware is for any custom tracing logic
        response = await call_next(request)
        return response


class MetricsMiddleware(BaseHTTPMiddleware):
    """Dedicated metrics collection middleware."""

    def __init__(self, app: Any):
        super().__init__(app)
        self._request_count = 0

    async def dispatch(self, request: Request, call_next: Any) -> Response:
        start_time = time.time()
        self._request_count += 1

        obs_manager = get_observability_manager()

        try:
            response = await call_next(request)

            # Record successful request metrics
            if obs_manager and obs_manager.metrics_collector:
                duration = time.time() - start_time

                # Update various metrics
                obs_manager.metrics_collector.record_http_request(
                    method=request.method,
                    endpoint=request.url.path,
                    status_code=response.status_code,
                    duration=duration,
                )

                # Update uptime periodically
                if self._request_count % 100 == 0:
                    obs_manager.metrics_collector.update_uptime()

            return response

        except Exception as e:
            # Record error metrics
            if obs_manager and obs_manager.metrics_collector:
                obs_manager.metrics_collector.record_error(
                    error_type=type(e).__name__, component="middleware"
                )

            raise


class BusinessMetricsMiddleware(BaseHTTPMiddleware):
    """Business-specific metrics collection middleware."""

    async def dispatch(self, request: Request, call_next: Any) -> Response:
        obs_manager = get_observability_manager()

        response = await call_next(request)

        if obs_manager and obs_manager.metrics_collector:
            # Record business metrics based on endpoint patterns
            endpoint = request.url.path

            # Track API usage
            user_type = self._determine_user_type(request)
            status = "success" if response.status_code < 400 else "error"

            obs_manager.metrics_collector.record_api_call(
                endpoint=endpoint, user_type=user_type, status=status
            )

            # Track specific business operations
            if endpoint.startswith("/v1/data-quality"):
                obs_manager.metrics_collector.record_data_processing(
                    dataset="api_request", records_count=1, status=status
                )

            elif endpoint.startswith("/v1/auth"):
                if response.status_code == 200:
                    obs_manager.metrics_collector.record_user_login("success")
                else:
                    obs_manager.metrics_collector.record_user_login("failure")

        return response

    def _determine_user_type(self, request: Request) -> str:
        """Determine user type from request context."""
        # This would integrate with your authentication system
        if hasattr(request.state, "user"):
            user = request.state.user
            if hasattr(user, "role"):
                return user.role

        # Check for API key authentication
        if "X-API-Key" in request.headers:
            return "api_user"

        # Check for JWT token
        auth_header = request.headers.get("Authorization", "")
        if auth_header.startswith("Bearer "):
            return "authenticated_user"

        return "anonymous"
