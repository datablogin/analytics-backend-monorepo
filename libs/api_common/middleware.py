"""Middleware for standardized API responses and error handling."""

import json
import time
from datetime import datetime
from uuid import uuid4

from fastapi import HTTPException, Request, Response, status
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from pydantic import ValidationError
from starlette.middleware.base import BaseHTTPMiddleware

from .response_models import (
    APIMetadata,
    ErrorDetail,
    ErrorResponse,
    ValidationErrorDetail,
    ValidationErrorResponse,
)


def json_encoder(obj):
    """Custom JSON encoder for datetime objects."""
    if isinstance(obj, datetime):
        return obj.isoformat()
    raise TypeError(f"Object of type {obj.__class__.__name__} is not JSON serializable")


class ResponseStandardizationMiddleware(BaseHTTPMiddleware):
    """Middleware to standardize all API responses."""

    def __init__(self, app, environment: str = "development", version: str = "v1"):
        super().__init__(app)
        self.environment = environment
        self.version = version

    async def dispatch(self, request: Request, call_next) -> Response:
        # Generate request ID
        request_id = str(uuid4())
        request.state.request_id = request_id

        # Add request timing
        start_time = time.time()

        try:
            response = await call_next(request)

            # Add standard headers
            response.headers["X-Request-ID"] = request_id
            response.headers["X-API-Version"] = self.version
            response.headers["X-Response-Time"] = str(
                round((time.time() - start_time) * 1000, 2)
            )

            return response

        except HTTPException as exc:
            return self._create_error_response(
                request_id=request_id,
                status_code=exc.status_code,
                error_code=f"HTTP_{exc.status_code}",
                message=exc.detail,
            )
        except RequestValidationError as exc:
            return self._create_validation_error_response(
                request_id=request_id,
                validation_error=exc,
            )
        except ValidationError as exc:
            return self._create_validation_error_response(
                request_id=request_id,
                validation_error=exc,
            )
        except Exception:
            return self._create_error_response(
                request_id=request_id,
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                error_code="INTERNAL_SERVER_ERROR",
                message="An unexpected error occurred",
            )

    def _create_error_response(
        self,
        request_id: str,
        status_code: int,
        error_code: str,
        message: str,
    ) -> JSONResponse:
        """Create a standardized error response."""
        metadata = APIMetadata(
            request_id=request_id,
            version=self.version,
            environment=self.environment,
        )

        error_response = ErrorResponse(
            error=ErrorDetail(code=error_code, message=message),
            metadata=metadata,
        )

        return JSONResponse(
            status_code=status_code,
            content=json.loads(
                json.dumps(error_response.model_dump(), default=json_encoder)
            ),
            headers={
                "X-Request-ID": request_id,
                "X-API-Version": self.version,
            },
        )

    def _create_validation_error_response(
        self,
        request_id: str,
        validation_error: RequestValidationError | ValidationError,
    ) -> JSONResponse:
        """Create a standardized validation error response."""
        metadata = APIMetadata(
            request_id=request_id,
            version=self.version,
            environment=self.environment,
        )

        validation_errors = []
        if isinstance(validation_error, RequestValidationError):
            for error in validation_error.errors():
                field = ".".join(str(loc) for loc in error["loc"])
                validation_errors.append(
                    ValidationErrorDetail(
                        field=field,
                        message=error["msg"],
                        invalid_value=error.get("input"),
                    )
                )
        else:
            for error in validation_error.errors():
                field = ".".join(str(loc) for loc in error["loc"])
                validation_errors.append(
                    ValidationErrorDetail(
                        field=field,
                        message=error["msg"],
                        invalid_value=error.get("input"),
                    )
                )

        error_response = ValidationErrorResponse(
            error=ErrorDetail(
                code="VALIDATION_ERROR",
                message="Request validation failed",
            ),
            validation_errors=validation_errors,
            metadata=metadata,
        )

        return JSONResponse(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            content=json.loads(
                json.dumps(error_response.model_dump(), default=json_encoder)
            ),
            headers={
                "X-Request-ID": request_id,
                "X-API-Version": self.version,
            },
        )


class RequestLoggingMiddleware(BaseHTTPMiddleware):
    """Middleware for logging API requests and responses."""

    async def dispatch(self, request: Request, call_next) -> Response:
        start_time = time.time()

        # Log request
        print(
            f"🔵 {request.method} {request.url.path} - Request ID: {getattr(request.state, 'request_id', 'N/A')}"
        )

        response = await call_next(request)

        # Log response
        process_time = round((time.time() - start_time) * 1000, 2)
        print(
            f"🟢 {request.method} {request.url.path} - {response.status_code} - {process_time}ms"
        )

        return response


class SecurityHeadersMiddleware(BaseHTTPMiddleware):
    """Middleware to add security headers to all responses."""

    async def dispatch(self, request: Request, call_next) -> Response:
        response = await call_next(request)

        # Add security headers
        response.headers["X-Content-Type-Options"] = "nosniff"
        response.headers["X-Frame-Options"] = "DENY"
        response.headers["X-XSS-Protection"] = "1; mode=block"
        response.headers["Referrer-Policy"] = "strict-origin-when-cross-origin"

        return response


class RateLimitMiddleware(BaseHTTPMiddleware):
    """Basic rate limiting middleware (placeholder for production implementation)."""

    def __init__(self, app, requests_per_minute: int = 100):
        super().__init__(app)
        self.requests_per_minute = requests_per_minute
        # In production, this would use Redis or similar
        self.request_counts: dict[str, list[float]] = {}

    async def dispatch(self, request: Request, call_next) -> Response:
        # Basic rate limiting based on IP
        client_ip = request.client.host if request.client else "unknown"
        current_time = time.time()

        # Clean old requests (older than 1 minute)
        if client_ip in self.request_counts:
            self.request_counts[client_ip] = [
                req_time
                for req_time in self.request_counts[client_ip]
                if current_time - req_time < 60
            ]
        else:
            self.request_counts[client_ip] = []

        # Check rate limit
        if len(self.request_counts[client_ip]) >= self.requests_per_minute:
            metadata = APIMetadata(
                request_id=getattr(request.state, "request_id", str(uuid4())),
                version="v1",
                environment="development",
            )

            error_response = ErrorResponse(
                error=ErrorDetail(
                    code="RATE_LIMIT_EXCEEDED",
                    message=f"Rate limit exceeded. Maximum {self.requests_per_minute} requests per minute.",
                ),
                metadata=metadata,
            )

            return JSONResponse(
                status_code=status.HTTP_429_TOO_MANY_REQUESTS,
                content=json.loads(
                    json.dumps(error_response.model_dump(), default=json_encoder)
                ),
                headers={
                    "X-RateLimit-Limit": str(self.requests_per_minute),
                    "X-RateLimit-Remaining": "0",
                    "X-RateLimit-Reset": str(int(current_time + 60)),
                },
            )

        # Add current request to count
        self.request_counts[client_ip].append(current_time)

        response = await call_next(request)

        # Add rate limit headers
        remaining = max(
            0, self.requests_per_minute - len(self.request_counts[client_ip])
        )
        response.headers["X-RateLimit-Limit"] = str(self.requests_per_minute)
        response.headers["X-RateLimit-Remaining"] = str(remaining)
        response.headers["X-RateLimit-Reset"] = str(int(current_time + 60))

        return response
