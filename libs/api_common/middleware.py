"""Middleware for standardized API responses and error handling."""

import json
import time
from collections import defaultdict
from datetime import datetime
from typing import Any
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


class DDoSProtectionMiddleware(BaseHTTPMiddleware):
    """Advanced DDoS protection middleware with multiple detection mechanisms."""

    def __init__(
        self,
        app,
        requests_per_minute: int = 100,
        burst_requests_per_second: int = 10,
        max_concurrent_requests: int = 50,
        suspicious_patterns_threshold: int = 5,
        auto_ban_duration: int = 3600,  # 1 hour
    ):
        super().__init__(app)

        # Validate configuration to prevent DoS attacks via misconfiguration
        if requests_per_minute <= 0 or requests_per_minute > 10000:
            raise ValueError("requests_per_minute must be between 1 and 10000")

        if burst_requests_per_second <= 0 or burst_requests_per_second > 1000:
            raise ValueError("burst_requests_per_second must be between 1 and 1000")

        if max_concurrent_requests <= 0 or max_concurrent_requests > 10000:
            raise ValueError("max_concurrent_requests must be between 1 and 10000")

        if suspicious_patterns_threshold <= 0 or suspicious_patterns_threshold > 100:
            raise ValueError("suspicious_patterns_threshold must be between 1 and 100")

        if auto_ban_duration < 60 or auto_ban_duration > 86400:  # 1 minute to 24 hours
            raise ValueError("auto_ban_duration must be between 60 and 86400 seconds")

        # Ensure burst limit doesn't exceed per-minute limit
        if burst_requests_per_second * 60 > requests_per_minute:
            raise ValueError(
                "burst_requests_per_second * 60 cannot exceed requests_per_minute"
            )

        self.requests_per_minute = requests_per_minute
        self.burst_requests_per_second = burst_requests_per_second
        self.max_concurrent_requests = max_concurrent_requests
        self.suspicious_patterns_threshold = suspicious_patterns_threshold
        self.auto_ban_duration = auto_ban_duration

        # In-memory storage (use Redis in production)
        self.request_counts: dict[str, list[float]] = defaultdict(list)
        self.burst_counts: dict[str, list[float]] = defaultdict(list)
        self.concurrent_requests: dict[str, int] = defaultdict(int)
        self.banned_ips: dict[str, float] = {}
        self.suspicious_patterns: dict[str, int] = defaultdict(int)
        self.failed_auth_attempts: dict[str, list[float]] = defaultdict(list)

    async def dispatch(self, request: Request, call_next) -> Response:
        client_ip = self._get_client_ip(request)
        current_time = time.time()

        # Check if IP is banned
        if self._is_banned(client_ip, current_time):
            return self._create_banned_response(request)

        # Check various DDoS protection mechanisms
        if self._check_rate_limit(client_ip, current_time):
            self._record_suspicious_activity(client_ip, "rate_limit_exceeded")
            return self._create_rate_limit_response(request)

        if self._check_burst_limit(client_ip, current_time):
            self._record_suspicious_activity(client_ip, "burst_limit_exceeded")
            return self._create_rate_limit_response(request, is_burst=True)

        if self._check_concurrent_limit(client_ip):
            self._record_suspicious_activity(client_ip, "concurrent_limit_exceeded")
            return self._create_concurrent_limit_response(request)

        # Check for suspicious patterns
        if self._detect_suspicious_patterns(request, client_ip):
            self._record_suspicious_activity(client_ip, "suspicious_pattern")
            return self._create_suspicious_activity_response(request)

        # Record request and increment concurrent counter
        self._record_request(client_ip, current_time)
        self.concurrent_requests[client_ip] += 1

        try:
            response = await call_next(request)

            # Check for auth failures
            if response.status_code == 401:
                self._record_auth_failure(client_ip, current_time)

            return response

        finally:
            # Decrement concurrent counter
            self.concurrent_requests[client_ip] -= 1
            if self.concurrent_requests[client_ip] <= 0:
                del self.concurrent_requests[client_ip]

    def _get_client_ip(self, request: Request) -> str:
        """Extract client IP considering proxies."""
        # Check X-Forwarded-For header (from load balancers/proxies)
        forwarded_for = request.headers.get("X-Forwarded-For")
        if forwarded_for:
            return forwarded_for.split(",")[0].strip()

        # Check X-Real-IP header
        real_ip = request.headers.get("X-Real-IP")
        if real_ip:
            return real_ip

        # Fall back to direct connection IP
        return request.client.host if request.client else "unknown"

    def _is_banned(self, ip: str, current_time: float) -> bool:
        """Check if IP is currently banned."""
        if ip in self.banned_ips:
            ban_expiry = self.banned_ips[ip]
            if current_time < ban_expiry:
                return True
            else:
                # Ban expired, remove from list
                del self.banned_ips[ip]
        return False

    def _check_rate_limit(self, ip: str, current_time: float) -> bool:
        """Check standard rate limit."""
        # Clean old requests (older than 1 minute)
        self.request_counts[ip] = [
            req_time
            for req_time in self.request_counts[ip]
            if current_time - req_time < 60
        ]

        return len(self.request_counts[ip]) >= self.requests_per_minute

    def _check_burst_limit(self, ip: str, current_time: float) -> bool:
        """Check burst rate limit."""
        # Clean old burst requests (older than 1 second)
        self.burst_counts[ip] = [
            req_time
            for req_time in self.burst_counts[ip]
            if current_time - req_time < 1
        ]

        return len(self.burst_counts[ip]) >= self.burst_requests_per_second

    def _check_concurrent_limit(self, ip: str) -> bool:
        """Check concurrent request limit."""
        return self.concurrent_requests[ip] >= self.max_concurrent_requests

    def _detect_suspicious_patterns(self, request: Request, ip: str) -> bool:
        """Detect suspicious request patterns."""
        suspicious_indicators = 0

        # Check for suspicious user agents
        user_agent = request.headers.get("user-agent", "").lower()
        suspicious_agents = ["bot", "crawler", "spider", "scraper", "curl", "wget"]
        if any(agent in user_agent for agent in suspicious_agents):
            suspicious_indicators += 1

        # Check for missing common headers
        if not request.headers.get("accept"):
            suspicious_indicators += 1
        if not request.headers.get("accept-language"):
            suspicious_indicators += 1

        # Check for unusual request methods
        if request.method not in ["GET", "POST", "PUT", "DELETE", "PATCH"]:
            suspicious_indicators += 1

        # Check for suspicious paths
        path = request.url.path.lower()
        suspicious_paths = ["/admin", "/.env", "/config", "/backup", "/phpmyadmin"]
        if any(sus_path in path for sus_path in suspicious_paths):
            suspicious_indicators += 2

        return suspicious_indicators >= 2

    def _record_request(self, ip: str, current_time: float) -> None:
        """Record a new request."""
        self.request_counts[ip].append(current_time)
        self.burst_counts[ip].append(current_time)

    def _record_auth_failure(self, ip: str, current_time: float) -> None:
        """Record an authentication failure."""
        # Clean old failures (older than 5 minutes)
        self.failed_auth_attempts[ip] = [
            failure_time
            for failure_time in self.failed_auth_attempts[ip]
            if current_time - failure_time < 300
        ]

        self.failed_auth_attempts[ip].append(current_time)

        # Auto-ban after multiple failed attempts
        if len(self.failed_auth_attempts[ip]) >= 5:
            self._ban_ip(ip, current_time)

    def _record_suspicious_activity(self, ip: str, activity_type: str) -> None:
        """Record suspicious activity and potentially ban IP."""
        self.suspicious_patterns[ip] += 1

        # Auto-ban if too many suspicious activities
        if self.suspicious_patterns[ip] >= self.suspicious_patterns_threshold:
            self._ban_ip(ip, time.time())

    def _ban_ip(self, ip: str, current_time: float) -> None:
        """Ban an IP address."""
        self.banned_ips[ip] = current_time + self.auto_ban_duration
        # Clear other counters for banned IP
        if ip in self.request_counts:
            del self.request_counts[ip]
        if ip in self.burst_counts:
            del self.burst_counts[ip]
        if ip in self.suspicious_patterns:
            del self.suspicious_patterns[ip]

    def _create_rate_limit_response(
        self, request: Request, is_burst: bool = False
    ) -> JSONResponse:
        """Create rate limit exceeded response."""
        error_code = "BURST_RATE_LIMIT_EXCEEDED" if is_burst else "RATE_LIMIT_EXCEEDED"
        message = (
            f"Burst rate limit exceeded. Maximum {self.burst_requests_per_second} requests per second."
            if is_burst
            else f"Rate limit exceeded. Maximum {self.requests_per_minute} requests per minute."
        )

        metadata = APIMetadata(
            request_id=getattr(request.state, "request_id", str(uuid4())),
            version="v1",
            environment="development",
        )

        error_response = ErrorResponse(
            error=ErrorDetail(code=error_code, message=message),
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
                "Retry-After": "60" if not is_burst else "1",
            },
        )

    def _create_concurrent_limit_response(self, request: Request) -> JSONResponse:
        """Create concurrent limit exceeded response."""
        metadata = APIMetadata(
            request_id=getattr(request.state, "request_id", str(uuid4())),
            version="v1",
            environment="development",
        )

        error_response = ErrorResponse(
            error=ErrorDetail(
                code="CONCURRENT_LIMIT_EXCEEDED",
                message=f"Too many concurrent requests. Maximum {self.max_concurrent_requests} allowed.",
            ),
            metadata=metadata,
        )

        return JSONResponse(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            content=json.loads(
                json.dumps(error_response.model_dump(), default=json_encoder)
            ),
        )

    def _create_suspicious_activity_response(self, request: Request) -> JSONResponse:
        """Create suspicious activity detected response."""
        metadata = APIMetadata(
            request_id=getattr(request.state, "request_id", str(uuid4())),
            version="v1",
            environment="development",
        )

        error_response = ErrorResponse(
            error=ErrorDetail(
                code="SUSPICIOUS_ACTIVITY_DETECTED",
                message="Suspicious activity detected. Access temporarily restricted.",
            ),
            metadata=metadata,
        )

        return JSONResponse(
            status_code=status.HTTP_403_FORBIDDEN,
            content=json.loads(
                json.dumps(error_response.model_dump(), default=json_encoder)
            ),
        )

    def _create_banned_response(self, request: Request) -> JSONResponse:
        """Create banned IP response."""
        metadata = APIMetadata(
            request_id=getattr(request.state, "request_id", str(uuid4())),
            version="v1",
            environment="development",
        )

        error_response = ErrorResponse(
            error=ErrorDetail(
                code="IP_BANNED",
                message="Your IP address has been temporarily banned due to suspicious activity.",
            ),
            metadata=metadata,
        )

        return JSONResponse(
            status_code=status.HTTP_403_FORBIDDEN,
            content=json.loads(
                json.dumps(error_response.model_dump(), default=json_encoder)
            ),
        )

    def get_stats(self) -> dict[str, Any]:
        """Get DDoS protection statistics."""
        current_time = time.time()
        active_bans = sum(
            1 for ban_time in self.banned_ips.values() if ban_time > current_time
        )

        return {
            "active_bans": active_bans,
            "total_ips_tracked": len(self.request_counts),
            "concurrent_requests": sum(self.concurrent_requests.values()),
            "suspicious_ips": len(self.suspicious_patterns),
            "config": {
                "requests_per_minute": self.requests_per_minute,
                "burst_requests_per_second": self.burst_requests_per_second,
                "max_concurrent_requests": self.max_concurrent_requests,
                "auto_ban_duration": self.auto_ban_duration,
            },
        }
