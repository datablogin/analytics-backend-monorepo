"""Intelligent retry mechanisms with backoff strategies."""

import inspect
import random
import time
from abc import ABC, abstractmethod
from collections.abc import Callable
from datetime import UTC, datetime  # type: ignore[attr-defined]
from enum import Enum
from typing import Any

import structlog
from pydantic import BaseModel, Field

logger = structlog.get_logger(__name__)


class RetryStrategy(str, Enum):
    """Retry strategy types."""

    NONE = "none"
    FIXED_INTERVAL = "fixed_interval"
    EXPONENTIAL_BACKOFF = "exponential_backoff"
    LINEAR_BACKOFF = "linear_backoff"
    POLYNOMIAL_BACKOFF = "polynomial_backoff"
    CUSTOM = "custom"


class RetryCondition(str, Enum):
    """Conditions for when to retry."""

    ALL_ERRORS = "all_errors"
    TRANSIENT_ERRORS = "transient_errors"
    TIMEOUT_ERRORS = "timeout_errors"
    NETWORK_ERRORS = "network_errors"
    CUSTOM_CONDITION = "custom_condition"


class BackoffStrategy(ABC):
    """Abstract base class for backoff strategies."""

    @abstractmethod
    def calculate_delay(self, attempt: int, base_delay: float) -> float:
        """Calculate delay for given attempt number."""
        pass


class FixedIntervalStrategy(BackoffStrategy):
    """Fixed interval between retries."""

    def calculate_delay(self, attempt: int, base_delay: float) -> float:
        """Return fixed delay regardless of attempt number."""
        return base_delay


class ExponentialBackoffStrategy(BackoffStrategy):
    """Exponential backoff with optional jitter."""

    def __init__(
        self, multiplier: float = 2.0, max_delay: float = 300.0, jitter: bool = True
    ):
        self.multiplier = multiplier
        self.max_delay = max_delay
        self.jitter = jitter

    def calculate_delay(self, attempt: int, base_delay: float) -> float:
        """Calculate exponential backoff delay."""
        # Calculate base exponential delay
        delay = base_delay * (self.multiplier ** (attempt - 1))

        # Apply maximum delay limit
        delay = min(delay, self.max_delay)

        # Add jitter to avoid thundering herd
        if self.jitter:
            jitter_range = delay * 0.1  # 10% jitter
            delay += random.uniform(-jitter_range, jitter_range)

        return max(0, delay)


class LinearBackoffStrategy(BackoffStrategy):
    """Linear backoff strategy."""

    def __init__(self, increment: float = 1.0, max_delay: float = 300.0):
        self.increment = increment
        self.max_delay = max_delay

    def calculate_delay(self, attempt: int, base_delay: float) -> float:
        """Calculate linear backoff delay."""
        delay = base_delay + (self.increment * (attempt - 1))
        return min(delay, self.max_delay)


class PolynomialBackoffStrategy(BackoffStrategy):
    """Polynomial backoff strategy."""

    def __init__(self, power: float = 2.0, max_delay: float = 300.0):
        self.power = power
        self.max_delay = max_delay

    def calculate_delay(self, attempt: int, base_delay: float) -> float:
        """Calculate polynomial backoff delay."""
        delay = base_delay * (attempt**self.power)
        return min(delay, self.max_delay)


class RetryPolicy(BaseModel):
    """Comprehensive retry policy configuration."""

    # Basic retry settings
    max_attempts: int = Field(
        default=3, ge=1, le=10, description="Maximum retry attempts"
    )
    base_delay_seconds: float = Field(
        default=1.0, ge=0.1, description="Base delay between retries"
    )
    strategy: RetryStrategy = Field(
        default=RetryStrategy.EXPONENTIAL_BACKOFF, description="Retry strategy"
    )

    # Backoff strategy parameters
    exponential_multiplier: float = Field(
        default=2.0, ge=1.0, description="Exponential multiplier"
    )
    linear_increment: float = Field(default=1.0, ge=0.1, description="Linear increment")
    polynomial_power: float = Field(default=2.0, ge=1.0, description="Polynomial power")
    max_delay_seconds: float = Field(
        default=300.0, ge=1.0, description="Maximum delay between retries"
    )
    jitter: bool = Field(default=True, description="Add random jitter to delays")

    # Retry conditions
    retry_condition: RetryCondition = Field(
        default=RetryCondition.TRANSIENT_ERRORS, description="When to retry"
    )
    retryable_errors: list[str] = Field(
        default_factory=lambda: [
            "ConnectionError",
            "TimeoutError",
            "TemporaryFailure",
            "ServiceUnavailableError",
            "RateLimitError",
        ],
        description="List of retryable error types",
    )

    # Circuit breaker settings
    circuit_breaker_enabled: bool = Field(
        default=False, description="Enable circuit breaker"
    )
    circuit_breaker_failure_threshold: int = Field(
        default=5, description="Failures before circuit opens"
    )
    circuit_breaker_timeout_seconds: float = Field(
        default=60.0, description="Circuit breaker timeout"
    )

    # Custom retry logic
    custom_retry_function: str | None = Field(
        default=None, description="Custom retry function name"
    )

    def create_backoff_strategy(self) -> BackoffStrategy:
        """Create backoff strategy based on configuration."""
        if self.strategy == RetryStrategy.FIXED_INTERVAL:
            return FixedIntervalStrategy()
        elif self.strategy == RetryStrategy.EXPONENTIAL_BACKOFF:
            return ExponentialBackoffStrategy(
                multiplier=self.exponential_multiplier,
                max_delay=self.max_delay_seconds,
                jitter=self.jitter,
            )
        elif self.strategy == RetryStrategy.LINEAR_BACKOFF:
            return LinearBackoffStrategy(
                increment=self.linear_increment, max_delay=self.max_delay_seconds
            )
        elif self.strategy == RetryStrategy.POLYNOMIAL_BACKOFF:
            return PolynomialBackoffStrategy(
                power=self.polynomial_power, max_delay=self.max_delay_seconds
            )
        else:
            # Default to exponential backoff
            return ExponentialBackoffStrategy(jitter=self.jitter)

    def should_retry(self, error: Exception, attempt: int) -> bool:
        """Determine if error should be retried."""
        # Check if we've exceeded max attempts
        if attempt >= self.max_attempts:
            return False

        # Check retry condition
        if self.retry_condition == RetryCondition.ALL_ERRORS:
            return True
        elif self.retry_condition == RetryCondition.TRANSIENT_ERRORS:
            return self._is_transient_error(error)
        elif self.retry_condition == RetryCondition.TIMEOUT_ERRORS:
            return self._is_timeout_error(error)
        elif self.retry_condition == RetryCondition.NETWORK_ERRORS:
            return self._is_network_error(error)
        else:
            return False

    def _is_transient_error(self, error: Exception) -> bool:
        """Check if error is transient and retryable."""
        error_type = type(error).__name__
        error_message = str(error).lower()

        # Check against retryable error types
        if error_type in self.retryable_errors:
            return True

        # Check for common transient error patterns in message
        transient_patterns = [
            "connection",
            "timeout",
            "temporary",
            "unavailable",
            "rate limit",
            "too many requests",
            "service busy",
        ]

        return any(pattern in error_message for pattern in transient_patterns)

    def _is_timeout_error(self, error: Exception) -> bool:
        """Check if error is timeout-related."""
        error_type = type(error).__name__
        error_message = str(error).lower()

        timeout_indicators = ["timeout", "timeouterror"]
        return error_type in timeout_indicators or any(
            indicator in error_message for indicator in timeout_indicators
        )

    def _is_network_error(self, error: Exception) -> bool:
        """Check if error is network-related."""
        error_type = type(error).__name__
        error_message = str(error).lower()

        network_indicators = [
            "connectionerror",
            "networkerror",
            "dnserror",
            "connection",
            "network",
            "dns",
        ]
        return error_type in network_indicators or any(
            indicator in error_message for indicator in network_indicators
        )


class CircuitBreaker:
    """Circuit breaker for retry policy."""

    def __init__(self, failure_threshold: int = 5, timeout_seconds: float = 60.0):
        self.failure_threshold = failure_threshold
        self.timeout_seconds = timeout_seconds
        self.failure_count = 0
        self.last_failure_time: datetime | None = None
        self.state = "closed"  # closed, open, half-open

    def can_execute(self) -> bool:
        """Check if execution is allowed."""
        if self.state == "closed":
            return True
        elif self.state == "open":
            if self.last_failure_time:
                time_since_failure = (
                    datetime.now(UTC) - self.last_failure_time
                ).total_seconds()
                if time_since_failure >= self.timeout_seconds:
                    self.state = "half-open"
                    return True
            return False
        else:  # half-open
            return True

    def record_success(self) -> None:
        """Record successful execution."""
        self.failure_count = 0
        self.state = "closed"
        self.last_failure_time = None

    def record_failure(self) -> None:
        """Record failed execution."""
        self.failure_count += 1
        self.last_failure_time = datetime.now(UTC)

        if self.failure_count >= self.failure_threshold:
            self.state = "open"


class RetryExecutor:
    """Executes functions with retry logic."""

    def __init__(self, policy: RetryPolicy):
        self.policy = policy
        self.backoff_strategy = policy.create_backoff_strategy()
        self.circuit_breaker = (
            CircuitBreaker(
                failure_threshold=policy.circuit_breaker_failure_threshold,
                timeout_seconds=policy.circuit_breaker_timeout_seconds,
            )
            if policy.circuit_breaker_enabled
            else None
        )

    async def execute_with_retry(
        self,
        func: Callable,
        *args,
        task_name: str = "unknown",
        context: dict[str, Any] | None = None,
        **kwargs,
    ) -> tuple[Any, int]:
        """Execute function with retry logic."""
        attempt = 0
        last_error = None

        while attempt < self.policy.max_attempts:
            attempt += 1

            # Check circuit breaker
            if self.circuit_breaker and not self.circuit_breaker.can_execute():
                raise Exception(f"Circuit breaker is open for task '{task_name}'")

            try:
                logger.info(
                    "Executing task",
                    task_name=task_name,
                    attempt=attempt,
                    max_attempts=self.policy.max_attempts,
                )

                # Execute the function
                if hasattr(func, "__call__"):
                    if inspect.iscoroutinefunction(func):
                        result = await func(*args, **kwargs)
                    else:
                        result = func(*args, **kwargs)
                else:
                    raise ValueError(f"Function '{func}' is not callable")

                # Record success
                if self.circuit_breaker:
                    self.circuit_breaker.record_success()

                logger.info(
                    "Task executed successfully", task_name=task_name, attempt=attempt
                )

                return result, attempt - 1  # Return result and retry count (attempts - 1)

            except Exception as error:
                last_error = error

                # Record failure for circuit breaker
                if self.circuit_breaker:
                    self.circuit_breaker.record_failure()

                logger.warning(
                    "Task execution failed",
                    task_name=task_name,
                    attempt=attempt,
                    error=str(error),
                    error_type=type(error).__name__,
                )

                # Check if we should retry
                if not self.policy.should_retry(error, attempt):
                    logger.error(
                        "Task will not be retried",
                        task_name=task_name,
                        attempt=attempt,
                        reason="retry condition not met or max attempts reached",
                    )
                    break

                # Calculate delay before next attempt
                if attempt < self.policy.max_attempts:
                    delay = self.backoff_strategy.calculate_delay(
                        attempt, self.policy.base_delay_seconds
                    )

                    logger.info(
                        "Retrying task after delay",
                        task_name=task_name,
                        attempt=attempt,
                        delay_seconds=delay,
                    )

                    # Wait before retry
                    time.sleep(delay)

        # If we get here, all retry attempts failed
        logger.error(
            "Task failed after all retry attempts",
            task_name=task_name,
            total_attempts=attempt,
            final_error=str(last_error),
        )

        # Include retry count in the exception for tracking
        class TaskExecutionError(Exception):
            """Custom exception with retry count tracking."""
            def __init__(self, message: str, retry_count: int, original_error: Exception | None = None):
                super().__init__(message)
                self.retry_count = retry_count
                self.original_error = original_error

        raise TaskExecutionError(
            f"Task '{task_name}' failed after {attempt - 1} retries ({attempt} attempts)",
            retry_count=attempt - 1,
            original_error=last_error
        )


def create_default_retry_policy() -> RetryPolicy:
    """Create default retry policy for workflow tasks."""
    return RetryPolicy(
        max_attempts=3,
        base_delay_seconds=2.0,
        strategy=RetryStrategy.EXPONENTIAL_BACKOFF,
        exponential_multiplier=2.0,
        max_delay_seconds=120.0,
        jitter=True,
        retry_condition=RetryCondition.TRANSIENT_ERRORS,
    )


def create_aggressive_retry_policy() -> RetryPolicy:
    """Create aggressive retry policy for critical tasks."""
    return RetryPolicy(
        max_attempts=5,
        base_delay_seconds=1.0,
        strategy=RetryStrategy.EXPONENTIAL_BACKOFF,
        exponential_multiplier=1.5,
        max_delay_seconds=60.0,
        jitter=True,
        retry_condition=RetryCondition.ALL_ERRORS,
        circuit_breaker_enabled=True,
        circuit_breaker_failure_threshold=3,
        circuit_breaker_timeout_seconds=30.0,
    )
