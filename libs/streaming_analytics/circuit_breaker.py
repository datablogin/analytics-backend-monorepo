"""Circuit breaker patterns for external service calls."""

import asyncio
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from typing import Any

import structlog

logger = structlog.get_logger(__name__)


class CircuitState(str, Enum):
    """Circuit breaker states."""
    CLOSED = "closed"  # Normal operation
    OPEN = "open"      # Failing, requests rejected
    HALF_OPEN = "half_open"  # Testing if service recovered


@dataclass
class CircuitBreakerConfig:
    """Configuration for circuit breaker."""
    failure_threshold: int = 5  # Number of failures to open circuit
    recovery_timeout_seconds: int = 60  # Time to wait before trying again
    success_threshold: int = 3  # Successes needed to close circuit in half-open
    timeout_seconds: float = 30.0  # Request timeout


@dataclass
class CircuitBreakerStats:
    """Statistics for circuit breaker."""
    state: CircuitState = CircuitState.CLOSED
    failure_count: int = 0
    success_count: int = 0
    last_failure_time: datetime | None = None
    total_requests: int = 0
    total_failures: int = 0
    total_successes: int = 0
    state_changes: list[dict[str, Any]] = field(default_factory=list)


class CircuitBreaker:
    """Circuit breaker implementation for external service calls."""

    def __init__(self, name: str, config: CircuitBreakerConfig | None = None):
        self.name = name
        self.config = config or CircuitBreakerConfig()
        self.stats = CircuitBreakerStats()
        self.logger = logger.bind(component="circuit_breaker", name=name)
        self._lock = asyncio.Lock()

    async def call(self, func: Callable, *args, **kwargs) -> Any:
        """Execute function with circuit breaker protection."""
        async with self._lock:
            # Check if we should reject the call
            if self._should_reject():
                self.stats.total_requests += 1
                raise CircuitBreakerOpenError(
                    f"Circuit breaker {self.name} is open"
                )

        # Attempt the call
        self.stats.total_requests += 1
        start_time = time.time()

        try:
            # Execute with timeout
            result = await asyncio.wait_for(
                self._execute_function(func, *args, **kwargs),
                timeout=self.config.timeout_seconds
            )

            # Record success
            await self._record_success()

            execution_time = time.time() - start_time
            self.logger.debug("Circuit breaker call succeeded",
                            execution_time=execution_time)

            return result

        except TimeoutError:
            await self._record_failure("timeout")
            raise CircuitBreakerTimeoutError(
                f"Circuit breaker {self.name} timeout after {self.config.timeout_seconds}s"
            )
        except Exception as e:
            await self._record_failure(str(e))
            raise

    async def _execute_function(self, func: Callable, *args, **kwargs) -> Any:
        """Execute the function (async or sync)."""
        if asyncio.iscoroutinefunction(func):
            return await func(*args, **kwargs)
        else:
            # Run sync function in executor
            loop = asyncio.get_event_loop()
            return await loop.run_in_executor(None, func, *args, **kwargs)

    def _should_reject(self) -> bool:
        """Check if the circuit breaker should reject the call."""
        if self.stats.state == CircuitState.CLOSED:
            return False

        if self.stats.state == CircuitState.OPEN:
            # Check if we should transition to half-open
            if (self.stats.last_failure_time and
                datetime.utcnow() - self.stats.last_failure_time >=
                timedelta(seconds=self.config.recovery_timeout_seconds)):
                self._transition_to_half_open()
                return False
            return True

        # HALF_OPEN state - allow limited calls
        return False

    async def _record_success(self) -> None:
        """Record a successful call."""
        self.stats.success_count += 1
        self.stats.total_successes += 1

        if self.stats.state == CircuitState.HALF_OPEN:
            if self.stats.success_count >= self.config.success_threshold:
                await self._transition_to_closed()
        elif self.stats.state == CircuitState.OPEN:
            # This shouldn't happen, but just in case
            self._transition_to_half_open()

    async def _record_failure(self, error: str) -> None:
        """Record a failed call."""
        self.stats.failure_count += 1
        self.stats.total_failures += 1
        self.stats.last_failure_time = datetime.utcnow()

        self.logger.warning("Circuit breaker call failed",
                          error=error,
                          failure_count=self.stats.failure_count,
                          state=self.stats.state)

        if self.stats.state == CircuitState.CLOSED:
            if self.stats.failure_count >= self.config.failure_threshold:
                await self._transition_to_open()
        elif self.stats.state == CircuitState.HALF_OPEN:
            await self._transition_to_open()

    async def _transition_to_open(self) -> None:
        """Transition circuit breaker to OPEN state."""
        old_state = self.stats.state
        self.stats.state = CircuitState.OPEN
        self.stats.success_count = 0

        self._record_state_change(old_state, CircuitState.OPEN)

        self.logger.warning("Circuit breaker opened",
                          failure_count=self.stats.failure_count,
                          failure_threshold=self.config.failure_threshold)

    async def _transition_to_closed(self) -> None:
        """Transition circuit breaker to CLOSED state."""
        old_state = self.stats.state
        self.stats.state = CircuitState.CLOSED
        self.stats.failure_count = 0
        self.stats.success_count = 0

        self._record_state_change(old_state, CircuitState.CLOSED)

        self.logger.info("Circuit breaker closed",
                        success_count=self.stats.success_count,
                        success_threshold=self.config.success_threshold)

    def _transition_to_half_open(self) -> None:
        """Transition circuit breaker to HALF_OPEN state."""
        old_state = self.stats.state
        self.stats.state = CircuitState.HALF_OPEN
        self.stats.success_count = 0

        self._record_state_change(old_state, CircuitState.HALF_OPEN)

        self.logger.info("Circuit breaker half-opened - testing service recovery")

    def _record_state_change(self, from_state: CircuitState, to_state: CircuitState) -> None:
        """Record a state change."""
        self.stats.state_changes.append({
            'from_state': from_state.value,
            'to_state': to_state.value,
            'timestamp': datetime.utcnow().isoformat(),
            'failure_count': self.stats.failure_count,
            'success_count': self.stats.success_count
        })

        # Keep only last 50 state changes
        if len(self.stats.state_changes) > 50:
            self.stats.state_changes = self.stats.state_changes[-50:]

    def get_stats(self) -> dict[str, Any]:
        """Get circuit breaker statistics."""
        success_rate = 0.0
        if self.stats.total_requests > 0:
            success_rate = self.stats.total_successes / self.stats.total_requests

        return {
            'name': self.name,
            'state': self.stats.state.value,
            'failure_count': self.stats.failure_count,
            'success_count': self.stats.success_count,
            'total_requests': self.stats.total_requests,
            'total_failures': self.stats.total_failures,
            'total_successes': self.stats.total_successes,
            'success_rate': success_rate,
            'last_failure_time': self.stats.last_failure_time.isoformat() if self.stats.last_failure_time else None,
            'config': {
                'failure_threshold': self.config.failure_threshold,
                'recovery_timeout_seconds': self.config.recovery_timeout_seconds,
                'success_threshold': self.config.success_threshold,
                'timeout_seconds': self.config.timeout_seconds
            },
            'recent_state_changes': self.stats.state_changes[-10:]  # Last 10 changes
        }

    def reset(self) -> None:
        """Reset the circuit breaker to initial state."""
        self.stats = CircuitBreakerStats()
        self.logger.info("Circuit breaker reset")


class CircuitBreakerOpenError(Exception):
    """Exception raised when circuit breaker is open."""
    pass


class CircuitBreakerTimeoutError(Exception):
    """Exception raised when circuit breaker times out."""
    pass


class CircuitBreakerManager:
    """Manager for multiple circuit breakers."""

    def __init__(self):
        self.breakers: dict[str, CircuitBreaker] = {}
        self.logger = logger.bind(component="circuit_breaker_manager")

    def get_breaker(self, name: str, config: CircuitBreakerConfig | None = None) -> CircuitBreaker:
        """Get or create a circuit breaker."""
        if name not in self.breakers:
            self.breakers[name] = CircuitBreaker(name, config)
            self.logger.info("Created circuit breaker", name=name)

        return self.breakers[name]

    def remove_breaker(self, name: str) -> bool:
        """Remove a circuit breaker."""
        if name in self.breakers:
            del self.breakers[name]
            self.logger.info("Removed circuit breaker", name=name)
            return True
        return False

    def get_all_stats(self) -> dict[str, dict[str, Any]]:
        """Get statistics for all circuit breakers."""
        return {name: breaker.get_stats() for name, breaker in self.breakers.items()}

    def reset_all(self) -> None:
        """Reset all circuit breakers."""
        for breaker in self.breakers.values():
            breaker.reset()
        self.logger.info("Reset all circuit breakers", count=len(self.breakers))


# Global circuit breaker manager
_circuit_breaker_manager = CircuitBreakerManager()


def get_circuit_breaker(name: str, config: CircuitBreakerConfig | None = None) -> CircuitBreaker:
    """Get a circuit breaker instance."""
    return _circuit_breaker_manager.get_breaker(name, config)


def get_circuit_breaker_manager() -> CircuitBreakerManager:
    """Get the global circuit breaker manager."""
    return _circuit_breaker_manager
