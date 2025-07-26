"""Performance profiling and monitoring for streaming analytics components."""

import asyncio
import cProfile
import io
import pstats
import time
import tracemalloc
from collections import defaultdict, deque
from collections.abc import Callable
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from datetime import datetime
from functools import wraps
from typing import Any

import structlog

logger = structlog.get_logger(__name__)


@dataclass
class PerformanceProfile:
    """Performance profiling data for a component."""

    component_name: str
    start_time: datetime
    end_time: datetime | None = None
    cpu_time_seconds: float = 0.0
    memory_peak_mb: float = 0.0
    memory_current_mb: float = 0.0
    call_count: int = 0
    avg_call_duration_ms: float = 0.0
    max_call_duration_ms: float = 0.0
    min_call_duration_ms: float = float("inf")
    error_count: int = 0
    throughput_ops_per_sec: float = 0.0
    metadata: dict[str, Any] = field(default_factory=dict)

    @property
    def duration_seconds(self) -> float:
        """Total profiling duration in seconds."""
        if self.end_time is None:
            return 0.0
        return (self.end_time - self.start_time).total_seconds()

    @property
    def success_rate(self) -> float:
        """Success rate as percentage."""
        if self.call_count == 0:
            return 100.0
        return ((self.call_count - self.error_count) / self.call_count) * 100

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for serialization."""
        return {
            "component_name": self.component_name,
            "start_time": self.start_time.isoformat(),
            "end_time": self.end_time.isoformat() if self.end_time else None,
            "duration_seconds": self.duration_seconds,
            "cpu_time_seconds": self.cpu_time_seconds,
            "memory_peak_mb": self.memory_peak_mb,
            "memory_current_mb": self.memory_current_mb,
            "call_count": self.call_count,
            "avg_call_duration_ms": self.avg_call_duration_ms,
            "max_call_duration_ms": self.max_call_duration_ms,
            "min_call_duration_ms": self.min_call_duration_ms
            if self.min_call_duration_ms != float("inf")
            else 0.0,
            "error_count": self.error_count,
            "success_rate": self.success_rate,
            "throughput_ops_per_sec": self.throughput_ops_per_sec,
            "metadata": self.metadata,
        }


@dataclass
class MethodProfile:
    """Profiling data for individual method calls."""

    method_name: str
    call_count: int = 0
    total_time_ms: float = 0.0
    min_time_ms: float = float("inf")
    max_time_ms: float = 0.0
    error_count: int = 0
    last_call_time: datetime | None = None

    @property
    def avg_time_ms(self) -> float:
        """Average call time in milliseconds."""
        return self.total_time_ms / max(self.call_count, 1)

    def add_call(self, duration_ms: float, had_error: bool = False) -> None:
        """Add a method call measurement."""
        self.call_count += 1
        self.total_time_ms += duration_ms
        self.min_time_ms = min(self.min_time_ms, duration_ms)
        self.max_time_ms = max(self.max_time_ms, duration_ms)
        self.last_call_time = datetime.utcnow()

        if had_error:
            self.error_count += 1


class PerformanceProfiler:
    """Main performance profiler for streaming analytics."""

    def __init__(
        self, enable_memory_profiling: bool = True, enable_cpu_profiling: bool = True
    ):
        self.enable_memory_profiling = enable_memory_profiling
        self.enable_cpu_profiling = enable_cpu_profiling
        self.logger = logger.bind(component="performance_profiler")

        # Profiling state
        self._profiles: dict[str, PerformanceProfile] = {}
        self._method_profiles: dict[str, dict[str, MethodProfile]] = defaultdict(dict)
        self._cpu_profiler: cProfile.Profile | None = None
        self._memory_snapshot = None

        # Performance tracking
        self._call_history: dict[str, deque] = defaultdict(lambda: deque(maxlen=1000))
        self._is_profiling = False

        # Initialize memory tracking if enabled
        if self.enable_memory_profiling:
            tracemalloc.start()

    def start_profiling(self, component_name: str) -> None:
        """Start profiling a component."""
        if component_name in self._profiles:
            self.logger.warning(
                "Profiling already active for component", component=component_name
            )
            return

        profile = PerformanceProfile(
            component_name=component_name,
            start_time=datetime.utcnow(),
        )

        self._profiles[component_name] = profile
        self._is_profiling = True

        # Start CPU profiling if enabled
        if self.enable_cpu_profiling:
            if self._cpu_profiler is None:
                self._cpu_profiler = cProfile.Profile()
            self._cpu_profiler.enable()

        # Take memory snapshot if enabled
        if self.enable_memory_profiling and tracemalloc.is_tracing():
            self._memory_snapshot = tracemalloc.take_snapshot()

        self.logger.info("Started profiling component", component=component_name)

    def stop_profiling(self, component_name: str) -> PerformanceProfile | None:
        """Stop profiling a component and return results."""
        if component_name not in self._profiles:
            self.logger.warning(
                "No active profiling for component", component=component_name
            )
            return None

        profile = self._profiles[component_name]
        profile.end_time = datetime.utcnow()

        # Stop CPU profiling
        if self.enable_cpu_profiling and self._cpu_profiler:
            self._cpu_profiler.disable()
            profile.cpu_time_seconds = self._extract_cpu_time()

        # Calculate memory usage
        if self.enable_memory_profiling:
            profile.memory_current_mb = self._get_current_memory_mb()
            profile.memory_peak_mb = self._get_peak_memory_mb()

        # Calculate aggregate metrics from method profiles
        if component_name in self._method_profiles:
            self._aggregate_method_metrics(profile, component_name)

        # Calculate throughput
        if profile.duration_seconds > 0:
            profile.throughput_ops_per_sec = (
                profile.call_count / profile.duration_seconds
            )

        del self._profiles[component_name]

        self.logger.info(
            "Stopped profiling component",
            component=component_name,
            duration_seconds=profile.duration_seconds,
            call_count=profile.call_count,
            avg_duration_ms=profile.avg_call_duration_ms,
        )

        return profile

    def profile_method(self, component_name: str, method_name: str):
        """Decorator to profile individual methods."""

        def decorator(func: Callable) -> Callable:
            if asyncio.iscoroutinefunction(func):

                @wraps(func)
                async def async_wrapper(*args, **kwargs):
                    start_time = time.time()
                    had_error = False

                    try:
                        result = await func(*args, **kwargs)
                        return result
                    except Exception:
                        had_error = True
                        raise
                    finally:
                        duration_ms = (time.time() - start_time) * 1000
                        self._record_method_call(
                            component_name, method_name, duration_ms, had_error
                        )

                return async_wrapper
            else:

                @wraps(func)
                def sync_wrapper(*args, **kwargs):
                    start_time = time.time()
                    had_error = False

                    try:
                        result = func(*args, **kwargs)
                        return result
                    except Exception:
                        had_error = True
                        raise
                    finally:
                        duration_ms = (time.time() - start_time) * 1000
                        self._record_method_call(
                            component_name, method_name, duration_ms, had_error
                        )

                return sync_wrapper

        return decorator

    @asynccontextmanager
    async def profile_context(self, component_name: str):
        """Context manager for profiling a code block."""
        self.start_profiling(component_name)
        try:
            yield
        finally:
            self.stop_profiling(component_name)

    def _record_method_call(
        self, component_name: str, method_name: str, duration_ms: float, had_error: bool
    ) -> None:
        """Record a method call for profiling."""
        # Update method profile
        if method_name not in self._method_profiles[component_name]:
            self._method_profiles[component_name][method_name] = MethodProfile(
                method_name
            )

        method_profile = self._method_profiles[component_name][method_name]
        method_profile.add_call(duration_ms, had_error)

        # Update component profile if active
        if component_name in self._profiles:
            profile = self._profiles[component_name]
            profile.call_count += 1

            if had_error:
                profile.error_count += 1

            # Update timing statistics
            if duration_ms > profile.max_call_duration_ms:
                profile.max_call_duration_ms = duration_ms
            if duration_ms < profile.min_call_duration_ms:
                profile.min_call_duration_ms = duration_ms

            # Recalculate average
            total_calls = profile.call_count
            if total_calls > 1:
                profile.avg_call_duration_ms = (
                    profile.avg_call_duration_ms * (total_calls - 1) + duration_ms
                ) / total_calls
            else:
                profile.avg_call_duration_ms = duration_ms

        # Store in call history for trend analysis
        self._call_history[f"{component_name}.{method_name}"].append(
            {
                "timestamp": datetime.utcnow(),
                "duration_ms": duration_ms,
                "had_error": had_error,
            }
        )

    def _aggregate_method_metrics(
        self, profile: PerformanceProfile, component_name: str
    ) -> None:
        """Aggregate metrics from method profiles."""
        method_profiles = self._method_profiles[component_name]

        if not method_profiles:
            return

        # Calculate weighted averages and totals
        total_calls = sum(mp.call_count for mp in method_profiles.values())
        total_errors = sum(mp.error_count for mp in method_profiles.values())

        if total_calls > 0:
            weighted_avg_time = (
                sum(mp.avg_time_ms * mp.call_count for mp in method_profiles.values())
                / total_calls
            )

            profile.avg_call_duration_ms = weighted_avg_time
            profile.call_count = total_calls
            profile.error_count = total_errors
            profile.max_call_duration_ms = max(
                mp.max_time_ms for mp in method_profiles.values()
            )
            profile.min_call_duration_ms = min(
                mp.min_time_ms
                for mp in method_profiles.values()
                if mp.min_time_ms != float("inf")
            )

    def _extract_cpu_time(self) -> float:
        """Extract CPU time from profiler."""
        if not self._cpu_profiler:
            return 0.0

        try:
            # Capture profiler stats
            s = io.StringIO()
            ps = pstats.Stats(self._cpu_profiler, stream=s)

            # Get total time from stats
            return getattr(ps, "total_tt", 0.0)
        except Exception as e:
            self.logger.warning("Failed to extract CPU time", error=str(e))
            return 0.0

    def _get_current_memory_mb(self) -> float:
        """Get current memory usage in MB."""
        if not tracemalloc.is_tracing():
            return 0.0

        try:
            current, peak = tracemalloc.get_traced_memory()
            return current / (1024 * 1024)  # Convert to MB
        except Exception as e:
            self.logger.warning("Failed to get current memory", error=str(e))
            return 0.0

    def _get_peak_memory_mb(self) -> float:
        """Get peak memory usage in MB."""
        if not tracemalloc.is_tracing():
            return 0.0

        try:
            current, peak = tracemalloc.get_traced_memory()
            return peak / (1024 * 1024)  # Convert to MB
        except Exception as e:
            self.logger.warning("Failed to get peak memory", error=str(e))
            return 0.0

    def get_method_profile(
        self, component_name: str, method_name: str
    ) -> MethodProfile | None:
        """Get profiling data for a specific method."""
        return self._method_profiles.get(component_name, {}).get(method_name)

    def get_component_method_profiles(
        self, component_name: str
    ) -> dict[str, MethodProfile]:
        """Get all method profiles for a component."""
        return self._method_profiles.get(component_name, {})

    def get_performance_summary(self) -> dict[str, Any]:
        """Get overall performance summary."""
        # Calculate aggregate metrics across all components
        total_calls = sum(
            sum(mp.call_count for mp in methods.values())
            for methods in self._method_profiles.values()
        )

        total_errors = sum(
            sum(mp.error_count for mp in methods.values())
            for methods in self._method_profiles.values()
        )

        # Get current memory usage
        current_memory_mb = self._get_current_memory_mb()
        peak_memory_mb = self._get_peak_memory_mb()

        # Calculate hotspots (methods with highest average time)
        all_methods = []
        for component_name, methods in self._method_profiles.items():
            for method_name, profile in methods.items():
                all_methods.append(
                    {
                        "component": component_name,
                        "method": method_name,
                        "avg_time_ms": profile.avg_time_ms,
                        "call_count": profile.call_count,
                        "total_time_ms": profile.total_time_ms,
                    }
                )

        # Sort by total time to find biggest bottlenecks
        from typing import cast

        hotspots = sorted(
            all_methods, key=lambda x: cast(float, x["total_time_ms"]), reverse=True
        )[:10]

        return {
            "total_calls": total_calls,
            "total_errors": total_errors,
            "success_rate": ((total_calls - total_errors) / max(total_calls, 1)) * 100,
            "current_memory_mb": current_memory_mb,
            "peak_memory_mb": peak_memory_mb,
            "active_profiles": list(self._profiles.keys()),
            "component_count": len(self._method_profiles),
            "method_count": sum(
                len(methods) for methods in self._method_profiles.values()
            ),
            "hotspots": hotspots,
        }

    def generate_performance_report(self) -> dict[str, Any]:
        """Generate comprehensive performance report."""
        report = {
            "timestamp": datetime.utcnow().isoformat(),
            "summary": self.get_performance_summary(),
            "components": {},
            "trending": self._analyze_performance_trends(),
        }

        # Add detailed component data
        for component_name, methods in self._method_profiles.items():
            component_data = {
                "method_count": len(methods),
                "total_calls": sum(mp.call_count for mp in methods.values()),
                "total_errors": sum(mp.error_count for mp in methods.values()),
                "methods": {
                    method_name: {
                        "call_count": mp.call_count,
                        "avg_time_ms": mp.avg_time_ms,
                        "min_time_ms": mp.min_time_ms
                        if mp.min_time_ms != float("inf")
                        else 0.0,
                        "max_time_ms": mp.max_time_ms,
                        "error_count": mp.error_count,
                        "last_call": mp.last_call_time.isoformat()
                        if mp.last_call_time
                        else None,
                    }
                    for method_name, mp in methods.items()
                },
            }
            components = report.get("components")
            if isinstance(components, dict):
                components[component_name] = component_data

        return report

    def _analyze_performance_trends(self) -> dict[str, Any]:
        """Analyze performance trends from call history."""
        trends = {}

        for method_key, calls in self._call_history.items():
            if len(calls) < 10:  # Need minimum data for trend analysis
                continue

            # Convert to list for analysis
            call_list = list(calls)
            recent_calls = call_list[-50:]  # Last 50 calls
            older_calls = (
                call_list[-100:-50] if len(call_list) >= 100 else call_list[:-50]
            )

            if older_calls and recent_calls:
                recent_avg = sum(c["duration_ms"] for c in recent_calls) / len(
                    recent_calls
                )
                older_avg = sum(c["duration_ms"] for c in older_calls) / len(
                    older_calls
                )

                # Calculate trend (positive = getting slower, negative = getting faster)
                trend_percent = (
                    ((recent_avg - older_avg) / older_avg) * 100 if older_avg > 0 else 0
                )

                # Calculate error rate trend
                recent_errors = sum(1 for c in recent_calls if c["had_error"])
                older_errors = sum(1 for c in older_calls if c["had_error"])

                recent_error_rate = recent_errors / len(recent_calls) * 100
                older_error_rate = (
                    older_errors / len(older_calls) * 100 if older_calls else 0
                )

                trends[method_key] = {
                    "latency_trend_percent": trend_percent,
                    "recent_avg_ms": recent_avg,
                    "older_avg_ms": older_avg,
                    "recent_error_rate": recent_error_rate,
                    "older_error_rate": older_error_rate,
                    "sample_size": len(recent_calls),
                }

        return trends

    def reset_profiling_data(self, component_name: str | None = None) -> None:
        """Reset profiling data for a component or all components."""
        if component_name:
            if component_name in self._method_profiles:
                del self._method_profiles[component_name]
            if component_name in self._profiles:
                del self._profiles[component_name]
            # Clear call history for this component
            keys_to_remove = [
                k
                for k in self._call_history.keys()
                if k.startswith(f"{component_name}.")
            ]
            for key in keys_to_remove:
                del self._call_history[key]
        else:
            self._method_profiles.clear()
            self._profiles.clear()
            self._call_history.clear()

        self.logger.info("Reset profiling data", component=component_name or "all")


# Global profiler instance
_performance_profiler: PerformanceProfiler | None = None


def get_performance_profiler() -> PerformanceProfiler:
    """Get the global performance profiler instance."""
    global _performance_profiler

    if _performance_profiler is None:
        _performance_profiler = PerformanceProfiler()

    return _performance_profiler


# Decorator shortcuts for common use cases
def profile_streaming_method(component_name: str, method_name: str | None = None):
    """Decorator to profile streaming analytics methods."""

    def decorator(func: Callable) -> Callable:
        actual_method_name = method_name or func.__name__
        profiler = get_performance_profiler()
        return profiler.profile_method(component_name, actual_method_name)(func)

    return decorator


# Context manager for ad-hoc profiling
@asynccontextmanager
async def profile_streaming_component(component_name: str):
    """Context manager for profiling streaming components."""
    profiler = get_performance_profiler()
    async with profiler.profile_context(component_name):
        yield
