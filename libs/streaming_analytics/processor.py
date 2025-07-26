"""Stream processing framework with windowing and aggregations."""

import asyncio
import time
from abc import ABC, abstractmethod
from collections import defaultdict
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from typing import Any

import structlog

from .config import StreamProcessingConfig, get_streaming_config
from .event_store import EventSchema
from .performance_profiler import get_performance_profiler, profile_streaming_method

logger = structlog.get_logger(__name__)


class WindowType(str, Enum):
    """Types of streaming windows."""

    TUMBLING = "tumbling"
    SLIDING = "sliding"
    SESSION = "session"
    GLOBAL = "global"


class AggregationType(str, Enum):
    """Types of aggregations."""

    COUNT = "count"
    SUM = "sum"
    AVG = "average"
    MIN = "min"
    MAX = "max"
    DISTINCT_COUNT = "distinct_count"
    PERCENTILE = "percentile"
    FIRST = "first"
    LAST = "last"


@dataclass
class Window:
    """Represents a time window for stream processing."""

    start_time: datetime
    end_time: datetime
    window_type: WindowType
    key: str | None = None
    events: list[EventSchema] = field(default_factory=list)
    metadata: dict[str, Any] = field(default_factory=dict)

    @property
    def duration_ms(self) -> int:
        """Get window duration in milliseconds."""
        return int((self.end_time - self.start_time).total_seconds() * 1000)

    @property
    def event_count(self) -> int:
        """Get number of events in window."""
        return len(self.events)

    def add_event(self, event: EventSchema) -> None:
        """Add an event to the window."""
        self.events.append(event)

    def is_expired(self, current_time: datetime, allowed_lateness_ms: int = 0) -> bool:
        """Check if window is expired based on allowed lateness."""
        allowed_lateness = timedelta(milliseconds=allowed_lateness_ms)
        return current_time > (self.end_time + allowed_lateness)

    def get_event_times(self) -> list[datetime]:
        """Get timestamps of all events in window."""
        return [event.timestamp for event in self.events]


@dataclass
class AggregationResult:
    """Result of a stream aggregation."""

    window: Window
    aggregation_type: AggregationType
    field_name: str
    value: Any
    computed_at: datetime = field(default_factory=datetime.utcnow)
    metadata: dict[str, Any] = field(default_factory=dict)


class StreamProcessor(ABC):
    """Abstract base class for stream processors."""

    def __init__(self, config: StreamProcessingConfig | None = None):
        self.config = config or get_streaming_config().stream_processing
        self.logger = logger.bind(component=self.__class__.__name__)
        self._is_running = False
        self._processed_count = 0
        self._error_count = 0
        self._profiler = get_performance_profiler()

        # Performance optimization: pre-allocate commonly used objects
        self._stats_cache: dict[str, Any] = {
            "is_running": False,
            "processed_count": 0,
            "error_count": 0,
            "success_rate": 0.0,
        }
        self._last_stats_update = 0

    @abstractmethod
    @profile_streaming_method("stream_processor", "process_event")
    async def process_event(self, event: EventSchema) -> Any | None:
        """Process a single event."""
        pass

    async def start(self) -> None:
        """Start the stream processor."""
        self._is_running = True
        self.logger.info("Stream processor started")

    async def stop(self) -> None:
        """Stop the stream processor."""
        self._is_running = False
        self.logger.info(
            "Stream processor stopped",
            processed_count=self._processed_count,
            error_count=self._error_count,
        )

    def get_stats(self) -> dict[str, Any]:
        """Get processor statistics with caching for performance."""
        current_time = time.time()

        # Cache stats for 100ms to avoid repeated calculations
        if current_time - self._last_stats_update > 0.1:
            total_events = self._processed_count + self._error_count
            self._stats_cache.update(
                {
                    "is_running": self._is_running,
                    "processed_count": self._processed_count,
                    "error_count": self._error_count,
                    "success_rate": self._processed_count / max(total_events, 1),
                }
            )
            self._last_stats_update = current_time

        return self._stats_cache.copy()


class WindowManager:
    """Manages time windows for stream processing with memory optimization."""

    def __init__(self, config: StreamProcessingConfig):
        self.config = config
        self.logger = logger.bind(component="window_manager")
        self._profiler = get_performance_profiler()

        # Window storage: key -> list of windows
        self._windows: dict[str, list[Window]] = defaultdict(list)
        self._window_timers: dict[str, asyncio.Task] = {}
        self._cleanup_task: asyncio.Task | None = None
        self._is_running = False

        # Performance optimization: window pool for reuse
        self._window_pool: list[Window] = []
        self._max_pool_size = 1000

        # Memory management
        self._window_count_limit = 10000  # Prevent memory leaks
        self._last_memory_check = 0
        self._memory_check_interval = 30  # seconds

    async def start(self) -> None:
        """Start the window manager."""
        self._is_running = True

        # Start cleanup task
        self._cleanup_task = asyncio.create_task(self._cleanup_expired_windows())

        self.logger.info("Window manager started")

    async def stop(self) -> None:
        """Stop the window manager."""
        self._is_running = False

        # Cancel cleanup task
        if self._cleanup_task:
            self._cleanup_task.cancel()
            try:
                await self._cleanup_task
            except asyncio.CancelledError:
                pass

        # Cancel all window timers
        for task in self._window_timers.values():
            task.cancel()

        self._window_timers.clear()

        # Return all windows to pool before clearing
        for windows in self._windows.values():
            for window in windows:
                self._return_window_to_pool(window)

        self._windows.clear()

        self.logger.info("Window manager stopped")

    @profile_streaming_method("window_manager", "create_tumbling_window")
    def create_tumbling_window(
        self, key: str, size_ms: int, start_time: datetime | None = None
    ) -> Window:
        """Create a tumbling window with object pooling."""
        if start_time is None:
            start_time = datetime.utcnow()

        # Align to window boundaries
        start_timestamp = int(start_time.timestamp() * 1000)
        aligned_start = (start_timestamp // size_ms) * size_ms

        window_start = datetime.fromtimestamp(aligned_start / 1000)
        window_end = datetime.fromtimestamp((aligned_start + size_ms) / 1000)

        # Try to reuse window from pool
        window = self._get_pooled_window()
        if window:
            # Reset window properties
            window.start_time = window_start
            window.end_time = window_end
            window.window_type = WindowType.TUMBLING
            window.key = key
            window.events.clear()
            window.metadata = {"size_ms": size_ms}
        else:
            window = Window(
                start_time=window_start,
                end_time=window_end,
                window_type=WindowType.TUMBLING,
                key=key,
                metadata={"size_ms": size_ms},
            )

        self._windows[key].append(window)
        self._schedule_window_close(window)

        # Check memory usage periodically
        self._check_memory_usage()

        self.logger.debug(
            "Created tumbling window",
            key=key,
            start_time=window_start,
            end_time=window_end,
            size_ms=size_ms,
        )

        return window

    def create_sliding_window(
        self, key: str, size_ms: int, slide_ms: int, start_time: datetime | None = None
    ) -> list[Window]:
        """Create overlapping sliding windows."""
        if start_time is None:
            start_time = datetime.utcnow()

        windows = []
        current_time = start_time

        # Create multiple overlapping windows
        num_windows = max(1, size_ms // slide_ms)

        for i in range(num_windows):
            window_start = current_time - timedelta(milliseconds=i * slide_ms)
            window_end = window_start + timedelta(milliseconds=size_ms)

            window = Window(
                start_time=window_start,
                end_time=window_end,
                window_type=WindowType.SLIDING,
                key=f"{key}_{i}",
                metadata={"size_ms": size_ms, "slide_ms": slide_ms, "window_index": i},
            )

            windows.append(window)
            self._windows[key].append(window)
            self._schedule_window_close(window)

        self.logger.debug(
            "Created sliding windows",
            key=key,
            num_windows=len(windows),
            size_ms=size_ms,
            slide_ms=slide_ms,
        )

        return windows

    def create_session_window(
        self, key: str, session_timeout_ms: int, start_time: datetime | None = None
    ) -> Window:
        """Create a session window."""
        if start_time is None:
            start_time = datetime.utcnow()

        # Session windows are dynamically sized based on activity
        window = Window(
            start_time=start_time,
            end_time=start_time + timedelta(milliseconds=session_timeout_ms),
            window_type=WindowType.SESSION,
            key=key,
            metadata={"session_timeout_ms": session_timeout_ms},
        )

        self._windows[key].append(window)

        self.logger.debug(
            "Created session window",
            key=key,
            start_time=start_time,
            timeout_ms=session_timeout_ms,
        )

        return window

    def get_active_windows(self, key: str, event_time: datetime) -> list[Window]:
        """Get active windows for a key at given event time."""
        active_windows = []

        for window in self._windows.get(key, []):
            if window.start_time <= event_time <= window.end_time:
                active_windows.append(window)

        return active_windows

    def add_event_to_windows(self, event: EventSchema, windows: list[Window]) -> None:
        """Add an event to multiple windows."""
        for window in windows:
            window.add_event(event)

            # Extend session windows on new activity
            if window.window_type == WindowType.SESSION:
                timeout_ms = window.metadata["session_timeout_ms"]
                window.end_time = max(
                    window.end_time,
                    event.timestamp + timedelta(milliseconds=timeout_ms),
                )

    def _schedule_window_close(self, window: Window) -> None:
        """Schedule a window to be closed after its end time."""

        async def close_window():
            try:
                # Wait until window end time + allowed lateness
                delay_ms = (window.end_time - datetime.utcnow()).total_seconds() * 1000
                delay_ms += self.config.allowed_lateness_ms

                if delay_ms > 0:
                    await asyncio.sleep(delay_ms / 1000)

                # Remove window from active windows
                if window.key in self._windows:
                    try:
                        self._windows[window.key].remove(window)
                        # Return window to pool for reuse
                        self._return_window_to_pool(window)
                    except ValueError:
                        pass  # Window already removed

                self.logger.debug(
                    "Window closed",
                    key=window.key,
                    start_time=window.start_time,
                    end_time=window.end_time,
                    event_count=window.event_count,
                )

            except asyncio.CancelledError:
                pass
            except Exception as e:
                self.logger.error("Error closing window", key=window.key, error=str(e))

        task_key = f"{window.key}_{window.start_time.timestamp()}"
        self._window_timers[task_key] = asyncio.create_task(close_window())

    async def _cleanup_expired_windows(self) -> None:
        """Periodically clean up expired windows."""
        while self._is_running:
            try:
                current_time = datetime.utcnow()
                removed_count = 0

                for key in list(self._windows.keys()):
                    windows = self._windows[key]
                    expired_windows = [
                        w
                        for w in windows
                        if w.is_expired(current_time, self.config.allowed_lateness_ms)
                    ]

                    for window in expired_windows:
                        windows.remove(window)
                        # Return expired window to pool
                        self._return_window_to_pool(window)
                        removed_count += 1

                    # Remove empty key entries
                    if not windows:
                        del self._windows[key]

                if removed_count > 0:
                    self.logger.debug("Cleaned up expired windows", count=removed_count)

                # Sleep for cleanup interval
                await asyncio.sleep(self.config.checkpoint_interval_ms / 1000)

            except asyncio.CancelledError:
                break
            except Exception as e:
                self.logger.error("Error in window cleanup", error=str(e))
                await asyncio.sleep(5)  # Backoff on error

    def _get_pooled_window(self) -> Window | None:
        """Get a window from the pool for reuse."""
        if self._window_pool:
            return self._window_pool.pop()
        return None

    def _return_window_to_pool(self, window: Window) -> None:
        """Return a window to the pool for reuse."""
        if len(self._window_pool) < self._max_pool_size:
            # Clear window data to prevent memory leaks
            window.events.clear()
            window.metadata.clear()
            self._window_pool.append(window)

    def _check_memory_usage(self) -> None:
        """Check and manage memory usage."""
        current_time = time.time()
        if current_time - self._last_memory_check < self._memory_check_interval:
            return

        self._last_memory_check = current_time

        # Count total windows
        total_windows = sum(len(windows) for windows in self._windows.values())

        # Proactive cleanup at 80% of limit to prevent hitting hard limit
        proactive_limit = int(self._window_count_limit * 0.8)

        if total_windows > proactive_limit:
            cleanup_type = (
                "proactive"
                if total_windows <= self._window_count_limit
                else "emergency"
            )
            self.logger.info(
                f"Running {cleanup_type} window cleanup",
                total_windows=total_windows,
                proactive_limit=proactive_limit,
                hard_limit=self._window_count_limit,
            )
            # Force cleanup of oldest windows
            self._force_window_cleanup()

    def _force_window_cleanup(self) -> None:
        """Force cleanup of oldest windows to prevent memory issues."""
        current_time = datetime.utcnow()
        removed_count = 0

        for key in list(self._windows.keys()):
            windows = self._windows[key]
            # Remove windows older than 2x their allowed lateness
            cleanup_threshold = self.config.allowed_lateness_ms * 2

            windows_to_remove = [
                w
                for w in windows
                if (current_time - w.end_time).total_seconds() * 1000
                > cleanup_threshold
            ]

            for window in windows_to_remove:
                windows.remove(window)
                self._return_window_to_pool(window)
                removed_count += 1

            # Remove empty key entries
            if not windows:
                del self._windows[key]

        if removed_count > 0:
            self.logger.info("Force cleaned up old windows", count=removed_count)


class StreamAggregator:
    """Performs aggregations on stream windows."""

    def __init__(self):
        self.logger = logger.bind(component="stream_aggregator")

    def aggregate(
        self,
        window: Window,
        aggregation_type: AggregationType,
        field_name: str,
        **kwargs,
    ) -> AggregationResult:
        """Perform aggregation on window data."""
        try:
            if not window.events:
                return AggregationResult(
                    window=window,
                    aggregation_type=aggregation_type,
                    field_name=field_name,
                    value=None,
                )

            # Extract field values from events
            values = self._extract_field_values(window.events, field_name)

            # Perform aggregation
            if aggregation_type == AggregationType.COUNT:
                result_value = len(window.events)
            elif aggregation_type == AggregationType.SUM:
                numeric_values: list[float] = [
                    float(v) for v in values if isinstance(v, (int, float))
                ]
                result_value = float(sum(numeric_values)) if numeric_values else 0.0
            elif aggregation_type == AggregationType.AVG:
                numeric_values = [v for v in values if isinstance(v, (int, float))]
                result_value = (
                    sum(numeric_values) / len(numeric_values) if numeric_values else 0
                )
            elif aggregation_type == AggregationType.MIN:
                numeric_values = [v for v in values if isinstance(v, (int, float))]
                result_value = min(numeric_values) if numeric_values else None
            elif aggregation_type == AggregationType.MAX:
                numeric_values = [v for v in values if isinstance(v, (int, float))]
                result_value = max(numeric_values) if numeric_values else None
            elif aggregation_type == AggregationType.DISTINCT_COUNT:
                result_value = len(set(values))
            elif aggregation_type == AggregationType.PERCENTILE:
                percentile = kwargs.get("percentile", 50)
                numeric_values = sorted(
                    [v for v in values if isinstance(v, (int, float))]
                )
                if numeric_values:
                    index = int(len(numeric_values) * percentile / 100)
                    result_value = numeric_values[min(index, len(numeric_values) - 1)]
                else:
                    result_value = None
            elif aggregation_type == AggregationType.FIRST:
                result_value = values[0] if values else None
            elif aggregation_type == AggregationType.LAST:
                result_value = values[-1] if values else None
            else:
                raise ValueError(f"Unknown aggregation type: {aggregation_type}")

            return AggregationResult(
                window=window,
                aggregation_type=aggregation_type,
                field_name=field_name,
                value=result_value,
                metadata={"value_count": len(values)},
            )

        except Exception as e:
            self.logger.error(
                "Aggregation failed",
                window_key=window.key,
                aggregation_type=aggregation_type,
                field_name=field_name,
                error=str(e),
            )
            raise

    def _extract_field_values(
        self, events: list[EventSchema], field_name: str
    ) -> list[Any]:
        """Extract field values from events."""
        values = []

        for event in events:
            try:
                # Support nested field access with dot notation
                value = event.model_dump()

                for field_part in field_name.split("."):
                    if isinstance(value, dict) and field_part in value:
                        value = value[field_part]
                    else:
                        value = None
                        break

                if value is not None:
                    values.append(value)

            except Exception as e:
                self.logger.debug(
                    "Failed to extract field value",
                    field_name=field_name,
                    event_id=event.event_id,
                    error=str(e),
                )

        return values


class WindowedProcessor(StreamProcessor):
    """Stream processor with windowing and aggregation capabilities."""

    def __init__(self, config: StreamProcessingConfig | None = None):
        super().__init__(config)
        self.window_manager = WindowManager(self.config)
        self.aggregator = StreamAggregator()
        self._aggregation_handlers: list[Callable[[AggregationResult], None]] = []
        self._window_definitions: dict[str, dict[str, Any]] = {}

    async def start(self) -> None:
        """Start the windowed processor."""
        await super().start()
        await self.window_manager.start()
        self.logger.info("Windowed processor started")

    async def stop(self) -> None:
        """Stop the windowed processor."""
        await self.window_manager.stop()
        await super().stop()
        self.logger.info("Windowed processor stopped")

    def define_window(
        self,
        key_field: str,
        window_type: WindowType,
        size_ms: int,
        slide_ms: int | None = None,
        session_timeout_ms: int | None = None,
    ) -> None:
        """Define a window for processing."""
        window_def = {
            "key_field": key_field,
            "window_type": window_type,
            "size_ms": size_ms,
            "slide_ms": slide_ms,
            "session_timeout_ms": session_timeout_ms,
        }

        window_key = f"{key_field}_{window_type.value}"
        self._window_definitions[window_key] = window_def

        self.logger.info(
            "Window definition added",
            key_field=key_field,
            window_type=window_type.value,
            size_ms=size_ms,
        )

    def add_aggregation_handler(
        self, handler: Callable[[AggregationResult], None]
    ) -> None:
        """Add a handler for aggregation results."""
        self._aggregation_handlers.append(handler)
        self.logger.info(
            "Aggregation handler added", handler_count=len(self._aggregation_handlers)
        )

    async def process_event(self, event: EventSchema) -> list[AggregationResult] | None:
        """Process an event through defined windows."""
        if not self._is_running:
            return None

        try:
            results = []

            for window_key, window_def in self._window_definitions.items():
                # Extract key value from event
                key_value = self._extract_key_value(event, window_def["key_field"])
                if key_value is None:
                    continue

                window_key_full = f"{window_key}_{key_value}"

                # Get or create appropriate windows
                if window_def["window_type"] == WindowType.TUMBLING:
                    windows = self.window_manager.get_active_windows(
                        window_key_full, event.timestamp
                    )

                    if not windows:
                        windows = [
                            self.window_manager.create_tumbling_window(
                                window_key_full, window_def["size_ms"], event.timestamp
                            )
                        ]

                elif window_def["window_type"] == WindowType.SLIDING:
                    windows = self.window_manager.create_sliding_window(
                        window_key_full,
                        window_def["size_ms"],
                        window_def["slide_ms"] or (window_def["size_ms"] // 2),
                        event.timestamp,
                    )

                elif window_def["window_type"] == WindowType.SESSION:
                    windows = self.window_manager.get_active_windows(
                        window_key_full, event.timestamp
                    )

                    if not windows:
                        windows = [
                            self.window_manager.create_session_window(
                                window_key_full,
                                window_def["session_timeout_ms"]
                                or 300000,  # 5 min default
                                event.timestamp,
                            )
                        ]

                else:
                    continue

                # Add event to windows
                self.window_manager.add_event_to_windows(event, windows)

                # Trigger aggregations for completed windows
                for window in windows:
                    if window.end_time <= datetime.utcnow():
                        # Perform default aggregations
                        count_result = self.aggregator.aggregate(
                            window, AggregationType.COUNT, "event_id"
                        )
                        results.append(count_result)

                        # Call handlers
                        await self._call_aggregation_handlers(count_result)

            self._processed_count += 1
            return results if results else None

        except Exception as e:
            self._error_count += 1
            self.logger.error(
                "Error processing event", event_id=event.event_id, error=str(e)
            )
            raise

    def _extract_key_value(self, event: EventSchema, key_field: str) -> str | None:
        """Extract key value from event for windowing."""
        try:
            event_dict = event.model_dump()

            # Support nested field access
            value = event_dict
            for field_part in key_field.split("."):
                if isinstance(value, dict) and field_part in value:
                    value = value[field_part]
                else:
                    return None

            return str(value) if value is not None else None

        except Exception:
            return None

    async def _call_aggregation_handlers(self, result: AggregationResult) -> None:
        """Call all registered aggregation handlers."""
        for handler in self._aggregation_handlers:
            try:
                if asyncio.iscoroutinefunction(handler):
                    result_coro = handler(result)
                    if result_coro is not None:
                        await result_coro
                else:
                    await asyncio.get_event_loop().run_in_executor(
                        None, handler, result
                    )
            except Exception as e:
                self.logger.error(
                    "Error in aggregation handler", handler=str(handler), error=str(e)
                )


# Convenience functions for creating processors
def create_tumbling_window_processor(
    window_size_ms: int,
    key_field: str = "source_service",
    config: StreamProcessingConfig | None = None,
) -> WindowedProcessor:
    """Create a processor with tumbling windows."""
    processor = WindowedProcessor(config)
    processor.define_window(
        key_field=key_field, window_type=WindowType.TUMBLING, size_ms=window_size_ms
    )
    return processor


def create_sliding_window_processor(
    window_size_ms: int,
    slide_ms: int,
    key_field: str = "source_service",
    config: StreamProcessingConfig | None = None,
) -> WindowedProcessor:
    """Create a processor with sliding windows."""
    processor = WindowedProcessor(config)
    processor.define_window(
        key_field=key_field,
        window_type=WindowType.SLIDING,
        size_ms=window_size_ms,
        slide_ms=slide_ms,
    )
    return processor


def create_session_window_processor(
    session_timeout_ms: int,
    key_field: str = "payload.user_id",
    config: StreamProcessingConfig | None = None,
) -> WindowedProcessor:
    """Create a processor with session windows."""
    processor = WindowedProcessor(config)
    processor.define_window(
        key_field=key_field,
        window_type=WindowType.SESSION,
        size_ms=0,  # Not used for session windows
        session_timeout_ms=session_timeout_ms,
    )
    return processor
