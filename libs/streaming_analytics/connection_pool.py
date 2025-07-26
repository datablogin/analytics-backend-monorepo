"""Connection pooling and caching optimizations for streaming analytics."""

import asyncio
import time
from collections import defaultdict
from dataclasses import dataclass
from typing import Any, Callable, Generic, TypeVar

import structlog

logger = structlog.get_logger(__name__)

T = TypeVar("T")


@dataclass
class CacheEntry(Generic[T]):
    """Cache entry with TTL and access tracking."""

    value: T
    created_at: float
    last_accessed: float
    access_count: int = 0
    ttl_seconds: float = 300  # 5 minutes default

    @property
    def is_expired(self) -> bool:
        """Check if cache entry is expired."""
        return time.time() - self.created_at > self.ttl_seconds

    @property
    def age_seconds(self) -> float:
        """Get age of cache entry in seconds."""
        return time.time() - self.created_at

    def access(self) -> T:
        """Access the cached value and update tracking."""
        self.last_accessed = time.time()
        self.access_count += 1
        return self.value


class HighPerformanceCache(Generic[T]):
    """High-performance cache with TTL, LRU eviction, and memory management."""

    def __init__(
        self,
        max_size: int = 1000,
        default_ttl_seconds: float = 300,
        cleanup_interval_seconds: float = 60,
    ):
        self.max_size = max_size
        self.default_ttl_seconds = default_ttl_seconds
        self.cleanup_interval_seconds = cleanup_interval_seconds

        self._cache: dict[str, CacheEntry[T]] = {}
        self._access_order: list[str] = []  # For LRU tracking
        self._lock = asyncio.Lock()
        self._cleanup_task: asyncio.Task | None = None

        # Statistics
        self._hit_count = 0
        self._miss_count = 0
        self._eviction_count = 0

        self.logger = logger.bind(component="high_performance_cache")

    async def start(self) -> None:
        """Start the cache cleanup task."""
        if self._cleanup_task is None:
            self._cleanup_task = asyncio.create_task(self._cleanup_loop())
            self.logger.info("Cache cleanup task started")

    async def stop(self) -> None:
        """Stop the cache cleanup task."""
        if self._cleanup_task:
            self._cleanup_task.cancel()
            try:
                await self._cleanup_task
            except asyncio.CancelledError:
                pass
            self._cleanup_task = None
            self.logger.info("Cache cleanup task stopped")

    async def get(self, key: str) -> T | None:
        """Get value from cache."""
        async with self._lock:
            if key in self._cache:
                entry = self._cache[key]

                if entry.is_expired:
                    # Remove expired entry
                    del self._cache[key]
                    self._access_order.remove(key)
                    self._miss_count += 1
                    return None

                # Update access order for LRU
                if key in self._access_order:
                    self._access_order.remove(key)
                self._access_order.append(key)

                self._hit_count += 1
                return entry.access()

            self._miss_count += 1
            return None

    async def set(self, key: str, value: T, ttl_seconds: float | None = None) -> None:
        """Set value in cache."""
        async with self._lock:
            ttl = ttl_seconds or self.default_ttl_seconds

            # Check if we need to evict entries
            if len(self._cache) >= self.max_size and key not in self._cache:
                await self._evict_lru()

            # Create new entry
            entry = CacheEntry(
                value=value,
                created_at=time.time(),
                last_accessed=time.time(),
                ttl_seconds=ttl,
            )

            # Update cache
            if key in self._cache:
                # Update access order
                if key in self._access_order:
                    self._access_order.remove(key)

            self._cache[key] = entry
            self._access_order.append(key)

    async def delete(self, key: str) -> bool:
        """Delete value from cache."""
        async with self._lock:
            if key in self._cache:
                del self._cache[key]
                if key in self._access_order:
                    self._access_order.remove(key)
                return True
            return False

    async def clear(self) -> None:
        """Clear all cached values."""
        async with self._lock:
            self._cache.clear()
            self._access_order.clear()
            self.logger.info("Cache cleared")

    async def _evict_lru(self) -> None:
        """Evict least recently used entry."""
        if not self._access_order:
            return

        lru_key = self._access_order[0]
        del self._cache[lru_key]
        self._access_order.remove(lru_key)
        self._eviction_count += 1

        self.logger.debug("Evicted LRU entry", key=lru_key)

    async def _cleanup_loop(self) -> None:
        """Periodic cleanup of expired entries."""
        while True:
            try:
                await asyncio.sleep(self.cleanup_interval_seconds)
                await self._cleanup_expired()
            except asyncio.CancelledError:
                break
            except Exception as e:
                self.logger.error("Error in cache cleanup", error=str(e))

    async def _cleanup_expired(self) -> None:
        """Clean up expired cache entries."""
        async with self._lock:
            expired_keys = [
                key for key, entry in self._cache.items() if entry.is_expired
            ]

            for key in expired_keys:
                del self._cache[key]
                if key in self._access_order:
                    self._access_order.remove(key)

            if expired_keys:
                self.logger.debug("Cleaned up expired entries", count=len(expired_keys))

    def get_stats(self) -> dict[str, Any]:
        """Get cache statistics."""
        total_requests = self._hit_count + self._miss_count
        hit_rate = (self._hit_count / max(total_requests, 1)) * 100

        return {
            "size": len(self._cache),
            "max_size": self.max_size,
            "hit_count": self._hit_count,
            "miss_count": self._miss_count,
            "eviction_count": self._eviction_count,
            "hit_rate_percent": hit_rate,
            "memory_usage_estimate_mb": self._estimate_memory_usage(),
        }

    def _estimate_memory_usage(self) -> float:
        """Estimate memory usage in MB (rough approximation)."""
        import sys

        try:
            total_size = sys.getsizeof(self._cache)
            total_size += sys.getsizeof(self._access_order)

            # Estimate size of stored objects
            if self._cache:
                sample_entry = next(iter(self._cache.values()))
                entry_size = sys.getsizeof(sample_entry) + sys.getsizeof(
                    sample_entry.value
                )
                total_size += entry_size * len(self._cache)

            return total_size / (1024 * 1024)  # Convert to MB
        except Exception:
            return 0.0


class ConnectionPool:
    """High-performance connection pool for streaming analytics."""

    def __init__(
        self,
        max_connections: int = 100,
        min_connections: int = 10,
        connection_timeout_seconds: float = 30,
        idle_timeout_seconds: float = 300,
    ):
        self.max_connections = max_connections
        self.min_connections = min_connections
        self.connection_timeout_seconds = connection_timeout_seconds
        self.idle_timeout_seconds = idle_timeout_seconds

        self._available_connections: asyncio.Queue = asyncio.Queue(
            maxsize=max_connections
        )
        self._in_use_connections: set[Any] = set()
        self._connection_factory: Callable[[], Any] | None = None
        self._connection_count = 0
        self._lock = asyncio.Lock()

        # Statistics
        self._connections_created = 0
        self._connections_reused = 0
        self._connections_closed = 0

        self.logger = logger.bind(component="connection_pool")

    def set_connection_factory(self, factory: Callable[[], Any]) -> None:
        """Set the factory function for creating new connections."""
        self._connection_factory = factory

    async def get_connection(self) -> Any:
        """Get a connection from the pool."""
        if not self._connection_factory:
            raise RuntimeError("Connection factory not set")

        # Try to get an available connection
        try:
            connection = await asyncio.wait_for(
                self._available_connections.get(),
                timeout=0.1,  # Quick check
            )

            # Validate connection is still good
            if await self._validate_connection(connection):
                async with self._lock:
                    self._in_use_connections.add(connection)
                self._connections_reused += 1
                return connection
            else:
                # Connection is bad, close it
                await self._close_connection(connection)

        except TimeoutError:
            pass  # No available connections

        # Create new connection if under limit
        async with self._lock:
            if self._connection_count < self.max_connections:
                connection = await self._create_connection()
                self._in_use_connections.add(connection)
                return connection

        # Wait for available connection (blocking)
        connection = await asyncio.wait_for(
            self._available_connections.get(), timeout=self.connection_timeout_seconds
        )

        async with self._lock:
            self._in_use_connections.add(connection)

        self._connections_reused += 1
        return connection

    async def return_connection(self, connection: Any) -> None:
        """Return a connection to the pool."""
        async with self._lock:
            if connection in self._in_use_connections:
                self._in_use_connections.remove(connection)
            else:
                # Connection not tracked, close it
                await self._close_connection(connection)
                return

        # Validate connection before returning to pool
        if await self._validate_connection(connection):
            try:
                await self._available_connections.put(connection)
            except asyncio.QueueFull:
                # Pool is full, close excess connection
                await self._close_connection(connection)
        else:
            # Connection is bad, close it
            await self._close_connection(connection)

    async def _create_connection(self) -> Any:
        """Create a new connection."""
        connection = await self._connection_factory()
        self._connection_count += 1
        self._connections_created += 1

        self.logger.debug(
            "Created new connection",
            total_connections=self._connection_count,
            max_connections=self.max_connections,
        )

        return connection

    async def _validate_connection(self, connection: Any) -> bool:
        """Validate that a connection is still usable."""
        try:
            # Basic validation - connection should have required methods
            if hasattr(connection, "ping"):
                return await connection.ping()
            elif hasattr(connection, "is_connected"):
                return connection.is_connected()
            else:
                # Assume connection is valid if no validation method
                return True
        except Exception as e:
            self.logger.debug("Connection validation failed", error=str(e))
            return False

    async def _close_connection(self, connection: Any) -> None:
        """Close a connection and update counters."""
        try:
            if hasattr(connection, "close"):
                await connection.close()
            elif hasattr(connection, "disconnect"):
                await connection.disconnect()
        except Exception as e:
            self.logger.debug("Error closing connection", error=str(e))

        async with self._lock:
            self._connection_count -= 1
            self._connections_closed += 1

    async def close_all(self) -> None:
        """Close all connections in the pool."""
        async with self._lock:
            # Close all available connections
            while not self._available_connections.empty():
                try:
                    connection = await asyncio.wait_for(
                        self._available_connections.get(), timeout=0.1
                    )
                    await self._close_connection(connection)
                except TimeoutError:
                    break

            # Close all in-use connections (they will be closed when returned)
            for connection in list(self._in_use_connections):
                await self._close_connection(connection)
                self._in_use_connections.remove(connection)

        self.logger.info("All connections closed")

    def get_stats(self) -> dict[str, Any]:
        """Get connection pool statistics."""
        return {
            "total_connections": self._connection_count,
            "available_connections": self._available_connections.qsize(),
            "in_use_connections": len(self._in_use_connections),
            "max_connections": self.max_connections,
            "min_connections": self.min_connections,
            "connections_created": self._connections_created,
            "connections_reused": self._connections_reused,
            "connections_closed": self._connections_closed,
            "pool_utilization_percent": (
                len(self._in_use_connections) / self.max_connections
            )
            * 100,
        }


class StreamingMetricsCache:
    """Specialized cache for streaming metrics with aggregation."""

    def __init__(self, window_size_seconds: int = 60):
        self.window_size_seconds = window_size_seconds
        self._metrics: dict[str, list[tuple[float, Any]]] = defaultdict(list)
        self._aggregated_cache: dict[str, Any] = {}
        self._last_aggregation = 0
        self._lock = asyncio.Lock()

        self.logger = logger.bind(component="streaming_metrics_cache")

    async def add_metric(
        self, metric_name: str, value: Any, timestamp: float | None = None
    ) -> None:
        """Add a metric value to the cache."""
        if timestamp is None:
            timestamp = time.time()

        async with self._lock:
            self._metrics[metric_name].append((timestamp, value))

            # Clean up old metrics outside the window
            cutoff_time = timestamp - self.window_size_seconds
            self._metrics[metric_name] = [
                (t, v) for t, v in self._metrics[metric_name] if t >= cutoff_time
            ]

    async def get_aggregated_metrics(
        self, force_refresh: bool = False
    ) -> dict[str, Any]:
        """Get aggregated metrics for the current window."""
        current_time = time.time()

        # Refresh cache every 5 seconds or on force refresh
        if force_refresh or current_time - self._last_aggregation > 5:
            async with self._lock:
                await self._refresh_aggregated_cache(current_time)
                self._last_aggregation = current_time

        return self._aggregated_cache.copy()

    async def _refresh_aggregated_cache(self, current_time: float) -> None:
        """Refresh the aggregated metrics cache."""
        cutoff_time = current_time - self.window_size_seconds
        aggregated = {}

        for metric_name, values in self._metrics.items():
            # Filter to current window
            window_values = [v for t, v in values if t >= cutoff_time]

            if not window_values:
                continue

            # Try to aggregate numeric values
            try:
                numeric_values = [float(v) for v in window_values]
                aggregated[metric_name] = {
                    "count": len(numeric_values),
                    "sum": sum(numeric_values),
                    "avg": sum(numeric_values) / len(numeric_values),
                    "min": min(numeric_values),
                    "max": max(numeric_values),
                    "latest": numeric_values[-1],
                }
            except (ValueError, TypeError):
                # Non-numeric values, just store count and latest
                aggregated[metric_name] = {
                    "count": len(window_values),
                    "latest": window_values[-1],
                }

        self._aggregated_cache = aggregated


# Global instances for easy access
_global_cache: HighPerformanceCache[Any] | None = None
_global_connection_pool: ConnectionPool | None = None
_global_metrics_cache: StreamingMetricsCache | None = None


async def get_global_cache() -> HighPerformanceCache[Any]:
    """Get the global high-performance cache instance."""
    global _global_cache

    if _global_cache is None:
        _global_cache = HighPerformanceCache(max_size=10000, default_ttl_seconds=300)
        await _global_cache.start()

    return _global_cache


async def get_global_connection_pool() -> ConnectionPool:
    """Get the global connection pool instance."""
    global _global_connection_pool

    if _global_connection_pool is None:
        _global_connection_pool = ConnectionPool(
            max_connections=100,
            min_connections=10,
            connection_timeout_seconds=30,
        )

    return _global_connection_pool


async def get_global_metrics_cache() -> StreamingMetricsCache:
    """Get the global streaming metrics cache instance."""
    global _global_metrics_cache

    if _global_metrics_cache is None:
        _global_metrics_cache = StreamingMetricsCache(
            window_size_seconds=300
        )  # 5 minutes

    return _global_metrics_cache


async def shutdown_global_caches() -> None:
    """Shutdown all global cache instances."""
    global _global_cache, _global_connection_pool, _global_metrics_cache

    if _global_cache:
        await _global_cache.stop()
        _global_cache = None

    if _global_connection_pool:
        await _global_connection_pool.close_all()
        _global_connection_pool = None

    if _global_metrics_cache:
        _global_metrics_cache = None
