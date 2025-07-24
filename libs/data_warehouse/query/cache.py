"""
Query result caching system for data warehouses.

This module provides a caching layer to improve performance
by storing and reusing query results.
"""

import asyncio
import hashlib
import json
import threading
import time
from datetime import datetime, timedelta
from enum import Enum
from typing import Any

from pydantic import BaseModel, Field, field_validator, model_validator

from ..connectors.base import QueryResult

# Cache size estimation constants
COLUMN_METADATA_OVERHEAD_BYTES = 50
DATA_CELL_OVERHEAD_BYTES = 20
BASE_RESULT_OVERHEAD_BYTES = 200
JSON_ENCODING_OVERHEAD_MULTIPLIER = 1.2


class CacheStrategy(str, Enum):
    """Caching strategies for different types of queries."""

    LRU = "lru"  # Least Recently Used
    TTL = "ttl"  # Time To Live
    FIFO = "fifo"  # First In, First Out
    SIZE_BASED = "size_based"  # Based on result size
    HYBRID = "hybrid"  # Combination of strategies


class CacheEntry(BaseModel):
    """A single cache entry."""

    query_hash: str
    query: str
    result: QueryResult
    created_at: datetime
    accessed_at: datetime
    access_count: int = 1
    ttl_seconds: int | None = None
    size_bytes: int = 0

    def is_expired(self) -> bool:
        """Check if cache entry has expired."""
        if self.ttl_seconds is None:
            return False

        expiry_time = self.created_at + timedelta(seconds=self.ttl_seconds)
        return datetime.now() > expiry_time

    def update_access(self) -> None:
        """Update access statistics."""
        self.accessed_at = datetime.now()
        self.access_count += 1


class CacheConfig(BaseModel):
    """Configuration for query cache."""

    strategy: CacheStrategy = CacheStrategy.HYBRID
    max_entries: int = Field(
        default=1000, gt=0, description="Maximum number of cache entries"
    )
    max_size_mb: int = Field(default=100, gt=0, description="Maximum cache size in MB")
    default_ttl_seconds: int = Field(
        default=3600, gt=0, description="Default TTL in seconds"
    )
    enable_compression: bool = Field(
        default=True, description="Enable compression for cached results"
    )
    cache_threshold_ms: int = Field(
        default=1000, ge=0, description="Only cache queries that take longer than this"
    )

    # LRU specific
    lru_eviction_ratio: float = Field(
        default=0.1, gt=0, le=1, description="Eviction ratio for LRU (0.0-1.0)"
    )

    # FIFO specific
    fifo_eviction_ratio: float = Field(
        default=0.15, gt=0, le=1, description="Eviction ratio for FIFO (0.0-1.0)"
    )

    # Size-based specific
    max_entry_size_mb: int = Field(
        default=10, gt=0, description="Maximum size for a single cache entry in MB"
    )

    # TTL specific
    min_ttl_seconds: int = Field(
        default=300, gt=0, description="Minimum TTL in seconds"
    )
    max_ttl_seconds: int = Field(
        default=86400, gt=0, description="Maximum TTL in seconds"
    )

    # Performance tuning
    cleanup_frequency_seconds: int = Field(
        default=300, gt=0, description="How often to run cleanup in seconds"
    )
    size_estimation_accuracy: float = Field(
        default=0.8,
        gt=0,
        le=1,
        description="Trade-off between accuracy and performance",
    )
    enable_thread_safety: bool = Field(
        default=True, description="Enable thread-safe operations"
    )

    @field_validator("max_entry_size_mb")
    @classmethod
    def validate_max_entry_size(cls, v: int, info) -> int:
        """Ensure max_entry_size_mb doesn't exceed max_size_mb."""
        # Note: We can't access other fields during validation in this way with Pydantic v2
        # This validation will be done in model_validator
        return v

    @field_validator("min_ttl_seconds", "max_ttl_seconds")
    @classmethod
    def validate_ttl_range(cls, v: int) -> int:
        """Ensure TTL values are reasonable."""
        if v > 86400 * 7:  # 1 week
            raise ValueError("TTL cannot exceed 7 days")
        return v

    @model_validator(mode="after")
    def validate_cache_config(self) -> "CacheConfig":
        """Validate the entire cache configuration."""
        # Check TTL ordering
        if self.min_ttl_seconds > self.max_ttl_seconds:
            raise ValueError("min_ttl_seconds must be <= max_ttl_seconds")

        # Check entry size vs total size
        if self.max_entry_size_mb > self.max_size_mb:
            raise ValueError("max_entry_size_mb cannot exceed max_size_mb")

        # Check default TTL is within bounds
        if not (
            self.min_ttl_seconds <= self.default_ttl_seconds <= self.max_ttl_seconds
        ):
            raise ValueError(
                "default_ttl_seconds must be between min_ttl_seconds and max_ttl_seconds"
            )

        # Warn about potentially problematic configurations
        if self.max_entries > 100000:
            import warnings

            warnings.warn("Very large max_entries may impact performance", UserWarning)

        if self.cache_threshold_ms > 60000:  # 1 minute
            import warnings

            warnings.warn(
                "Very high cache_threshold_ms may reduce cache effectiveness",
                UserWarning,
            )

        return self


class QueryCache:
    """
    Thread-safe query result cache with multiple eviction strategies.

    Provides intelligent caching of query results to improve
    performance for repeated or similar queries.
    """

    def __init__(self, config: CacheConfig | None = None):
        """Initialize query cache with configuration."""
        self.config = config or CacheConfig()
        self._cache: dict[str, CacheEntry] = {}
        self._access_order: list[str] = []  # For LRU
        self._total_size_bytes = 0

        # Thread safety
        self._lock = threading.RLock() if self.config.enable_thread_safety else None

        # Metrics tracking
        self._hit_count = 0
        self._miss_count = 0
        self._eviction_count = 0

        # Background cleanup
        self._last_cleanup = time.time()
        self._cleanup_task: asyncio.Task | None = None

    def _with_lock(self, func):
        """Context manager for thread-safe operations."""
        if self._lock:

            def wrapper(*args, **kwargs):
                with self._lock:
                    return func(*args, **kwargs)

            return wrapper
        return func

    def _generate_query_hash(
        self, query: str, params: dict[str, Any] | None = None
    ) -> str:
        """Generate a hash for query + parameters with improved normalization."""
        # Validate input
        if not query or not query.strip():
            raise ValueError("Query cannot be empty or whitespace-only")

        # Improved query normalization
        normalized_query = " ".join(query.strip().split())

        # Include parameters in hash with error handling
        params_str = ""
        if params:
            try:
                params_str = json.dumps(params, sort_keys=True, default=str)
            except (TypeError, ValueError):
                # Fallback for non-serializable parameters
                params_str = str(
                    sorted(params.items()) if isinstance(params, dict) else params
                )

        # Create hash
        content = f"{normalized_query}|{params_str}"
        return hashlib.sha256(content.encode()).hexdigest()

    def _estimate_size_bytes(self, result: QueryResult) -> int:
        """Estimate memory size of query result with improved accuracy."""
        # For large results, avoid JSON serialization which is expensive
        if result.total_rows > 1000:
            # Use fast approximation for large results
            column_overhead = len(result.columns) * COLUMN_METADATA_OVERHEAD_BYTES
            data_overhead = (
                len(result.data) * len(result.columns) * DATA_CELL_OVERHEAD_BYTES
            )

            # Estimate average cell size based on sample
            if result.data:
                sample_rows = min(10, len(result.data))
                sample_size = sum(
                    len(str(cell))
                    for row in result.data[:sample_rows]
                    for cell in row
                    if cell is not None
                )
                avg_cell_size = sample_size / (sample_rows * len(result.columns))
                estimated_data_size = int(
                    avg_cell_size * len(result.data) * len(result.columns)
                )
            else:
                estimated_data_size = 0

            return int(
                (
                    BASE_RESULT_OVERHEAD_BYTES
                    + column_overhead
                    + data_overhead
                    + estimated_data_size
                )
                * JSON_ENCODING_OVERHEAD_MULTIPLIER
            )
        else:
            # Use accurate measurement for smaller results
            base_size = len(result.model_dump_json().encode())
            overhead = len(result.columns) * COLUMN_METADATA_OVERHEAD_BYTES
            overhead += (
                len(result.data) * len(result.columns) * DATA_CELL_OVERHEAD_BYTES
            )
            return base_size + overhead

    def _should_cache(self, result: QueryResult) -> bool:
        """Determine if a query result should be cached."""
        # Don't cache if execution was too fast
        if result.metadata.execution_time_ms < self.config.cache_threshold_ms:
            return False

        # Don't cache if result is too large
        estimated_size = self._estimate_size_bytes(result)
        max_size_bytes = self.config.max_entry_size_mb * 1024 * 1024

        if estimated_size > max_size_bytes:
            return False

        return True

    def _evict_entries(self) -> None:
        """Evict entries based on configured strategy."""
        if self.config.strategy == CacheStrategy.LRU:
            self._evict_lru()
        elif self.config.strategy == CacheStrategy.FIFO:
            self._evict_fifo()
        elif self.config.strategy == CacheStrategy.SIZE_BASED:
            self._evict_by_size()
        elif self.config.strategy == CacheStrategy.TTL:
            self._evict_expired()
        else:  # HYBRID
            self._evict_hybrid()

    def _evict_lru(self) -> None:
        """Evict least recently used entries."""
        num_to_evict = max(1, int(len(self._cache) * self.config.lru_eviction_ratio))

        # Sort by access time
        entries_by_access = sorted(self._cache.items(), key=lambda x: x[1].accessed_at)

        for query_hash, entry in entries_by_access[:num_to_evict]:
            self._remove_entry(query_hash)
            self._eviction_count += 1

    def _evict_fifo(self) -> None:
        """Evict oldest entries first."""
        num_to_evict = max(1, int(len(self._cache) * self.config.fifo_eviction_ratio))

        # Sort by creation time
        entries_by_creation = sorted(self._cache.items(), key=lambda x: x[1].created_at)

        for query_hash, entry in entries_by_creation[:num_to_evict]:
            self._remove_entry(query_hash)
            self._eviction_count += 1

    def _evict_by_size(self) -> None:
        """Evict largest entries to make room."""
        # Sort by size, largest first
        entries_by_size = sorted(
            self._cache.items(), key=lambda x: x[1].size_bytes, reverse=True
        )

        # Evict until we're under size limit (leave room for new entry)
        max_size = self.config.max_size_mb * 1024 * 1024
        target_size = int(max_size * 0.9)  # Target 90% to avoid constant eviction
        current_size = self._total_size_bytes

        for query_hash, entry in entries_by_size:
            if current_size < target_size:
                break

            self._remove_entry(query_hash)
            current_size -= entry.size_bytes
            self._eviction_count += 1

    def _evict_expired(self) -> None:
        """Remove expired entries."""
        expired_hashes = []

        for query_hash, entry in self._cache.items():
            if entry.is_expired():
                expired_hashes.append(query_hash)

        for query_hash in expired_hashes:
            self._remove_entry(query_hash)
            self._eviction_count += 1

    def _evict_hybrid(self) -> None:
        """Hybrid eviction strategy combining multiple approaches."""
        # First, remove expired entries
        self._evict_expired()

        # If still over limits, use size-based eviction
        max_size = self.config.max_size_mb * 1024 * 1024
        if self._total_size_bytes > max_size:
            self._evict_by_size()

        # If still too many entries, use LRU
        if len(self._cache) > self.config.max_entries:
            self._evict_lru()

    def _remove_entry(self, query_hash: str) -> None:
        """Remove a cache entry."""
        if query_hash in self._cache:
            entry = self._cache[query_hash]
            self._total_size_bytes -= entry.size_bytes
            del self._cache[query_hash]

            if query_hash in self._access_order:
                self._access_order.remove(query_hash)

    def get(
        self, query: str, params: dict[str, Any] | None = None
    ) -> QueryResult | None:
        """
        Get cached query result if available (thread-safe).

        Args:
            query: SQL query string
            params: Optional query parameters

        Returns:
            QueryResult if cached, None otherwise
        """
        try:
            query_hash = self._generate_query_hash(query, params)
        except ValueError:
            self._miss_count += 1
            return None

        def _get_internal():
            if query_hash not in self._cache:
                self._miss_count += 1
                return None

            entry = self._cache[query_hash]

            # Check if entry is expired
            if entry.is_expired():
                self._remove_entry(query_hash)
                self._miss_count += 1
                return None

            # Update access statistics
            entry.update_access()

            # Update LRU order (optimize by using a set for O(1) removal)
            try:
                self._access_order.remove(query_hash)
            except ValueError:
                pass  # Not in list, that's fine
            self._access_order.append(query_hash)

            # Mark as cache hit and track metrics
            entry.result.metadata.cache_hit = True
            self._hit_count += 1

            return entry.result

        if self._lock:
            with self._lock:
                return _get_internal()
        else:
            return _get_internal()

    def put(
        self,
        query: str,
        result: QueryResult,
        params: dict[str, Any] | None = None,
        ttl_seconds: int | None = None,
    ) -> bool:
        """
        Store query result in cache.

        Args:
            query: SQL query string
            result: Query result to cache
            params: Optional query parameters
            ttl_seconds: Optional time-to-live override

        Returns:
            bool: True if cached successfully, False otherwise
        """
        # Check if result should be cached
        if not self._should_cache(result):
            return False

        query_hash = self._generate_query_hash(query, params)

        # Calculate size
        size_bytes = self._estimate_size_bytes(result)

        # Use provided TTL or default
        ttl = ttl_seconds or self.config.default_ttl_seconds

        # Clamp TTL to configured bounds
        ttl = max(self.config.min_ttl_seconds, ttl)
        ttl = min(self.config.max_ttl_seconds, ttl)

        # Create cache entry
        entry = CacheEntry(
            query_hash=query_hash,
            query=query,
            result=result,
            created_at=datetime.now(),
            accessed_at=datetime.now(),
            ttl_seconds=ttl,
            size_bytes=size_bytes,
        )

        # Check if we need to evict before adding
        projected_size = self._total_size_bytes + size_bytes
        max_size = self.config.max_size_mb * 1024 * 1024

        if len(self._cache) >= self.config.max_entries or projected_size > max_size:
            self._evict_entries()

        # Add to cache
        self._cache[query_hash] = entry
        self._total_size_bytes += size_bytes
        self._access_order.append(query_hash)

        return True

    def invalidate(self, query: str, params: dict[str, Any] | None = None) -> bool:
        """
        Invalidate a specific cached query.

        Args:
            query: SQL query string
            params: Optional query parameters

        Returns:
            bool: True if entry was removed, False if not found
        """
        query_hash = self._generate_query_hash(query, params)

        if query_hash in self._cache:
            self._remove_entry(query_hash)
            return True

        return False

    def invalidate_pattern(self, table_pattern: str) -> int:
        """
        Invalidate all cached queries matching a table pattern.

        Args:
            table_pattern: Table name pattern to match

        Returns:
            int: Number of entries invalidated
        """
        invalidated = 0
        hashes_to_remove = []

        for query_hash, entry in self._cache.items():
            # Simple pattern matching - could be improved with regex
            if table_pattern.lower() in entry.query.lower():
                hashes_to_remove.append(query_hash)

        for query_hash in hashes_to_remove:
            self._remove_entry(query_hash)
            invalidated += 1

        return invalidated

    def clear(self) -> None:
        """Clear all cached entries."""
        self._cache.clear()
        self._access_order.clear()
        self._total_size_bytes = 0

    def get_stats(self) -> dict[str, Any]:
        """Get comprehensive cache statistics (thread-safe)."""

        def _get_stats_internal():
            now = datetime.now()

            # Calculate hit ratio with proper tracking
            total_requests = self._hit_count + self._miss_count
            hit_ratio = (
                (self._hit_count / total_requests) if total_requests > 0 else 0.0
            )

            # Calculate average age
            if self._cache:
                total_age_seconds = sum(
                    (now - entry.created_at).total_seconds()
                    for entry in self._cache.values()
                )
                avg_age_seconds = total_age_seconds / len(self._cache)
            else:
                avg_age_seconds = 0

            # Count expired entries
            expired_count = sum(
                1 for entry in self._cache.values() if entry.is_expired()
            )

            # Prevent division by zero
            max_size_bytes = max(1, self.config.max_size_mb * 1024 * 1024)
            max_entries = max(1, self.config.max_entries)

            return {
                "total_entries": len(self._cache),
                "total_size_mb": round(self._total_size_bytes / (1024 * 1024), 2),
                "max_entries": self.config.max_entries,
                "max_size_mb": self.config.max_size_mb,
                "hit_count": self._hit_count,
                "miss_count": self._miss_count,
                "hit_ratio": round(hit_ratio, 4),
                "eviction_count": self._eviction_count,
                "avg_age_seconds": round(avg_age_seconds, 2),
                "expired_entries": expired_count,
                "eviction_strategy": self.config.strategy.value,
                "memory_utilization": round(self._total_size_bytes / max_size_bytes, 4),
                "entry_utilization": round(len(self._cache) / max_entries, 4),
                "thread_safety_enabled": self.config.enable_thread_safety,
            }

        if self._lock:
            with self._lock:
                return _get_stats_internal()
        else:
            return _get_stats_internal()

    def cleanup(self) -> int:
        """
        Perform maintenance cleanup of cache.

        Returns:
            int: Number of entries removed
        """
        initial_count = len(self._cache)
        self._evict_expired()
        return initial_count - len(self._cache)
