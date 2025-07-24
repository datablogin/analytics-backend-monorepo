"""
Query result caching system for data warehouses.

This module provides a caching layer to improve performance
by storing and reusing query results.
"""

import hashlib
import json
from datetime import datetime, timedelta
from enum import Enum
from typing import Any

from pydantic import BaseModel

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
    max_entries: int = 1000
    max_size_mb: int = 100
    default_ttl_seconds: int = 3600  # 1 hour
    enable_compression: bool = True
    cache_threshold_ms: int = 1000  # Only cache queries that take > 1s

    # LRU specific
    lru_eviction_ratio: float = 0.1  # Evict 10% when full

    # Size-based specific
    max_entry_size_mb: int = 10

    # TTL specific
    min_ttl_seconds: int = 300  # 5 minutes minimum
    max_ttl_seconds: int = 86400  # 24 hours maximum


class QueryCache:
    """
    Query result cache with multiple eviction strategies.

    Provides intelligent caching of query results to improve
    performance for repeated or similar queries.
    """

    def __init__(self, config: CacheConfig | None = None):
        """Initialize query cache with configuration."""
        self.config = config or CacheConfig()
        self._cache: dict[str, CacheEntry] = {}
        self._access_order: list[str] = []  # For LRU
        self._total_size_bytes = 0

    def _generate_query_hash(
        self, query: str, params: dict[str, Any] | None = None
    ) -> str:
        """Generate a hash for query + parameters."""
        # Normalize query (remove extra whitespace, convert to lowercase)
        normalized_query = " ".join(query.strip().lower().split())

        # Include parameters in hash
        params_str = ""
        if params:
            params_str = json.dumps(params, sort_keys=True)

        # Create hash
        content = f"{normalized_query}|{params_str}"
        return hashlib.sha256(content.encode()).hexdigest()

    def _estimate_size_bytes(self, result: QueryResult) -> int:
        """Estimate memory size of query result with improved accuracy."""
        # For large results, avoid JSON serialization which is expensive
        if result.total_rows > 1000:
            # Use fast approximation for large results
            column_overhead = len(result.columns) * COLUMN_METADATA_OVERHEAD_BYTES
            data_overhead = len(result.data) * len(result.columns) * DATA_CELL_OVERHEAD_BYTES

            # Estimate average cell size based on sample
            if result.data:
                sample_rows = min(10, len(result.data))
                sample_size = sum(
                    len(str(cell)) for row in result.data[:sample_rows]
                    for cell in row if cell is not None
                )
                avg_cell_size = sample_size / (sample_rows * len(result.columns))
                estimated_data_size = int(avg_cell_size * len(result.data) * len(result.columns))
            else:
                estimated_data_size = 0

            return int((BASE_RESULT_OVERHEAD_BYTES + column_overhead + data_overhead + estimated_data_size) * JSON_ENCODING_OVERHEAD_MULTIPLIER)
        else:
            # Use accurate measurement for smaller results
            base_size = len(result.model_dump_json().encode())
            overhead = len(result.columns) * COLUMN_METADATA_OVERHEAD_BYTES
            overhead += len(result.data) * len(result.columns) * DATA_CELL_OVERHEAD_BYTES
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

    def _evict_fifo(self) -> None:
        """Evict oldest entries first."""
        num_to_evict = max(1, int(len(self._cache) * self.config.lru_eviction_ratio))

        # Sort by creation time
        entries_by_creation = sorted(self._cache.items(), key=lambda x: x[1].created_at)

        for query_hash, entry in entries_by_creation[:num_to_evict]:
            self._remove_entry(query_hash)

    def _evict_by_size(self) -> None:
        """Evict largest entries to make room."""
        # Sort by size, largest first
        entries_by_size = sorted(
            self._cache.items(), key=lambda x: x[1].size_bytes, reverse=True
        )

        # Evict until we're under size limit
        max_size = self.config.max_size_mb * 1024 * 1024
        current_size = self._total_size_bytes

        for query_hash, entry in entries_by_size:
            if current_size <= max_size:
                break

            self._remove_entry(query_hash)
            current_size -= entry.size_bytes

    def _evict_expired(self) -> None:
        """Remove expired entries."""
        expired_hashes = []

        for query_hash, entry in self._cache.items():
            if entry.is_expired():
                expired_hashes.append(query_hash)

        for query_hash in expired_hashes:
            self._remove_entry(query_hash)

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
        Get cached query result if available.

        Args:
            query: SQL query string
            params: Optional query parameters

        Returns:
            QueryResult if cached, None otherwise
        """
        query_hash = self._generate_query_hash(query, params)

        if query_hash not in self._cache:
            return None

        entry = self._cache[query_hash]

        # Check if entry is expired
        if entry.is_expired():
            self._remove_entry(query_hash)
            return None

        # Update access statistics
        entry.update_access()

        # Update LRU order
        if query_hash in self._access_order:
            self._access_order.remove(query_hash)
        self._access_order.append(query_hash)

        # Mark as cache hit
        entry.result.metadata.cache_hit = True

        return entry.result

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
        """Get cache statistics."""
        now = datetime.now()

        # Calculate hit ratio (would need to track misses in production)
        total_hits = sum(entry.access_count for entry in self._cache.values())

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
        expired_count = sum(1 for entry in self._cache.values() if entry.is_expired())

        return {
            "total_entries": len(self._cache),
            "total_size_mb": self._total_size_bytes / (1024 * 1024),
            "max_entries": self.config.max_entries,
            "max_size_mb": self.config.max_size_mb,
            "total_hits": total_hits,
            "avg_age_seconds": avg_age_seconds,
            "expired_entries": expired_count,
            "eviction_strategy": self.config.strategy,
            "memory_utilization": self._total_size_bytes
            / (self.config.max_size_mb * 1024 * 1024),
            "entry_utilization": len(self._cache) / self.config.max_entries,
        }

    def cleanup(self) -> int:
        """
        Perform maintenance cleanup of cache.

        Returns:
            int: Number of entries removed
        """
        initial_count = len(self._cache)
        self._evict_expired()
        return initial_count - len(self._cache)
