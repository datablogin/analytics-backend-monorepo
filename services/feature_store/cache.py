"""Caching implementation for feature store."""

import json
import logging
from datetime import datetime, timedelta
from functools import cache
from typing import Any

import redis.asyncio as redis
from redis.asyncio import Redis

logger = logging.getLogger(__name__)


class FeatureCache:
    """Feature caching implementation with Redis and in-memory fallback."""

    def __init__(
        self,
        redis_url: str | None = None,
        default_ttl: int = 300,  # 5 minutes
        max_memory_size: int = 1000,
    ) -> None:
        """Initialize feature cache."""
        self.redis_client: Redis | None = None
        self.default_ttl = default_ttl
        self.max_memory_size = max_memory_size

        # Initialize Redis if URL provided
        if redis_url:
            try:
                self.redis_client = redis.from_url(redis_url)
                logger.info("Redis cache initialized")
            except Exception as e:
                logger.warning(f"Failed to initialize Redis cache: {e}")
                self.redis_client = None

    async def get_feature_values(
        self, entity_id: str, feature_names: list[str]
    ) -> dict[str, Any]:
        """Get cached feature values for an entity."""
        cache_key = self._get_feature_values_key(entity_id, feature_names)

        # Try Redis first
        if self.redis_client:
            try:
                cached_data = await self.redis_client.get(cache_key)
                if cached_data:
                    return json.loads(cached_data)
            except Exception as e:
                logger.warning(f"Redis get error: {e}")

        # Try in-memory cache
        cached_data = self._get_memory_cache(cache_key)
        if cached_data:
            return cached_data

        return {}

    async def set_feature_values(
        self,
        entity_id: str,
        feature_names: list[str],
        values: dict[str, Any],
        ttl: int | None = None,
    ) -> None:
        """Cache feature values for an entity."""
        cache_key = self._get_feature_values_key(entity_id, feature_names)
        ttl = ttl or self.default_ttl

        cache_data = {
            "values": values,
            "timestamp": datetime.utcnow().isoformat(),
        }

        # Try Redis first
        if self.redis_client:
            try:
                await self.redis_client.setex(
                    cache_key, ttl, json.dumps(cache_data, default=str)
                )
                return
            except Exception as e:
                logger.warning(f"Redis set error: {e}")

        # Fallback to in-memory cache
        self._set_memory_cache(cache_key, cache_data, ttl)

    async def get_feature_definition(
        self, feature_name: str
    ) -> dict[str, Any] | None:
        """Get cached feature definition."""
        cache_key = self._get_feature_definition_key(feature_name)

        # Try Redis first
        if self.redis_client:
            try:
                cached_data = await self.redis_client.get(cache_key)
                if cached_data:
                    return json.loads(cached_data)
            except Exception as e:
                logger.warning(f"Redis get error: {e}")

        # Try in-memory cache
        return self._get_memory_cache(cache_key)

    async def set_feature_definition(
        self,
        feature_name: str,
        definition: dict[str, Any],
        ttl: int | None = None,
    ) -> None:
        """Cache feature definition."""
        cache_key = self._get_feature_definition_key(feature_name)
        ttl = ttl or self.default_ttl * 4  # Cache definitions longer

        # Try Redis first
        if self.redis_client:
            try:
                await self.redis_client.setex(
                    cache_key, ttl, json.dumps(definition, default=str)
                )
                return
            except Exception as e:
                logger.warning(f"Redis set error: {e}")

        # Fallback to in-memory cache
        self._set_memory_cache(cache_key, definition, ttl)

    async def invalidate_feature_values(self, entity_id: str) -> None:
        """Invalidate all cached feature values for an entity."""
        pattern = f"feature_values:{entity_id}:*"

        # Try Redis first
        if self.redis_client:
            try:
                async for key in self.redis_client.scan_iter(match=pattern):
                    await self.redis_client.delete(key)
                logger.debug(f"Invalidated Redis cache for entity: {entity_id}")
                return
            except Exception as e:
                logger.warning(f"Redis invalidation error: {e}")

        # Fallback to in-memory cache
        self._invalidate_memory_cache_pattern(pattern)

    async def invalidate_feature_definition(self, feature_name: str) -> None:
        """Invalidate cached feature definition."""
        cache_key = self._get_feature_definition_key(feature_name)

        # Try Redis first
        if self.redis_client:
            try:
                await self.redis_client.delete(cache_key)
                logger.debug(f"Invalidated Redis cache for feature: {feature_name}")
                return
            except Exception as e:
                logger.warning(f"Redis invalidation error: {e}")

        # Fallback to in-memory cache
        self._invalidate_memory_cache(cache_key)

    async def clear_all(self) -> None:
        """Clear all cached data."""
        # Try Redis first
        if self.redis_client:
            try:
                await self.redis_client.flushdb()
                logger.info("Cleared Redis cache")
                return
            except Exception as e:
                logger.warning(f"Redis clear error: {e}")

        # Clear in-memory cache
        self._clear_memory_cache()

    async def health_check(self) -> dict[str, Any]:
        """Check cache health status."""
        status = {
            "redis": {"available": False, "latency_ms": None},
            "memory": {"available": True, "size": len(self._memory_cache)},
        }

        # Check Redis
        if self.redis_client:
            try:
                start_time = datetime.utcnow()
                await self.redis_client.ping()
                latency = (datetime.utcnow() - start_time).total_seconds() * 1000
                status["redis"]["available"] = True
                status["redis"]["latency_ms"] = round(latency, 2)  # type: ignore
            except Exception as e:
                logger.warning(f"Redis health check failed: {e}")

        return status

    def _get_feature_values_key(self, entity_id: str, feature_names: list[str]) -> str:
        """Generate cache key for feature values."""
        sorted_features = sorted(feature_names)
        return f"feature_values:{entity_id}:{':'.join(sorted_features)}"

    def _get_feature_definition_key(self, feature_name: str) -> str:
        """Generate cache key for feature definition."""
        return f"feature_definition:{feature_name}"

    # In-memory cache implementation
    @cache
    def _get_memory_cache_instance(self):
        """Get or create memory cache instance."""
        return {}

    @property
    def _memory_cache(self) -> dict[str, dict[str, Any]]:
        """Get memory cache."""
        return self._get_memory_cache_instance()

    def _get_memory_cache(self, key: str) -> dict[str, Any] | None:
        """Get value from memory cache."""
        try:
            cache_entry = self._memory_cache.get(key)
            if cache_entry:
                # Check expiration
                expires_at = datetime.fromisoformat(cache_entry["expires_at"])
                if datetime.utcnow() < expires_at:
                    return cache_entry["data"]
                else:
                    # Expired, remove from cache
                    del self._memory_cache[key]
        except Exception as e:
            logger.warning(f"Memory cache get error: {e}")

        return None

    def _set_memory_cache(self, key: str, data: dict[str, Any], ttl: int) -> None:
        """Set value in memory cache with TTL."""
        try:
            # Implement simple LRU eviction if cache is full
            if len(self._memory_cache) >= self.max_memory_size:
                # Remove oldest entry
                oldest_key = next(iter(self._memory_cache))
                del self._memory_cache[oldest_key]

            expires_at = datetime.utcnow() + timedelta(seconds=ttl)
            self._memory_cache[key] = {
                "data": data,
                "expires_at": expires_at.isoformat(),
            }
        except Exception as e:
            logger.warning(f"Memory cache set error: {e}")

    def _invalidate_memory_cache(self, key: str) -> None:
        """Invalidate single key from memory cache."""
        self._memory_cache.pop(key, None)

    def _invalidate_memory_cache_pattern(self, pattern: str) -> None:
        """Invalidate keys matching pattern from memory cache."""
        # Simple pattern matching for memory cache
        pattern = pattern.replace("*", "")  # Remove wildcard
        keys_to_remove = [k for k in self._memory_cache.keys() if k.startswith(pattern)]
        for key in keys_to_remove:
            del self._memory_cache[key]

    def _clear_memory_cache(self) -> None:
        """Clear all memory cache."""
        self._memory_cache.clear()


# Default cache instance
_default_cache: FeatureCache | None = None


def get_cache(
    redis_url: str | None = None,
    default_ttl: int = 300,
    max_memory_size: int = 1000,
) -> FeatureCache:
    """Get or create default cache instance."""
    global _default_cache

    if _default_cache is None:
        _default_cache = FeatureCache(
            redis_url=redis_url,
            default_ttl=default_ttl,
            max_memory_size=max_memory_size,
        )

    return _default_cache
