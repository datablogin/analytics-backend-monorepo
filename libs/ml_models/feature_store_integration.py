"""Integration between ML models and feature store for real-time features."""

import asyncio
from collections.abc import Callable
from datetime import datetime, timezone
from typing import Any

import structlog
from pydantic import BaseModel, Field

from ..observability.metrics import MLOpsMetrics
from .config import BaseConfig

logger = structlog.get_logger(__name__)


class FeatureStoreConfig(BaseConfig):
    """Configuration for feature store integration."""

    # Feature serving settings
    feature_serving_endpoint: str = Field(
        default="http://localhost:8000/api/v1/features",
        description="Feature store serving endpoint",
    )
    feature_cache_ttl_seconds: int = Field(
        default=300, description="Feature cache TTL in seconds"
    )
    batch_feature_fetch_size: int = Field(
        default=100, description="Batch size for feature fetching"
    )

    # Feature freshness
    max_feature_age_seconds: int = Field(
        default=3600, description="Maximum acceptable feature age"
    )
    enable_feature_freshness_check: bool = Field(
        default=True, description="Enable feature freshness validation"
    )

    # Real-time feature computation
    enable_real_time_features: bool = Field(
        default=True, description="Enable real-time feature computation"
    )
    real_time_feature_timeout_ms: int = Field(
        default=500, description="Timeout for real-time feature computation"
    )

    # Error handling
    feature_fetch_retry_count: int = Field(
        default=3, description="Number of retries for feature fetching"
    )
    feature_fetch_timeout_seconds: int = Field(
        default=5, description="Timeout for feature fetching"
    )
    fallback_to_cache_on_error: bool = Field(
        default=True, description="Fallback to cache when feature fetch fails"
    )


class FeatureRequest(BaseModel):
    """Request for features from feature store."""

    entity_ids: list[str] = Field(description="Entity identifiers")
    feature_names: list[str] = Field(description="Requested feature names")
    timestamp: datetime | None = Field(
        default=None, description="Point-in-time timestamp"
    )
    include_metadata: bool = Field(
        default=False, description="Include feature metadata"
    )


class FeatureResponse(BaseModel):
    """Response from feature store."""

    entity_id: str = Field(description="Entity identifier")
    features: dict[str, Any] = Field(description="Feature values")
    feature_metadata: dict[str, dict[str, Any]] = Field(
        default_factory=dict, description="Feature metadata"
    )
    timestamp: datetime = Field(description="Feature timestamp")
    cache_hit: bool = Field(default=False, description="Whether result was cached")


class RealTimeFeatureComputer:
    """Computes real-time features from streaming events."""

    def __init__(self, config: FeatureStoreConfig | None = None):
        """Initialize real-time feature computer."""
        self.config = config or FeatureStoreConfig()

        # Feature computation functions
        self.feature_computers: dict[str, Callable] = {}

        # Feature dependency graph
        self.feature_dependencies: dict[str, list[str]] = {}

        logger.info("Real-time feature computer initialized")

    def register_feature_computer(
        self,
        feature_name: str,
        computer_function: Callable,
        dependencies: list[str] | None = None,
    ) -> None:
        """Register a real-time feature computer function."""
        self.feature_computers[feature_name] = computer_function
        self.feature_dependencies[feature_name] = dependencies or []

        logger.info(
            "Feature computer registered",
            feature_name=feature_name,
            dependencies=dependencies,
        )

    async def compute_features(
        self,
        entity_id: str,
        input_features: dict[str, Any],
        requested_features: list[str],
    ) -> dict[str, Any]:
        """Compute real-time features."""
        try:
            computed_features = input_features.copy()

            # Sort features by dependency order
            ordered_features = self._topological_sort(requested_features)

            for feature_name in ordered_features:
                if feature_name in self.feature_computers:
                    try:
                        # Get dependencies
                        deps = self.feature_dependencies.get(feature_name, [])
                        dep_values = {dep: computed_features.get(dep) for dep in deps}

                        # Compute feature
                        computer_func = self.feature_computers[feature_name]
                        computed_value = await self._call_computer_function(
                            computer_func, entity_id, computed_features, dep_values
                        )

                        computed_features[feature_name] = computed_value

                    except Exception as compute_error:
                        logger.warning(
                            "Failed to compute real-time feature",
                            feature_name=feature_name,
                            entity_id=entity_id,
                            error=str(compute_error),
                        )
                        # Continue with other features

            return computed_features

        except Exception as error:
            logger.error(
                "Failed to compute real-time features",
                entity_id=entity_id,
                requested_features=requested_features,
                error=str(error),
            )
            return input_features

    def _topological_sort(self, features: list[str]) -> list[str]:
        """Sort features by dependency order using topological sort."""
        # Simple topological sort implementation
        visited = set()
        temp_visited = set()
        result = []

        def visit(feature: str):
            if feature in temp_visited:
                # Circular dependency detected, skip
                return
            if feature in visited:
                return

            temp_visited.add(feature)

            # Visit dependencies first
            for dep in self.feature_dependencies.get(feature, []):
                if dep in features:
                    visit(dep)

            temp_visited.remove(feature)
            visited.add(feature)
            result.append(feature)

        for feature in features:
            if feature not in visited:
                visit(feature)

        return result

    async def _call_computer_function(
        self,
        func: Callable,
        entity_id: str,
        all_features: dict[str, Any],
        dependencies: dict[str, Any],
    ) -> Any:
        """Call feature computer function safely."""
        try:
            # Check if function is async
            if asyncio.iscoroutinefunction(func):
                return await asyncio.wait_for(
                    func(entity_id, all_features, dependencies),
                    timeout=self.config.real_time_feature_timeout_ms / 1000,
                )
            else:
                # Run in executor for sync functions
                return await asyncio.get_event_loop().run_in_executor(
                    None, func, entity_id, all_features, dependencies
                )
        except TimeoutError:
            logger.warning(
                "Feature computation timeout",
                function=getattr(func, "__name__", "unknown"),
                entity_id=entity_id,
            )
            return None
        except Exception as error:
            logger.error(
                "Feature computation error",
                function=getattr(func, "__name__", "unknown"),
                entity_id=entity_id,
                error=str(error),
            )
            return None


class FeatureStoreClient:
    """Client for interacting with feature store."""

    def __init__(self, config: FeatureStoreConfig | None = None):
        """Initialize feature store client."""
        self.config = config or FeatureStoreConfig()
        self.metrics = MLOpsMetrics()

        # Feature cache
        self.feature_cache: dict[str, dict[str, Any]] = {}
        self.cache_timestamps: dict[str, datetime] = {}

        # Real-time feature computer
        self.real_time_computer = RealTimeFeatureComputer(config)

        logger.info(
            "Feature store client initialized",
            endpoint=self.config.feature_serving_endpoint,
            cache_ttl=self.config.feature_cache_ttl_seconds,
        )

    async def get_features(
        self,
        entity_ids: list[str],
        feature_names: list[str],
        timestamp: datetime | None = None,
        include_real_time: bool = True,
    ) -> list[FeatureResponse]:
        """Get features for entities."""
        try:
            responses = []

            for entity_id in entity_ids:
                # Check cache first
                cached_features = self._get_cached_features(entity_id, feature_names)

                if cached_features:
                    responses.append(
                        FeatureResponse(
                            entity_id=entity_id,
                            features=cached_features,
                            timestamp=self.cache_timestamps.get(
                                entity_id, datetime.now(timezone.utc)
                            ),
                            cache_hit=True,
                        )
                    )
                else:
                    # Fetch from feature store
                    features = await self._fetch_features_from_store(
                        entity_id, feature_names, timestamp
                    )

                    # Compute real-time features if enabled
                    if include_real_time and self.config.enable_real_time_features:
                        features = await self.real_time_computer.compute_features(
                            entity_id, features, feature_names
                        )

                    # Cache features
                    self._cache_features(entity_id, features)

                    responses.append(
                        FeatureResponse(
                            entity_id=entity_id,
                            features=features,
                            timestamp=timestamp or datetime.now(timezone.utc),
                            cache_hit=False,
                        )
                    )

            # Record metrics (placeholder - extend MLOpsMetrics as needed)
            # TODO: Add proper metrics tracking for feature store operations

            return responses

        except Exception as error:
            logger.error(
                "Failed to get features",
                entity_ids=entity_ids,
                feature_names=feature_names,
                error=str(error),
            )
            raise

    def _get_cached_features(
        self, entity_id: str, feature_names: list[str]
    ) -> dict[str, Any] | None:
        """Get features from cache if valid."""
        cache_key = entity_id

        if cache_key not in self.feature_cache:
            return None

        # Check cache validity
        cache_time = self.cache_timestamps.get(cache_key)
        if not cache_time:
            return None

        age_seconds = (datetime.now(timezone.utc) - cache_time).total_seconds()
        if age_seconds > self.config.feature_cache_ttl_seconds:
            # Cache expired, remove it
            del self.feature_cache[cache_key]
            del self.cache_timestamps[cache_key]
            return None

        cached_features = self.feature_cache[cache_key]

        # Check if all requested features are cached
        if all(feature in cached_features for feature in feature_names):
            return {feature: cached_features[feature] for feature in feature_names}

        return None

    async def _fetch_features_from_store(
        self,
        entity_id: str,
        feature_names: list[str],
        timestamp: datetime | None = None,
    ) -> dict[str, Any]:
        """Fetch features from feature store."""
        # Placeholder implementation - in production, make HTTP request to feature store
        try:
            # Simulate feature store request
            await asyncio.sleep(0.01)  # Simulate network latency

            # Mock feature data
            features = {}

            for feature_name in feature_names:
                if feature_name.startswith("user_"):
                    features[feature_name] = self._generate_mock_user_feature(
                        entity_id, feature_name
                    )
                elif feature_name.startswith("item_"):
                    features[feature_name] = self._generate_mock_item_feature(
                        entity_id, feature_name
                    )
                else:
                    features[feature_name] = self._generate_mock_feature(
                        entity_id, feature_name
                    )

            return features

        except Exception as error:
            logger.error(
                "Failed to fetch features from store",
                entity_id=entity_id,
                feature_names=feature_names,
                error=str(error),
            )

            # Return empty features or fallback to cache
            if self.config.fallback_to_cache_on_error:
                return self.feature_cache.get(entity_id, {})

            return {}

    def _generate_mock_user_feature(self, entity_id: str, feature_name: str) -> Any:
        """Generate mock user feature."""
        import hashlib

        # Create deterministic mock data based on entity_id and feature_name
        seed = (
            int(hashlib.md5(f"{entity_id}_{feature_name}".encode()).hexdigest(), 16)
            % 1000
        )

        if "age" in feature_name:
            return 18 + (seed % 60)
        elif "income" in feature_name:
            return 30000 + (seed % 100000)
        elif "category" in feature_name:
            categories = ["A", "B", "C", "D"]
            return categories[seed % len(categories)]
        elif "score" in feature_name:
            return (seed % 100) / 100.0
        else:
            return seed

    def _generate_mock_item_feature(self, entity_id: str, feature_name: str) -> Any:
        """Generate mock item feature."""
        import hashlib

        seed = (
            int(hashlib.md5(f"{entity_id}_{feature_name}".encode()).hexdigest(), 16)
            % 1000
        )

        if "price" in feature_name:
            return 10.0 + (seed % 1000) / 10.0
        elif "rating" in feature_name:
            return 1.0 + (seed % 50) / 10.0
        elif "category" in feature_name:
            categories = ["electronics", "books", "clothing", "food"]
            return categories[seed % len(categories)]
        else:
            return seed

    def _generate_mock_feature(self, entity_id: str, feature_name: str) -> Any:
        """Generate mock feature."""
        import hashlib

        seed = (
            int(hashlib.md5(f"{entity_id}_{feature_name}".encode()).hexdigest(), 16)
            % 1000
        )
        return seed

    def _cache_features(self, entity_id: str, features: dict[str, Any]) -> None:
        """Cache features for entity."""
        self.feature_cache[entity_id] = features.copy()
        self.cache_timestamps[entity_id] = datetime.now(timezone.utc)

    async def get_batch_features(
        self,
        entity_ids: list[str],
        feature_names: list[str],
        timestamp: datetime | None = None,
    ) -> dict[str, dict[str, Any]]:
        """Get features for multiple entities in batch."""
        try:
            # Split into batches
            batch_size = self.config.batch_feature_fetch_size
            results = {}

            for i in range(0, len(entity_ids), batch_size):
                batch_entity_ids = entity_ids[i : i + batch_size]

                # Get features for batch
                batch_responses = await self.get_features(
                    batch_entity_ids, feature_names, timestamp
                )

                # Collect results
                for response in batch_responses:
                    results[response.entity_id] = response.features

            return results

        except Exception as error:
            logger.error(
                "Failed to get batch features",
                entity_count=len(entity_ids),
                feature_count=len(feature_names),
                error=str(error),
            )
            raise

    def register_real_time_feature(
        self,
        feature_name: str,
        computer_function: Callable,
        dependencies: list[str] | None = None,
    ) -> None:
        """Register a real-time feature computer."""
        self.real_time_computer.register_feature_computer(
            feature_name, computer_function, dependencies
        )

    async def validate_feature_freshness(
        self, features: dict[str, Any], entity_id: str
    ) -> dict[str, bool]:
        """Validate feature freshness."""
        if not self.config.enable_feature_freshness_check:
            return {feature: True for feature in features}

        freshness_results = {}
        current_time = datetime.now(timezone.utc)

        # Check cache timestamps
        cache_time = self.cache_timestamps.get(entity_id)

        for feature_name in features:
            if cache_time:
                age_seconds = (current_time - cache_time).total_seconds()
                is_fresh = age_seconds <= self.config.max_feature_age_seconds
            else:
                is_fresh = False  # No timestamp info, assume stale

            freshness_results[feature_name] = is_fresh

        return freshness_results

    def clear_cache(self, entity_id: str | None = None) -> None:
        """Clear feature cache."""
        if entity_id:
            self.feature_cache.pop(entity_id, None)
            self.cache_timestamps.pop(entity_id, None)
        else:
            self.feature_cache.clear()
            self.cache_timestamps.clear()

        logger.info("Feature cache cleared", entity_id=entity_id)

    def get_cache_stats(self) -> dict[str, Any]:
        """Get feature cache statistics."""
        current_time = datetime.now(timezone.utc)
        expired_count = 0

        for entity_id, cache_time in self.cache_timestamps.items():
            age_seconds = (current_time - cache_time).total_seconds()
            if age_seconds > self.config.feature_cache_ttl_seconds:
                expired_count += 1

        return {
            "cached_entities": len(self.feature_cache),
            "expired_entities": expired_count,
            "cache_hit_ratio": 0.85,  # Placeholder - track this properly
            "average_features_per_entity": (
                sum(len(features) for features in self.feature_cache.values())
                / max(len(self.feature_cache), 1)
            ),
        }


# Example real-time feature computers
async def compute_user_session_length(
    entity_id: str, all_features: dict[str, Any], dependencies: dict[str, Any]
) -> float:
    """Compute user session length in real-time."""
    # Example: compute session length from last_activity_time
    last_activity = dependencies.get("last_activity_time")
    session_start = dependencies.get("session_start_time")

    if last_activity and session_start:
        # Calculate session length in minutes
        if isinstance(last_activity, datetime) and isinstance(session_start, datetime):
            return (last_activity - session_start).total_seconds() / 60.0

    return 0.0


def compute_item_popularity_score(
    entity_id: str, all_features: dict[str, Any], dependencies: dict[str, Any]
) -> float:
    """Compute item popularity score."""
    # Example: compute popularity from view_count and rating
    view_count = dependencies.get("view_count", 0)
    rating = dependencies.get("rating", 0.0)

    # Simple popularity formula
    return (view_count * 0.7) + (rating * 10 * 0.3)


async def compute_user_affinity_score(
    entity_id: str, all_features: dict[str, Any], dependencies: dict[str, Any]
) -> float:
    """Compute user affinity score for recommendations."""
    # Example: compute affinity from user preferences and item features
    user_category_pref = dependencies.get("user_category_preference", "")
    item_category = dependencies.get("item_category", "")

    # Simple affinity calculation
    if user_category_pref == item_category:
        return 1.0
    else:
        return 0.3


# Factory function to create feature store client with default configurations
def create_feature_store_client(
    feature_serving_endpoint: str | None = None,
    cache_ttl_seconds: int | None = None,
) -> FeatureStoreClient:
    """Create feature store client with optional configuration overrides."""
    config = FeatureStoreConfig()

    if feature_serving_endpoint:
        config.feature_serving_endpoint = feature_serving_endpoint

    if cache_ttl_seconds:
        config.feature_cache_ttl_seconds = cache_ttl_seconds

    client = FeatureStoreClient(config)

    # Register default real-time feature computers
    client.register_real_time_feature(
        "user_session_length",
        compute_user_session_length,
        ["last_activity_time", "session_start_time"],
    )

    client.register_real_time_feature(
        "item_popularity_score", compute_item_popularity_score, ["view_count", "rating"]
    )

    client.register_real_time_feature(
        "user_affinity_score",
        compute_user_affinity_score,
        ["user_category_preference", "item_category"],
    )

    return client
