"""Feature store for reusable feature engineering and serving."""

import json
from datetime import datetime, timedelta, timezone
from typing import Any

import pandas as pd
import structlog
from pydantic import BaseModel, Field
from sqlalchemy import Column, DateTime, Integer, String, Text, create_engine
from sqlalchemy.ext.declarative import DeclarativeMeta, declarative_base
from sqlalchemy.orm import sessionmaker

from ..observability.metrics import MLOpsMetrics
from .config import BaseConfig

logger = structlog.get_logger(__name__)

Base: DeclarativeMeta = declarative_base()


class FeatureStoreConfig(BaseConfig):
    """Configuration for feature store."""

    # Database settings
    database_url: str = Field(
        default="sqlite:///feature_store.db",
        description="Database URL for feature store",
    )

    # Feature serving settings
    online_serving: bool = Field(
        default=True, description="Enable online feature serving"
    )
    offline_serving: bool = Field(
        default=True, description="Enable offline feature serving"
    )

    # Caching settings
    feature_cache_ttl_seconds: int = Field(
        default=3600, description="TTL for cached features"
    )
    enable_feature_caching: bool = Field(
        default=True, description="Enable feature value caching"
    )

    # Data freshness settings
    feature_freshness_threshold_hours: int = Field(
        default=24, description="Alert threshold for stale features"
    )
    enable_freshness_monitoring: bool = Field(
        default=True, description="Enable feature freshness monitoring"
    )

    # Validation settings
    enable_feature_validation: bool = Field(
        default=True, description="Enable feature validation"
    )
    max_null_percentage: float = Field(
        default=0.1, description="Maximum allowed null percentage"
    )


class FeatureDefinitionModel(Base):  # type: ignore[misc,valid-type]
    """Database model for feature definitions."""

    __tablename__ = "feature_definitions"

    id = Column(Integer, primary_key=True)
    name = Column(String(255), unique=True, nullable=False)
    description = Column(Text)
    feature_type = Column(String(50), nullable=False)
    data_type = Column(String(50), nullable=False)
    source_table = Column(String(255))
    transformation_logic = Column(Text)
    owner = Column(String(100))
    tags = Column(Text)  # JSON string
    created_at = Column(DateTime, default=lambda: datetime.now(timezone.utc))
    updated_at = Column(
        DateTime,
        default=lambda: datetime.now(timezone.utc),
        onupdate=lambda: datetime.now(timezone.utc),
    )


class FeatureValueModel(Base):  # type: ignore[misc,valid-type]
    """Database model for feature values."""

    __tablename__ = "feature_values"

    id = Column(Integer, primary_key=True)
    feature_name = Column(String(255), nullable=False)
    entity_id = Column(String(255), nullable=False)
    feature_value = Column(Text)  # JSON string for complex values
    created_at = Column(DateTime, default=lambda: datetime.now(timezone.utc))
    valid_from = Column(DateTime, nullable=False)
    valid_until = Column(DateTime)


class FeatureDefinition(BaseModel):
    """Feature definition schema."""

    name: str = Field(description="Unique feature name")
    description: str | None = Field(default=None, description="Feature description")
    feature_type: str = Field(
        description="Feature type (categorical, numerical, text, etc.)"
    )
    data_type: str = Field(description="Data type (int, float, string, boolean)")

    # Source information
    source_table: str | None = Field(default=None, description="Source table/dataset")
    transformation_logic: str | None = Field(
        default=None, description="SQL or transformation logic"
    )

    # Metadata
    owner: str | None = Field(default=None, description="Feature owner")
    tags: list[str] = Field(default_factory=list, description="Feature tags")

    # Validation rules
    validation_rules: dict[str, Any] = Field(
        default_factory=dict, description="Feature validation rules"
    )

    # Freshness settings
    expected_update_frequency: str | None = Field(
        default=None, description="Expected update frequency (hourly, daily, weekly)"
    )

    # Serving settings
    online_serving_enabled: bool = Field(
        default=True, description="Enable online serving"
    )
    offline_serving_enabled: bool = Field(
        default=True, description="Enable offline serving"
    )

    # Lifecycle
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    updated_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))


class FeatureValue(BaseModel):
    """Feature value schema."""

    feature_name: str = Field(description="Feature name")
    entity_id: str = Field(description="Entity ID (e.g., customer_id)")
    value: Any = Field(description="Feature value")

    # Temporal information
    timestamp: datetime = Field(
        default_factory=lambda: datetime.now(timezone.utc),
        description="Value timestamp",
    )
    valid_from: datetime = Field(
        default_factory=lambda: datetime.now(timezone.utc),
        description="Valid from timestamp",
    )
    valid_until: datetime | None = Field(
        default=None, description="Valid until timestamp"
    )

    # Metadata
    source: str | None = Field(default=None, description="Value source")
    quality_score: float | None = Field(
        default=None, description="Data quality score (0-1)"
    )


class FeatureSet(BaseModel):
    """Collection of related features."""

    name: str = Field(description="Feature set name")
    description: str | None = Field(default=None, description="Feature set description")
    features: list[str] = Field(description="List of feature names in the set")

    # Metadata
    owner: str | None = Field(default=None, description="Feature set owner")
    tags: list[str] = Field(default_factory=list, description="Feature set tags")

    # Serving configuration
    serving_config: dict[str, Any] = Field(
        default_factory=dict, description="Serving configuration"
    )

    # Lifecycle
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    updated_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))


class FeatureStore:
    """Feature store for managing ML features."""

    def __init__(self, config: FeatureStoreConfig | None = None):
        """Initialize feature store."""
        self.config = config or FeatureStoreConfig()
        self.metrics = MLOpsMetrics()

        # Database setup
        self.engine = create_engine(self.config.database_url)
        Base.metadata.create_all(self.engine)
        self.SessionLocal = sessionmaker(bind=self.engine)

        # Feature cache (in production, use Redis or similar)
        self.feature_cache: dict[str, dict[str, Any]] = {}

        logger.info(
            "Feature store initialized",
            database_url=self.config.database_url,
            online_serving=self.config.online_serving,
        )

    def create_feature(self, feature_def: FeatureDefinition) -> str:
        """Create a new feature definition."""
        try:
            session = self.SessionLocal()

            # Check if feature already exists
            existing = (
                session.query(FeatureDefinitionModel)
                .filter(FeatureDefinitionModel.name == feature_def.name)
                .first()
            )

            if existing:
                raise ValueError(f"Feature '{feature_def.name}' already exists")

            # Create feature
            feature_model = FeatureDefinitionModel(
                name=feature_def.name,
                description=feature_def.description,
                feature_type=feature_def.feature_type,
                data_type=feature_def.data_type,
                source_table=feature_def.source_table,
                transformation_logic=feature_def.transformation_logic,
                owner=feature_def.owner,
                tags=json.dumps(feature_def.tags),
            )

            session.add(feature_model)
            session.commit()

            feature_id = feature_model.id
            session.close()

            # Record metrics
            self.metrics.record_feature_creation(
                feature_name=feature_def.name,
                feature_type=feature_def.feature_type,
                owner=feature_def.owner,
            )

            logger.info(
                "Feature created",
                feature_name=feature_def.name,
                feature_id=feature_id,
                owner=feature_def.owner,
            )

            return str(feature_id)

        except Exception as error:
            logger.error(
                "Failed to create feature",
                feature_name=feature_def.name,
                error=str(error),
            )
            raise

    def get_feature_definition(self, feature_name: str) -> FeatureDefinition | None:
        """Get feature definition by name."""
        try:
            session = self.SessionLocal()

            feature_model = (
                session.query(FeatureDefinitionModel)
                .filter(FeatureDefinitionModel.name == feature_name)
                .first()
            )

            session.close()

            if not feature_model:
                return None

            feature_def = FeatureDefinition(
                name=feature_model.name,  # type: ignore[arg-type]
                description=feature_model.description,  # type: ignore[arg-type]
                feature_type=feature_model.feature_type,  # type: ignore[arg-type]
                data_type=feature_model.data_type,  # type: ignore[arg-type]
                source_table=feature_model.source_table,  # type: ignore[arg-type]
                transformation_logic=feature_model.transformation_logic,  # type: ignore[arg-type]
                owner=feature_model.owner,  # type: ignore[arg-type]
                tags=json.loads(feature_model.tags) if feature_model.tags else [],  # type: ignore[arg-type]
                created_at=feature_model.created_at.replace(tzinfo=timezone.utc),
                updated_at=feature_model.updated_at.replace(tzinfo=timezone.utc),
            )

            return feature_def

        except Exception as error:
            logger.error(
                "Failed to get feature definition",
                feature_name=feature_name,
                error=str(error),
            )
            return None

    def write_features(
        self,
        feature_values: list[FeatureValue],
        batch_size: int = 1000,
    ) -> int:
        """Write feature values to store."""
        try:
            session = self.SessionLocal()
            written_count = 0

            for i in range(0, len(feature_values), batch_size):
                batch = feature_values[i : i + batch_size]

                # Validate features
                if self.config.enable_feature_validation:
                    self._validate_feature_batch(batch)

                # Convert to database models
                feature_models = []
                for fv in batch:
                    feature_model = FeatureValueModel(
                        feature_name=fv.feature_name,
                        entity_id=fv.entity_id,
                        feature_value=json.dumps(fv.value),
                        created_at=fv.timestamp,
                        valid_from=fv.valid_from,
                        valid_until=fv.valid_until,
                    )
                    feature_models.append(feature_model)

                # Bulk insert
                session.bulk_save_objects(feature_models)
                session.commit()
                written_count += len(batch)

                # Update cache if enabled
                if self.config.enable_feature_caching:
                    self._update_feature_cache(batch)

            session.close()

            # Record metrics
            self.metrics.record_feature_write(
                feature_count=written_count,
                batch_count=len(range(0, len(feature_values), batch_size)),
                success=True,
            )

            logger.info(
                "Features written",
                count=written_count,
                batches=len(range(0, len(feature_values), batch_size)),
            )

            return written_count

        except Exception as error:
            self.metrics.record_feature_write(
                feature_count=0,
                batch_count=0,
                success=False,
                error_type=type(error).__name__,
            )

            logger.error(
                "Failed to write features",
                error=str(error),
            )
            raise

    def read_features(
        self,
        feature_names: list[str],
        entity_ids: list[str],
        as_of_time: datetime | None = None,
    ) -> pd.DataFrame:
        """Read feature values from store."""
        try:
            # Check cache first
            if self.config.enable_feature_caching:
                cached_features = self._get_cached_features(
                    feature_names, entity_ids, as_of_time
                )
                if cached_features is not None:
                    return cached_features

            session = self.SessionLocal()

            # Build query
            query = session.query(FeatureValueModel).filter(
                FeatureValueModel.feature_name.in_(feature_names),
                FeatureValueModel.entity_id.in_(entity_ids),
            )

            if as_of_time:
                query = query.filter(
                    FeatureValueModel.valid_from <= as_of_time,
                    (FeatureValueModel.valid_until.is_(None))
                    | (FeatureValueModel.valid_until > as_of_time),
                )

            # Get latest values for each feature-entity combination
            results = query.all()
            session.close()

            # Convert to DataFrame
            if not results:
                # Return empty DataFrame with correct columns
                columns = ["entity_id"] + feature_names
                return pd.DataFrame(columns=columns)

            # Group by entity and feature, take latest value
            feature_data: dict[tuple[str, str], FeatureValueModel] = {}
            for result in results:
                key = (result.entity_id, result.feature_name)  # type: ignore[assignment]
                if (
                    key not in feature_data  # type: ignore[operator]
                    or result.created_at > feature_data[key].created_at  # type: ignore[index]
                ):
                    feature_data[key] = result  # type: ignore[index]

            # Build DataFrame
            rows = []
            for entity_id in entity_ids:
                row = {"entity_id": entity_id}
                for feature_name in feature_names:
                    key = (entity_id, feature_name)  # This will be str, str at runtime
                    if key in feature_data:  # type: ignore[operator]
                        try:
                            value = json.loads(feature_data[key].feature_value)  # type: ignore[arg-type,index]
                        except (json.JSONDecodeError, TypeError):
                            value = feature_data[key].feature_value  # type: ignore[index]
                        row[feature_name] = value
                    else:
                        row[feature_name] = None
                rows.append(row)

            df = pd.DataFrame(rows)

            # Cache results
            if self.config.enable_feature_caching:
                self._cache_features(feature_names, entity_ids, df, as_of_time)

            # Record metrics
            self.metrics.record_feature_read(
                feature_count=len(feature_names),
                entity_count=len(entity_ids),
                result_count=len(df),
                cache_hit=False,
                success=True,
            )

            logger.info(
                "Features read",
                feature_count=len(feature_names),
                entity_count=len(entity_ids),
                result_rows=len(df),
            )

            return df

        except Exception as error:
            self.metrics.record_feature_read(
                feature_count=len(feature_names),
                entity_count=len(entity_ids),
                result_count=0,
                cache_hit=False,
                success=False,
                error_type=type(error).__name__,
            )

            logger.error(
                "Failed to read features",
                feature_names=feature_names[:5],  # Limit for logging
                entity_count=len(entity_ids),
                error=str(error),
            )
            raise

    def _validate_feature_batch(self, feature_values: list[FeatureValue]) -> None:
        """Validate batch of feature values."""
        try:
            feature_names = set(fv.feature_name for fv in feature_values)

            for feature_name in feature_names:
                # Get feature definition
                feature_def = self.get_feature_definition(feature_name)
                if not feature_def:
                    logger.warning("Feature definition not found", feature=feature_name)
                    continue

                # Get values for this feature
                feature_batch = [
                    fv for fv in feature_values if fv.feature_name == feature_name
                ]

                # Check null percentage
                null_count = sum(1 for fv in feature_batch if fv.value is None)
                null_percentage = null_count / len(feature_batch)

                if null_percentage > self.config.max_null_percentage:
                    raise ValueError(
                        f"Feature '{feature_name}' has {null_percentage:.2%} null values, "
                        f"exceeding threshold {self.config.max_null_percentage:.2%}"
                    )

                # Validate data types
                for fv in feature_batch:
                    if fv.value is not None:
                        self._validate_feature_value_type(fv, feature_def)

        except Exception as error:
            logger.error("Feature validation failed", error=str(error))
            raise

    def _validate_feature_value_type(
        self, feature_value: FeatureValue, feature_def: FeatureDefinition
    ) -> None:
        """Validate feature value type."""
        value = feature_value.value
        expected_type = feature_def.data_type

        if expected_type == "int" and not isinstance(value, int):
            raise ValueError(
                f"Feature '{feature_value.feature_name}' expected int, got {type(value)}"
            )
        elif expected_type == "float" and not isinstance(value, int | float):
            raise ValueError(
                f"Feature '{feature_value.feature_name}' expected float, got {type(value)}"
            )
        elif expected_type == "string" and not isinstance(value, str):
            raise ValueError(
                f"Feature '{feature_value.feature_name}' expected string, got {type(value)}"
            )
        elif expected_type == "boolean" and not isinstance(value, bool):
            raise ValueError(
                f"Feature '{feature_value.feature_name}' expected boolean, got {type(value)}"
            )

    def _update_feature_cache(self, feature_values: list[FeatureValue]) -> None:
        """Update feature cache with new values."""
        try:
            for fv in feature_values:
                cache_key = f"{fv.feature_name}:{fv.entity_id}"
                self.feature_cache[cache_key] = {
                    "value": fv.value,
                    "timestamp": fv.timestamp,
                    "valid_until": fv.valid_until,
                    "cached_at": datetime.now(timezone.utc),
                }
        except Exception as error:
            logger.warning("Failed to update feature cache", error=str(error))

    def _get_cached_features(
        self,
        feature_names: list[str],
        entity_ids: list[str],
        as_of_time: datetime | None = None,
    ) -> pd.DataFrame | None:
        """Get features from cache if available and fresh."""
        try:
            current_time = datetime.now(timezone.utc)
            cache_cutoff = current_time - timedelta(
                seconds=self.config.feature_cache_ttl_seconds
            )

            rows = []
            all_found = True

            for entity_id in entity_ids:
                row = {"entity_id": entity_id}
                for feature_name in feature_names:
                    cache_key = f"{feature_name}:{entity_id}"

                    if cache_key in self.feature_cache:
                        cached = self.feature_cache[cache_key]

                        # Check cache freshness
                        if cached["cached_at"] < cache_cutoff:
                            all_found = False
                            break

                        # Check temporal validity
                        if as_of_time:
                            if (
                                cached["valid_until"]
                                and as_of_time > cached["valid_until"]
                            ):
                                all_found = False
                                break

                        row[feature_name] = cached["value"]
                    else:
                        all_found = False
                        break

                if not all_found:
                    break

                rows.append(row)

            if all_found:
                logger.debug(
                    "Features served from cache",
                    feature_count=len(feature_names),
                    entity_count=len(entity_ids),
                )
                return pd.DataFrame(rows)

            return None

        except Exception as error:
            logger.warning("Failed to get cached features", error=str(error))
            return None

    def _cache_features(
        self,
        feature_names: list[str],
        entity_ids: list[str],
        df: pd.DataFrame,
        as_of_time: datetime | None = None,
    ) -> None:
        """Cache feature values."""
        try:
            current_time = datetime.now(timezone.utc)

            for _, row in df.iterrows():
                entity_id = row["entity_id"]
                for feature_name in feature_names:
                    if feature_name in row and pd.notna(row[feature_name]):
                        cache_key = f"{feature_name}:{entity_id}"
                        self.feature_cache[cache_key] = {
                            "value": row[feature_name],
                            "timestamp": as_of_time or current_time,
                            "valid_until": None,  # Would be set based on feature definition
                            "cached_at": current_time,
                        }
        except Exception as error:
            logger.warning("Failed to cache features", error=str(error))

    def create_feature_set(self, feature_set: FeatureSet) -> str:
        """Create a feature set."""
        try:
            # Validate that all features exist
            missing_features = []
            for feature_name in feature_set.features:
                if not self.get_feature_definition(feature_name):
                    missing_features.append(feature_name)

            if missing_features:
                raise ValueError(f"Missing feature definitions: {missing_features}")

            # In a full implementation, this would be stored in database
            feature_set_id = f"fs_{int(datetime.now(timezone.utc).timestamp())}"

            logger.info(
                "Feature set created",
                name=feature_set.name,
                feature_set_id=feature_set_id,
                feature_count=len(feature_set.features),
            )

            return feature_set_id

        except Exception as error:
            logger.error(
                "Failed to create feature set",
                name=feature_set.name,
                error=str(error),
            )
            raise

    def list_features(
        self,
        feature_type: str | None = None,
        owner: str | None = None,
        tags: list[str] | None = None,
    ) -> list[FeatureDefinition]:
        """List feature definitions with optional filtering."""
        try:
            session = self.SessionLocal()

            query = session.query(FeatureDefinitionModel)

            if feature_type:
                query = query.filter(
                    FeatureDefinitionModel.feature_type == feature_type
                )
            if owner:
                query = query.filter(FeatureDefinitionModel.owner == owner)

            results = query.all()
            session.close()

            features = []
            for result in results:
                # Filter by tags if specified
                result_tags = json.loads(result.tags) if result.tags else []  # type: ignore[arg-type]
                if tags and not any(tag in result_tags for tag in tags):
                    continue

                feature_def = FeatureDefinition(
                    name=result.name,  # type: ignore[arg-type]
                    description=result.description,  # type: ignore[arg-type]
                    feature_type=result.feature_type,  # type: ignore[arg-type]
                    data_type=result.data_type,  # type: ignore[arg-type]
                    source_table=result.source_table,  # type: ignore[arg-type]
                    transformation_logic=result.transformation_logic,  # type: ignore[arg-type]
                    owner=result.owner,  # type: ignore[arg-type]
                    tags=result_tags,
                    created_at=result.created_at.replace(tzinfo=timezone.utc),
                    updated_at=result.updated_at.replace(tzinfo=timezone.utc),
                )
                features.append(feature_def)

            logger.info(
                "Features listed",
                count=len(features),
                feature_type=feature_type,
                owner=owner,
            )

            return features

        except Exception as error:
            logger.error("Failed to list features", error=str(error))
            raise

    def get_feature_stats(self) -> dict[str, Any]:
        """Get feature store statistics."""
        try:
            session = self.SessionLocal()

            # Count features by type
            feature_counts = {}
            from sqlalchemy import func
            results = (
                session.query(
                    FeatureDefinitionModel.feature_type,
                    func.count(FeatureDefinitionModel.id).label("count"),
                )
                .group_by(FeatureDefinitionModel.feature_type)
                .all()
            )

            for result in results:
                feature_counts[result.feature_type] = result.count  # type: ignore[attr-defined]

            # Count total features
            total_features = session.query(FeatureDefinitionModel).count()

            # Count recent features (last 7 days)
            week_ago = datetime.now(timezone.utc) - timedelta(days=7)
            recent_features = (
                session.query(FeatureDefinitionModel)
                .filter(FeatureDefinitionModel.created_at > week_ago)
                .count()
            )

            # Count feature values (recent)
            day_ago = datetime.now(timezone.utc) - timedelta(days=1)
            recent_values = (
                session.query(FeatureValueModel)
                .filter(FeatureValueModel.created_at > day_ago)
                .count()
            )

            session.close()

            return {
                "total_features": total_features,
                "features_by_type": feature_counts,
                "recent_features_7d": recent_features,
                "recent_values_24h": recent_values,
                "cache_size": len(self.feature_cache),
                "online_serving_enabled": self.config.online_serving,
                "offline_serving_enabled": self.config.offline_serving,
                "last_updated": datetime.now(timezone.utc).isoformat(),
            }

        except Exception as error:
            logger.error("Failed to get feature stats", error=str(error))
            raise

    def cleanup_old_values(self, retention_days: int = 90) -> int:
        """Clean up old feature values."""
        try:
            cutoff_date = datetime.now(timezone.utc) - timedelta(days=retention_days)

            session = self.SessionLocal()

            deleted = (
                session.query(FeatureValueModel)
                .filter(FeatureValueModel.created_at < cutoff_date)
                .delete()
            )

            session.commit()
            session.close()

            logger.info(
                "Old feature values cleaned up",
                deleted_count=deleted,
                retention_days=retention_days,
            )

            return deleted

        except Exception as error:
            logger.error("Failed to cleanup old values", error=str(error))
            raise
