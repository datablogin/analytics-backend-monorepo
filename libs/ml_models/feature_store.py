"""Feature store for reusable feature engineering and serving.

NOTE: This module is in partial async conversion state. Some methods are async
while others remain synchronous. This causes type checking errors that need
to be resolved with a complete async refactoring.
"""

import json
import time
from datetime import datetime, timedelta, timezone
from typing import Any

import pandas as pd
import structlog
from pydantic import BaseModel, Field
from sqlalchemy import (
    Column,
    DateTime,
    Float,
    Index,
    Integer,
    String,
    Text,
)
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from sqlalchemy.ext.declarative import DeclarativeMeta, declarative_base

from ..data_processing.lineage import DataLineageTracker
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

    # Performance settings
    online_serving_timeout_ms: int = Field(
        default=100, description="Maximum latency for online serving (ms)"
    )
    batch_serving_chunk_size: int = Field(
        default=10000, description="Chunk size for batch serving"
    )

    # Caching settings
    feature_cache_ttl_seconds: int = Field(
        default=3600, description="TTL for cached features"
    )
    enable_feature_caching: bool = Field(
        default=True, description="Enable feature value caching"
    )
    cache_backend: str = Field(
        default="memory", description="Cache backend (memory, redis)"
    )
    redis_url: str | None = Field(default=None, description="Redis URL for caching")

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

    # Monitoring and drift detection
    enable_drift_detection: bool = Field(
        default=True, description="Enable feature drift detection"
    )
    drift_detection_sample_size: int = Field(
        default=1000, description="Sample size for drift detection"
    )

    # Lineage tracking
    enable_lineage_tracking: bool = Field(
        default=True, description="Enable data lineage tracking"
    )

    # Feature discovery and documentation
    enable_feature_discovery: bool = Field(
        default=True, description="Enable feature discovery portal"
    )

    # Real-time streaming
    enable_real_time_ingestion: bool = Field(
        default=False, description="Enable real-time feature ingestion"
    )
    kafka_bootstrap_servers: str | None = Field(
        default=None, description="Kafka servers for streaming"
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

    # Additional columns for enhanced functionality
    event_timestamp = Column(DateTime, nullable=False)
    ingestion_timestamp = Column(DateTime, default=lambda: datetime.now(timezone.utc))
    source = Column(String(255))
    data_quality_score = Column(Float)

    # Indexes for performance
    __table_args__ = (
        Index(
            "idx_feature_entity_time", "feature_name", "entity_id", "event_timestamp"
        ),
        Index("idx_feature_valid_from", "feature_name", "valid_from"),
        Index("idx_entity_time", "entity_id", "event_timestamp"),
    )


class FeatureSetModel(Base):  # type: ignore[misc,valid-type]
    """Database model for feature sets."""

    __tablename__ = "feature_sets"

    id = Column(Integer, primary_key=True)
    name = Column(String(255), unique=True, nullable=False)
    description = Column(Text)
    feature_names = Column(Text)  # JSON array of feature names
    owner = Column(String(100))
    tags = Column(Text)  # JSON array
    serving_config = Column(Text)  # JSON configuration
    created_at = Column(DateTime, default=lambda: datetime.now(timezone.utc))
    updated_at = Column(
        DateTime,
        default=lambda: datetime.now(timezone.utc),
        onupdate=lambda: datetime.now(timezone.utc),
    )


class FeatureTransformationModel(Base):  # type: ignore[misc,valid-type]
    """Database model for feature transformations."""

    __tablename__ = "feature_transformations"

    id = Column(Integer, primary_key=True)
    name = Column(String(255), unique=True, nullable=False)
    description = Column(Text)
    input_features = Column(Text)  # JSON array of input feature names
    output_features = Column(Text)  # JSON array of output feature names
    transformation_code = Column(Text)  # SQL, Python, or other transformation code
    transformation_type = Column(String(50))  # sql, python, spark, etc.
    schedule = Column(String(100))  # Cron expression for batch processing
    owner = Column(String(100))
    created_at = Column(DateTime, default=lambda: datetime.now(timezone.utc))
    updated_at = Column(
        DateTime,
        default=lambda: datetime.now(timezone.utc),
        onupdate=lambda: datetime.now(timezone.utc),
    )


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
    event_timestamp: datetime = Field(
        default_factory=lambda: datetime.now(timezone.utc),
        description="Event timestamp when the feature was generated",
    )
    ingestion_timestamp: datetime = Field(
        default_factory=lambda: datetime.now(timezone.utc),
        description="Timestamp when feature was ingested into store",
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


class FeatureTransformation(BaseModel):
    """Feature transformation schema."""

    name: str = Field(description="Unique transformation name")
    description: str | None = Field(
        default=None, description="Transformation description"
    )
    input_features: list[str] = Field(description="Input feature names")
    output_features: list[str] = Field(description="Output feature names")
    transformation_code: str = Field(description="Transformation logic")
    transformation_type: str = Field(
        default="python", description="Type of transformation (sql, python, spark)"
    )
    schedule: str | None = Field(
        default=None, description="Cron schedule for batch processing"
    )
    owner: str | None = Field(default=None, description="Transformation owner")
    parameters: dict[str, Any] = Field(
        default_factory=dict, description="Transformation parameters"
    )
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    updated_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))


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
        # Convert SQLite URLs to async format
        database_url = self.config.database_url
        if database_url.startswith("sqlite:///"):
            database_url = database_url.replace("sqlite:///", "sqlite+aiosqlite:///")
        elif database_url.startswith("postgresql://"):
            database_url = database_url.replace(
                "postgresql://", "postgresql+asyncpg://"
            )

        self.engine = create_async_engine(database_url)
        self.SessionLocal = async_sessionmaker(bind=self.engine)

        # Cache setup
        self._setup_cache()

        # Lineage tracking
        if self.config.enable_lineage_tracking:
            self.lineage_tracker = DataLineageTracker()
        else:
            self.lineage_tracker = None

        # Feature transformations registry
        self.transformations: dict[str, FeatureTransformation] = {}

        # Monitoring components (will be initialized on demand)
        self._drift_detector = None

        logger.info(
            "Feature store initialized",
            database_url=self.config.database_url,
            online_serving=self.config.online_serving,
            cache_backend=self.config.cache_backend,
            lineage_enabled=self.config.enable_lineage_tracking,
        )

    async def initialize_tables(self) -> None:
        """Initialize database tables asynchronously."""
        async with self.engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)

        logger.info("Feature store tables initialized")

    def _setup_cache(self) -> None:
        """Setup caching backend."""
        if self.config.cache_backend == "redis" and self.config.redis_url:
            try:
                import redis

                self.cache_client = redis.Redis.from_url(self.config.redis_url)
                # Test connection
                self.cache_client.ping()
                logger.info("Redis cache initialized", url=self.config.redis_url)
            except Exception as error:
                logger.warning(
                    "Failed to initialize Redis cache, falling back to memory",
                    error=str(error),
                )
                self.cache_client = None
        else:
            self.cache_client = None

        # Initialize feature cache
        self.feature_cache: dict[str, dict[str, Any]] = {}

    async def create_feature(self, feature_def: FeatureDefinition) -> str:
        """Create a new feature definition."""
        try:
            async with self.SessionLocal() as session:
                # Check if feature already exists
                from sqlalchemy import select

                stmt = select(FeatureDefinitionModel).where(
                    FeatureDefinitionModel.name == feature_def.name
                )
                result = await session.execute(stmt)
                existing = result.scalar_one_or_none()

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
                await session.commit()
                feature_id = feature_model.id

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

    async def get_feature_definition(
        self, feature_name: str
    ) -> FeatureDefinition | None:
        """Get feature definition by name."""
        try:
            async with self.SessionLocal() as session:
                from sqlalchemy import select

                stmt = select(FeatureDefinitionModel).where(
                    FeatureDefinitionModel.name == feature_name
                )
                result = await session.execute(stmt)
                feature_model = result.scalar_one_or_none()

                if not feature_model:
                    return None

                feature_def = FeatureDefinition(
                    name=feature_model.name,
                    description=feature_model.description,
                    feature_type=feature_model.feature_type,
                    data_type=feature_model.data_type,
                    source_table=feature_model.source_table,
                    transformation_logic=feature_model.transformation_logic,
                    owner=feature_model.owner,
                    tags=(json.loads(feature_model.tags) if feature_model.tags else []),
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

    async def write_features(
        self,
        feature_values: list[FeatureValue],
        batch_size: int = 1000,
    ) -> int:
        """Write feature values to store."""
        try:
            async with self.SessionLocal() as session:
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
                            created_at=fv.ingestion_timestamp,
                            valid_from=fv.valid_from,
                            valid_until=fv.valid_until,
                            event_timestamp=fv.event_timestamp,
                            source=fv.source,
                            data_quality_score=fv.quality_score,
                        )
                        feature_models.append(feature_model)

                    # Add models to session
                    for model in feature_models:
                        session.add(model)

                    await session.commit()
                    written_count += len(batch)

                    # Update cache if enabled
                    if self.config.enable_feature_caching:
                        self._update_feature_cache(batch)

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

    def register_transformation(self, transformation: FeatureTransformation) -> str:
        """Register a feature transformation."""
        try:
            session = self.SessionLocal()

            # Check if transformation already exists
            existing = (
                session.query(FeatureTransformationModel)
                .filter(FeatureTransformationModel.name == transformation.name)
                .first()
            )

            if existing:
                raise ValueError(
                    f"Transformation '{transformation.name}' already exists"
                )

            # Create transformation
            transformation_model = FeatureTransformationModel(
                name=transformation.name,
                description=transformation.description,
                input_features=json.dumps(transformation.input_features),
                output_features=json.dumps(transformation.output_features),
                transformation_code=transformation.transformation_code,
                transformation_type=transformation.transformation_type,
                schedule=transformation.schedule,
                owner=transformation.owner,
            )

            session.add(transformation_model)
            session.commit()

            transformation_id = transformation_model.id
            session.close()

            # Store in memory registry
            self.transformations[transformation.name] = transformation

            # Track lineage if enabled
            if self.lineage_tracker:
                # Register transformation in lineage tracker
                lineage_trans_id = self.lineage_tracker.register_transformation(
                    name=transformation.name,
                    transformation_type=transformation.transformation_type,
                    description=transformation.description,
                    code=transformation.transformation_code,
                    parameters=transformation.parameters,
                )

                # Create lineage relationships
                for input_feature in transformation.input_features:
                    for output_feature in transformation.output_features:
                        # Get or create assets for features
                        input_asset = self.lineage_tracker.get_asset_by_name(
                            input_feature
                        )
                        if not input_asset:
                            input_asset_id = self.lineage_tracker.register_asset(
                                name=input_feature,
                                asset_type="feature",
                                metadata={"feature_type": "input"},
                            )
                        else:
                            input_asset_id = input_asset.id

                        output_asset = self.lineage_tracker.get_asset_by_name(
                            output_feature
                        )
                        if not output_asset:
                            output_asset_id = self.lineage_tracker.register_asset(
                                name=output_feature,
                                asset_type="feature",
                                metadata={"feature_type": "output"},
                            )
                        else:
                            output_asset_id = output_asset.id

                        # Add lineage relationship
                        self.lineage_tracker.add_lineage(
                            source_asset_id=input_asset_id,
                            target_asset_id=output_asset_id,
                            transformation_id=lineage_trans_id,
                            relationship_type="transforms_to",
                        )

            logger.info(
                "Feature transformation registered",
                transformation_name=transformation.name,
                transformation_id=transformation_id,
                input_features=transformation.input_features,
                output_features=transformation.output_features,
            )

            return str(transformation_id)

        except Exception as error:
            logger.error(
                "Failed to register transformation",
                transformation_name=transformation.name,
                error=str(error),
            )
            raise

    def execute_transformation(
        self,
        transformation_name: str,
        input_data: pd.DataFrame,
        parameters: dict[str, Any] | None = None,
    ) -> pd.DataFrame:
        """Execute a feature transformation."""
        try:
            if transformation_name not in self.transformations:
                raise ValueError(f"Transformation '{transformation_name}' not found")

            transformation = self.transformations[transformation_name]

            # Validate input features
            missing_features = [
                f for f in transformation.input_features if f not in input_data.columns
            ]
            if missing_features:
                raise ValueError(f"Missing input features: {missing_features}")

            # Execute transformation based on type
            start_time = time.time()

            if transformation.transformation_type == "python":
                output_data = self._execute_python_transformation(
                    transformation, input_data, parameters
                )
            elif transformation.transformation_type == "sql":
                output_data = self._execute_sql_transformation(
                    transformation, input_data, parameters
                )
            else:
                raise ValueError(
                    f"Unsupported transformation type: {transformation.transformation_type}"
                )

            execution_time = time.time() - start_time

            # Record metrics (would need to implement in MLOpsMetrics)
            # self.metrics.record_feature_transformation(
            #     transformation_name=transformation_name,
            #     input_rows=len(input_data),
            #     output_rows=len(output_data),
            #     execution_time_seconds=execution_time,
            #     success=True,
            # )

            logger.info(
                "Feature transformation executed",
                transformation_name=transformation_name,
                input_rows=len(input_data),
                output_rows=len(output_data),
                execution_time_seconds=execution_time,
            )

            return output_data

        except Exception as error:
            # Record metrics (would need to implement in MLOpsMetrics)
            # self.metrics.record_feature_transformation(
            #     transformation_name=transformation_name,
            #     input_rows=len(input_data) if "input_data" in locals() else 0,
            #     output_rows=0,
            #     execution_time_seconds=0,
            #     success=False,
            #     error_type=type(error).__name__,
            # )

            logger.error(
                "Failed to execute transformation",
                transformation_name=transformation_name,
                error=str(error),
            )
            raise

    def _execute_python_transformation(
        self,
        transformation: FeatureTransformation,
        input_data: pd.DataFrame,
        parameters: dict[str, Any] | None = None,
    ) -> pd.DataFrame:
        """Execute Python-based transformation with security restrictions."""
        try:
            # Use AST parsing for safer code execution
            import ast

            # Parse the transformation code
            try:
                parsed = ast.parse(transformation.transformation_code)
            except SyntaxError as e:
                raise ValueError(f"Invalid transformation syntax: {e}")

            # Define safe operations - whitelist approach
            safe_nodes = {
                ast.Expression,
                ast.Load,
                ast.Store,
                ast.Assign,
                ast.Name,
                ast.Constant,
                ast.Attribute,
                ast.Subscript,
                ast.BinOp,
                ast.Compare,
                ast.Call,
                ast.arg,
                ast.Index,
                ast.Slice,
                ast.List,
                ast.Dict,
                ast.Add,
                ast.Sub,
                ast.Mult,
                ast.Div,
                ast.Mod,
                ast.Eq,
                ast.NotEq,
                ast.Lt,
                ast.LtE,
                ast.Gt,
                ast.GtE,
                ast.And,
                ast.Or,
                ast.Not,
                ast.Is,
                ast.IsNot,
                ast.In,
                ast.NotIn,
            }

            # Validate AST nodes are safe
            for node in ast.walk(parsed):
                if type(node) not in safe_nodes:
                    raise ValueError(
                        f"Unsafe operation '{type(node).__name__}' not allowed in transformations"
                    )

                # Restrict function calls to whitelisted functions
                if isinstance(node, ast.Call):
                    if isinstance(node.func, ast.Name):
                        # Only allow specific pandas operations
                        allowed_functions = {
                            "len",
                            "sum",
                            "mean",
                            "std",
                            "min",
                            "max",
                            "abs",
                            "round",
                            "int",
                            "float",
                            "str",
                        }
                        if node.func.id not in allowed_functions:
                            raise ValueError(
                                f"Function '{node.func.id}' not allowed in transformations"
                            )
                    elif isinstance(node.func, ast.Attribute):
                        # Allow pandas DataFrame/Series methods
                        allowed_methods = {
                            "copy",
                            "fillna",
                            "dropna",
                            "drop_duplicates",
                            "groupby",
                            "agg",
                            "apply",
                            "map",
                            "replace",
                            "astype",
                            "reset_index",
                            "sort_values",
                            "head",
                            "tail",
                            "sample",
                            "describe",
                        }
                        if (
                            hasattr(node.func, "attr")
                            and node.func.attr not in allowed_methods
                        ):
                            raise ValueError(
                                f"Method '{node.func.attr}' not allowed in transformations"
                            )

            # Create restricted execution environment
            safe_globals = {
                "__builtins__": {
                    "len": len,
                    "sum": sum,
                    "min": min,
                    "max": max,
                    "abs": abs,
                    "round": round,
                    "int": int,
                    "float": float,
                    "str": str,
                },
                "pd": pd,
            }

            local_vars = {
                "df": input_data.copy(),  # Work on a copy
                "parameters": parameters or {},
                **transformation.parameters,
            }

            # Execute transformation code with restrictions
            exec(compile(parsed, "<transformation>", "exec"), safe_globals, local_vars)

            # Get result DataFrame
            if "result" not in local_vars:
                raise ValueError(
                    "Transformation must assign result to 'result' variable"
                )

            result_df = local_vars["result"]

            if not isinstance(result_df, pd.DataFrame):
                raise ValueError("Transformation result must be a pandas DataFrame")

            # Validate output features
            missing_outputs = [
                f for f in transformation.output_features if f not in result_df.columns
            ]
            if missing_outputs:
                raise ValueError(f"Missing output features: {missing_outputs}")

            return result_df[transformation.output_features]

        except Exception as error:
            logger.error(
                "Python transformation execution failed",
                transformation_name=transformation.name,
                error=str(error),
            )
            raise

    def _execute_sql_transformation(
        self,
        transformation: FeatureTransformation,
        input_data: pd.DataFrame,
        parameters: dict[str, Any] | None = None,
    ) -> pd.DataFrame:
        """Execute SQL-based transformation."""
        try:
            # For SQL transformations, we'd typically use a SQL engine
            # For now, using pandas SQL operations as a placeholder

            # This is a simplified implementation
            # In production, you'd use SQLAlchemy or similar for SQL execution

            # Store input data in temporary table
            temp_table_name = f"temp_{transformation.name}_{int(time.time())}"
            input_data.to_sql(
                temp_table_name, self.engine, if_exists="replace", index=False
            )

            # Execute SQL transformation
            sql_code = transformation.transformation_code

            # Simple parameter substitution (in production, use proper SQL parameterization)
            if parameters:
                for key, value in parameters.items():
                    sql_code = sql_code.replace(f":{key}", str(value))

            result_df = pd.read_sql(sql_code, self.engine)

            # Clean up temporary table
            from sqlalchemy import text

            with self.engine.connect() as conn:
                conn.execute(text(f"DROP TABLE IF EXISTS {temp_table_name}"))
                conn.commit()

            return result_df

        except Exception as error:
            logger.error(
                "SQL transformation execution failed",
                transformation_name=transformation.name,
                error=str(error),
            )
            raise

    def get_online_features(
        self,
        feature_names: list[str],
        entity_ids: list[str],
        timeout_ms: int | None = None,
    ) -> pd.DataFrame:
        """Get features for real-time inference with low latency."""
        start_time = time.time()
        timeout = timeout_ms or self.config.online_serving_timeout_ms

        try:
            if not self.config.online_serving:
                raise ValueError("Online serving is disabled")

            # Fast path: try cache first for all features
            if self.config.enable_feature_caching:
                cached_features = self._get_cached_features_fast(
                    feature_names, entity_ids
                )
                if cached_features is not None:
                    elapsed_ms = (time.time() - start_time) * 1000

                    # Record metrics
                    self.metrics.record_feature_read(
                        feature_count=len(feature_names),
                        entity_count=len(entity_ids),
                        result_count=len(cached_features),
                        cache_hit=True,
                        success=True,
                    )

                    logger.debug(
                        "Online features served from cache",
                        feature_count=len(feature_names),
                        entity_count=len(entity_ids),
                        latency_ms=elapsed_ms,
                    )

                    return cached_features

            # Fallback to database with timeout
            result = self._read_features_with_timeout(
                feature_names, entity_ids, None, timeout_ms
            )

            elapsed_ms = (time.time() - start_time) * 1000

            if elapsed_ms > timeout:
                logger.warning(
                    "Online feature serving exceeded timeout",
                    elapsed_ms=elapsed_ms,
                    timeout_ms=timeout,
                )

            return result

        except Exception as error:
            elapsed_ms = (time.time() - start_time) * 1000

            self.metrics.record_feature_read(
                feature_count=len(feature_names),
                entity_count=len(entity_ids),
                result_count=0,
                cache_hit=False,
                success=False,
                error_type=type(error).__name__,
            )

            logger.error(
                "Online feature serving failed",
                feature_names=feature_names[:5],
                entity_count=len(entity_ids),
                elapsed_ms=elapsed_ms,
                error=str(error),
            )
            raise

    def get_historical_features(
        self,
        feature_names: list[str],
        entity_timestamps: list[tuple[str, datetime]],
        point_in_time_accuracy: bool = True,
    ) -> pd.DataFrame:
        """Get historical features for training with point-in-time correctness."""
        try:
            if not self.config.offline_serving:
                raise ValueError("Offline serving is disabled")

            # Group by entity for efficient querying
            entity_groups: dict[str, list[datetime]] = {}
            for entity_id, timestamp in entity_timestamps:
                if entity_id not in entity_groups:
                    entity_groups[entity_id] = []
                entity_groups[entity_id].append(timestamp)

            all_results = []

            for entity_id, timestamps in entity_groups.items():
                for timestamp in timestamps:
                    if point_in_time_accuracy:
                        features = self._get_point_in_time_features(
                            feature_names, entity_id, timestamp
                        )
                    else:
                        # Faster query without strict point-in-time guarantees
                        features = self._get_latest_features_before(
                            feature_names, entity_id, timestamp
                        )

                    if not features.empty:
                        features["entity_id"] = entity_id
                        features["as_of_timestamp"] = timestamp
                        all_results.append(features)

            if not all_results:
                # Return empty DataFrame with correct columns
                columns = ["entity_id", "as_of_timestamp"] + feature_names
                return pd.DataFrame(columns=columns)

            result = pd.concat(all_results, ignore_index=True)

            logger.info(
                "Historical features retrieved",
                feature_count=len(feature_names),
                entity_timestamp_pairs=len(entity_timestamps),
                result_rows=len(result),
                point_in_time_accuracy=point_in_time_accuracy,
            )

            return result

        except Exception as error:
            logger.error(
                "Failed to get historical features",
                feature_names=feature_names[:5],
                entity_timestamp_count=len(entity_timestamps),
                error=str(error),
            )
            raise

    def _get_point_in_time_features(
        self,
        feature_names: list[str],
        entity_id: str,
        as_of_time: datetime,
    ) -> pd.DataFrame:
        """Get features with strict point-in-time correctness."""
        try:
            session = self.SessionLocal()

            # For each feature, get the latest value before as_of_time
            feature_data = {}

            for feature_name in feature_names:
                query = (
                    session.query(FeatureValueModel)
                    .filter(
                        FeatureValueModel.feature_name == feature_name,
                        FeatureValueModel.entity_id == entity_id,
                        FeatureValueModel.event_timestamp <= as_of_time,
                        # Ensure the feature was valid at the as_of_time
                        FeatureValueModel.valid_from <= as_of_time,
                        (FeatureValueModel.valid_until.is_(None))
                        | (FeatureValueModel.valid_until > as_of_time),
                    )
                    .order_by(FeatureValueModel.event_timestamp.desc())
                    .first()
                )

                if query:
                    try:
                        value = json.loads(str(query.feature_value))
                    except (json.JSONDecodeError, TypeError):
                        value = query.feature_value
                    feature_data[feature_name] = value
                else:
                    feature_data[feature_name] = None

            session.close()

            # Convert to DataFrame
            if feature_data:
                return pd.DataFrame([feature_data])
            else:
                return pd.DataFrame(columns=feature_names)

        except Exception as error:
            logger.error(
                "Point-in-time feature retrieval failed",
                feature_names=feature_names,
                entity_id=entity_id,
                as_of_time=as_of_time,
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
                    "timestamp": fv.event_timestamp,
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

    def _get_cached_features_fast(
        self,
        feature_names: list[str],
        entity_ids: list[str],
    ) -> pd.DataFrame | None:
        """Fast cache lookup optimized for online serving."""
        try:
            if self.cache_client:  # Redis cache
                return self._get_redis_cached_features(feature_names, entity_ids)
            else:  # Memory cache
                return self._get_cached_features(feature_names, entity_ids, None)
        except Exception as error:
            logger.warning("Fast cache lookup failed", error=str(error))
            return None

    def _get_redis_cached_features(
        self,
        feature_names: list[str],
        entity_ids: list[str],
    ) -> pd.DataFrame | None:
        """Get features from Redis cache."""
        try:
            if not self.cache_client:
                return None

            current_time = datetime.now(timezone.utc)
            cache_cutoff = current_time - timedelta(
                seconds=self.config.feature_cache_ttl_seconds
            )

            rows = []
            all_found = True

            for entity_id in entity_ids:
                row = {"entity_id": entity_id}
                for feature_name in feature_names:
                    cache_key = f"feature:{feature_name}:{entity_id}"

                    cached_data = self.cache_client.get(cache_key)
                    if cached_data:
                        cached = json.loads(cached_data)

                        # Check cache freshness
                        cached_at = datetime.fromisoformat(cached["cached_at"])
                        if cached_at < cache_cutoff:
                            all_found = False
                            break

                        row[feature_name] = cached["value"]
                    else:
                        all_found = False
                        break

                if not all_found:
                    break

                rows.append(row)

            if all_found and rows:
                return pd.DataFrame(rows)

            return None

        except Exception as error:
            logger.warning("Redis cache lookup failed", error=str(error))
            return None

    def _read_features_with_timeout(
        self,
        feature_names: list[str],
        entity_ids: list[str],
        as_of_time: datetime | None,
        timeout_ms: int,
    ) -> pd.DataFrame:
        """Read features with timeout for online serving."""
        import signal

        class TimeoutError(Exception):
            pass

        def timeout_handler(signum, frame):
            raise TimeoutError("Feature read timeout")

        try:
            # Set timeout
            signal.signal(signal.SIGALRM, timeout_handler)
            signal.alarm(timeout_ms // 1000)  # Convert to seconds

            # Read features
            result = self.read_features(feature_names, entity_ids, as_of_time)

            # Clear timeout
            signal.alarm(0)

            return result

        except TimeoutError:
            logger.error(
                "Feature read timeout",
                timeout_ms=timeout_ms,
                feature_count=len(feature_names),
                entity_count=len(entity_ids),
            )
            # Return empty DataFrame as fallback
            columns = ["entity_id"] + feature_names
            return pd.DataFrame(columns=columns)

        finally:
            # Ensure timeout is cleared
            signal.alarm(0)

    def _get_latest_features_before(
        self,
        feature_names: list[str],
        entity_id: str,
        before_time: datetime,
    ) -> pd.DataFrame:
        """Get latest features before a timestamp (faster, less strict)."""
        try:
            session = self.SessionLocal()

            feature_data = {}

            for feature_name in feature_names:
                query = (
                    session.query(FeatureValueModel)
                    .filter(
                        FeatureValueModel.feature_name == feature_name,
                        FeatureValueModel.entity_id == entity_id,
                        FeatureValueModel.created_at <= before_time,
                    )
                    .order_by(FeatureValueModel.created_at.desc())
                    .first()
                )

                if query:
                    try:
                        value = json.loads(str(query.feature_value))
                    except (json.JSONDecodeError, TypeError):
                        value = query.feature_value
                    feature_data[feature_name] = value
                else:
                    feature_data[feature_name] = None

            session.close()

            if feature_data:
                return pd.DataFrame([feature_data])
            else:
                return pd.DataFrame(columns=feature_names)

        except Exception as error:
            logger.error(
                "Latest features retrieval failed",
                feature_names=feature_names,
                entity_id=entity_id,
                before_time=before_time,
                error=str(error),
            )
            raise

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

    def detect_feature_drift(
        self,
        feature_name: str,
        reference_data: pd.DataFrame,
        current_data: pd.DataFrame,
    ) -> dict[str, Any]:
        """Detect drift in feature values."""
        try:
            if not self.config.enable_drift_detection:
                raise ValueError("Feature drift detection is disabled")

            # Initialize drift detector if needed
            if self._drift_detector is None:
                try:
                    from .monitoring import ModelMonitor, ModelMonitoringConfig

                    monitor_config = ModelMonitoringConfig(
                        drift_threshold=0.05,
                        min_samples_for_drift=self.config.drift_detection_sample_size,
                    )
                    self._drift_detector = ModelMonitor(monitor_config)
                except ImportError:
                    raise ValueError("ModelMonitor not available for drift detection")

            # Set reference data
            self._drift_detector.set_reference_data(feature_name, reference_data)

            # Detect drift
            drift_results = self._drift_detector.detect_data_drift(
                model_name=feature_name,
                current_data=current_data,
                feature_subset=[feature_name]
                if feature_name in current_data.columns
                else None,
            )

            if drift_results:
                drift_result = drift_results[0]
                return {
                    "feature_name": feature_name,
                    "drift_detected": drift_result.drift_detected,
                    "drift_magnitude": drift_result.drift_magnitude,
                    "p_value": drift_result.p_value,
                    "statistic": drift_result.statistic,
                    "method": drift_result.detection_method,
                    "severity": drift_result.severity,
                    "recommendation": drift_result.recommended_action,
                    "reference_samples": drift_result.reference_samples,
                    "current_samples": drift_result.current_samples,
                }
            else:
                return {
                    "feature_name": feature_name,
                    "drift_detected": False,
                    "message": "Insufficient data for drift detection",
                }

        except Exception as error:
            logger.error(
                "Feature drift detection failed",
                feature_name=feature_name,
                error=str(error),
            )
            raise

    def get_feature_lineage(self, feature_name: str) -> dict[str, Any]:
        """Get lineage information for a feature."""
        try:
            if not self.lineage_tracker:
                raise ValueError("Lineage tracking is disabled")

            # Find feature asset
            asset = self.lineage_tracker.get_asset_by_name(feature_name)
            if not asset:
                return {
                    "feature_name": feature_name,
                    "message": "Feature not found in lineage tracker",
                }

            # Get upstream and downstream features
            upstream_assets = self.lineage_tracker.get_upstream_assets(asset.id)
            downstream_assets = self.lineage_tracker.get_downstream_assets(asset.id)

            # Get impact analysis
            impact_analysis = self.lineage_tracker.analyze_impact(asset.id)

            return {
                "feature_name": feature_name,
                "asset_id": asset.id,
                "upstream_features": [
                    self.lineage_tracker.graph.assets[asset_id].name
                    for asset_id in upstream_assets
                    if asset_id in self.lineage_tracker.graph.assets
                ],
                "downstream_features": [
                    self.lineage_tracker.graph.assets[asset_id].name
                    for asset_id in downstream_assets
                    if asset_id in self.lineage_tracker.graph.assets
                ],
                "impact_analysis": impact_analysis,
            }

        except Exception as error:
            logger.error(
                "Failed to get feature lineage",
                feature_name=feature_name,
                error=str(error),
            )
            raise

    def get_feature_discovery_info(self) -> dict[str, Any]:
        """Get information for feature discovery portal."""
        try:
            if not self.config.enable_feature_discovery:
                raise ValueError("Feature discovery is disabled")

            session = self.SessionLocal()

            # Get feature statistics
            total_features = session.query(FeatureDefinitionModel).count()

            # Features by type
            from sqlalchemy import func

            feature_type_counts = (
                session.query(
                    FeatureDefinitionModel.feature_type,
                    func.count(FeatureDefinitionModel.id).label("count"),
                )
                .group_by(FeatureDefinitionModel.feature_type)
                .all()
            )

            # Recent features (last 30 days)
            thirty_days_ago = datetime.now(timezone.utc) - timedelta(days=30)
            recent_features = (
                session.query(FeatureDefinitionModel)
                .filter(FeatureDefinitionModel.created_at > thirty_days_ago)
                .count()
            )

            # Top owners
            owner_counts = (
                session.query(
                    FeatureDefinitionModel.owner,
                    func.count(FeatureDefinitionModel.id).label("count"),
                )
                .filter(FeatureDefinitionModel.owner.isnot(None))
                .group_by(FeatureDefinitionModel.owner)
                .order_by(func.count(FeatureDefinitionModel.id).desc())
                .limit(10)
                .all()
            )

            # Feature usage stats (would need additional tracking in production)
            # For now, return mock data
            popular_features = [
                {"name": "user_age", "usage_count": 150, "last_used": "2024-01-20"},
                {
                    "name": "transaction_amount",
                    "usage_count": 120,
                    "last_used": "2024-01-19",
                },
                {
                    "name": "user_location",
                    "usage_count": 100,
                    "last_used": "2024-01-18",
                },
            ]

            session.close()

            return {
                "total_features": total_features,
                "features_by_type": {
                    result.feature_type: result.count for result in feature_type_counts
                },
                "recent_features_30d": recent_features,
                "top_owners": [
                    {"owner": result.owner, "feature_count": result.count}
                    for result in owner_counts
                ],
                "popular_features": popular_features,
                "feature_store_health": {
                    "online_serving_enabled": self.config.online_serving,
                    "offline_serving_enabled": self.config.offline_serving,
                    "caching_enabled": self.config.enable_feature_caching,
                    "drift_detection_enabled": self.config.enable_drift_detection,
                    "lineage_tracking_enabled": self.config.enable_lineage_tracking,
                },
                "last_updated": datetime.now(timezone.utc).isoformat(),
            }

        except Exception as error:
            logger.error("Failed to get feature discovery info", error=str(error))
            raise

    def batch_compute_features(
        self,
        transformation_name: str,
        entity_df: pd.DataFrame,
        start_date: datetime,
        end_date: datetime,
        chunk_size: int | None = None,
    ) -> int:
        """Batch compute features using a transformation."""
        try:
            chunk_size = chunk_size or self.config.batch_serving_chunk_size

            if transformation_name not in self.transformations:
                raise ValueError(f"Transformation '{transformation_name}' not found")

            transformation = self.transformations[transformation_name]
            total_processed = 0

            # Process in chunks for memory efficiency
            for i in range(0, len(entity_df), chunk_size):
                chunk = entity_df.iloc[i : i + chunk_size].copy()

                # Add time range filter if applicable
                chunk["batch_start_date"] = start_date
                chunk["batch_end_date"] = end_date

                # Execute transformation
                result_df = self.execute_transformation(
                    transformation_name,
                    chunk,
                    {"start_date": start_date, "end_date": end_date},
                )

                # Convert to feature values and write
                feature_values = []
                for _, row in result_df.iterrows():
                    entity_id = row.get("entity_id", str(i))

                    for feature_name in transformation.output_features:
                        if feature_name in row:
                            feature_value = FeatureValue(
                                feature_name=feature_name,
                                entity_id=entity_id,
                                value=row[feature_name],
                                event_timestamp=row.get(
                                    "event_timestamp", datetime.now(timezone.utc)
                                ),
                                source=f"batch:{transformation_name}",
                            )
                            feature_values.append(feature_value)

                # Write features
                if feature_values:
                    written_count = self.write_features(feature_values)
                    total_processed += written_count

                logger.info(
                    "Batch chunk processed",
                    transformation=transformation_name,
                    chunk_start=i,
                    chunk_size=len(chunk),
                    features_written=len(feature_values),
                )

            logger.info(
                "Batch computation completed",
                transformation=transformation_name,
                total_entities=len(entity_df),
                total_features_written=total_processed,
                date_range=f"{start_date} to {end_date}",
            )

            return total_processed

        except Exception as error:
            logger.error(
                "Batch feature computation failed",
                transformation=transformation_name,
                error=str(error),
            )
            raise
