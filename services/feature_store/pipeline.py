"""Feature pipeline orchestration and batch processing."""

import asyncio
import json
from datetime import datetime
from enum import Enum
from typing import Any

import pandas as pd
import structlog
from libs.analytics_core.models import BaseModel as SQLBaseModel
from libs.observability.metrics import MLOpsMetrics
from pydantic import BaseModel, Field
from sqlalchemy import Column, DateTime, Integer, String, Text
from sqlalchemy.ext.asyncio import AsyncSession

from .core import FeatureStoreService
from .models import FeatureValueWrite

logger = structlog.get_logger(__name__)


class PipelineStatus(str, Enum):
    """Pipeline execution status."""

    PENDING = "pending"
    RUNNING = "running"
    SUCCESS = "success"
    FAILED = "failed"
    CANCELLED = "cancelled"


class PipelineType(str, Enum):
    """Pipeline type."""

    BATCH = "batch"
    STREAMING = "streaming"
    SCHEDULED = "scheduled"


class FeaturePipelineRun(SQLBaseModel):
    """Feature pipeline run tracking model."""

    __tablename__ = "feature_pipeline_runs"

    pipeline_name: str = Column(String(255), nullable=False, index=True)
    pipeline_type: str = Column(String(50), nullable=False)
    status: str = Column(String(50), nullable=False, default=PipelineStatus.PENDING)
    config: str = Column(Text, nullable=True)  # JSON string
    start_time: datetime = Column(DateTime(timezone=True), nullable=True)
    end_time: datetime = Column(DateTime(timezone=True), nullable=True)
    features_processed: int = Column(Integer, default=0)
    records_processed: int = Column(Integer, default=0)
    error_message: str = Column(Text, nullable=True)
    metrics: str = Column(Text, nullable=True)  # JSON string


class PipelineConfig(BaseModel):
    """Pipeline configuration."""

    name: str = Field(..., description="Pipeline name")
    pipeline_type: PipelineType = Field(..., description="Pipeline type")
    source_config: dict[str, Any] = Field(..., description="Data source configuration")
    feature_transformations: list[dict[str, Any]] = Field(
        default_factory=list, description="Feature transformation definitions"
    )
    schedule: str | None = Field(
        None, description="Cron schedule for scheduled pipelines"
    )
    batch_size: int = Field(default=1000, description="Batch size for processing")
    timeout_seconds: int = Field(default=3600, description="Pipeline timeout")
    retry_attempts: int = Field(default=3, description="Number of retry attempts")
    tags: list[str] = Field(default_factory=list, description="Pipeline tags")


class PipelineExecutionResult(BaseModel):
    """Pipeline execution result."""

    run_id: int = Field(..., description="Pipeline run ID")
    pipeline_name: str = Field(..., description="Pipeline name")
    status: PipelineStatus = Field(..., description="Execution status")
    start_time: datetime = Field(..., description="Start time")
    end_time: datetime | None = Field(None, description="End time")
    features_processed: int = Field(
        default=0, description="Number of features processed"
    )
    records_processed: int = Field(default=0, description="Number of records processed")
    execution_time_seconds: float | None = Field(
        None, description="Total execution time"
    )
    error_message: str | None = Field(None, description="Error message if failed")
    metrics: dict[str, Any] = Field(
        default_factory=dict, description="Execution metrics"
    )


class FeaturePipelineOrchestrator:
    """Feature pipeline orchestration engine."""

    def __init__(self, feature_store_service: FeatureStoreService):
        """Initialize pipeline orchestrator."""
        self.feature_store_service = feature_store_service
        self.metrics = MLOpsMetrics()
        self._running_pipelines: dict[str, asyncio.Task] = {}
        self._pipeline_configs: dict[str, PipelineConfig] = {}

    async def register_pipeline(self, config: PipelineConfig) -> str:
        """Register a new feature pipeline."""
        try:
            self._pipeline_configs[config.name] = config

            logger.info(
                "Pipeline registered",
                pipeline_name=config.name,
                pipeline_type=config.pipeline_type,
                features=len(config.feature_transformations),
            )

            return config.name

        except Exception as error:
            logger.error(
                "Failed to register pipeline",
                pipeline_name=config.name,
                error=str(error),
            )
            raise

    async def execute_pipeline(
        self, pipeline_name: str, override_config: dict[str, Any] | None = None
    ) -> PipelineExecutionResult:
        """Execute a feature pipeline."""
        if pipeline_name not in self._pipeline_configs:
            raise ValueError(f"Pipeline '{pipeline_name}' not found")

        config = self._pipeline_configs[pipeline_name]

        # Create pipeline run record
        async with self.feature_store_service.db_manager.get_session() as session:
            run = FeaturePipelineRun(
                pipeline_name=pipeline_name,
                pipeline_type=config.pipeline_type.value,
                config=json.dumps(config.model_dump()),
                start_time=datetime.utcnow(),
            )
            session.add(run)
            await session.commit()
            await session.refresh(run)
            run_id = run.id

        try:
            logger.info(
                "Starting pipeline execution",
                pipeline_name=pipeline_name,
                run_id=run_id,
                pipeline_type=config.pipeline_type,
            )

            start_time = datetime.utcnow()

            # Execute based on pipeline type
            if config.pipeline_type == PipelineType.BATCH:
                result = await self._execute_batch_pipeline(config, run_id, session)
            elif config.pipeline_type == PipelineType.STREAMING:
                result = await self._execute_streaming_pipeline(config, run_id, session)
            else:
                raise ValueError(f"Unsupported pipeline type: {config.pipeline_type}")

            end_time = datetime.utcnow()
            execution_time = (end_time - start_time).total_seconds()

            # Update run status
            await self._update_pipeline_run(
                session,
                run_id,
                PipelineStatus.SUCCESS,
                end_time=end_time,
                features_processed=result["features_processed"],
                records_processed=result["records_processed"],
                metrics=result.get("metrics", {}),
            )

            logger.info(
                "Pipeline execution completed",
                pipeline_name=pipeline_name,
                run_id=run_id,
                execution_time_seconds=execution_time,
                features_processed=result["features_processed"],
                records_processed=result["records_processed"],
            )

            return PipelineExecutionResult(
                run_id=run_id,
                pipeline_name=pipeline_name,
                status=PipelineStatus.SUCCESS,
                start_time=start_time,
                end_time=end_time,
                features_processed=result["features_processed"],
                records_processed=result["records_processed"],
                execution_time_seconds=execution_time,
                metrics=result.get("metrics", {}),
            )

        except Exception as error:
            logger.error(
                "Pipeline execution failed",
                pipeline_name=pipeline_name,
                run_id=run_id,
                error=str(error),
            )

            # Update run status with error
            await self._update_pipeline_run(
                session,
                run_id,
                PipelineStatus.FAILED,
                end_time=datetime.utcnow(),
                error_message=str(error),
            )

            return PipelineExecutionResult(
                run_id=run_id,
                pipeline_name=pipeline_name,
                status=PipelineStatus.FAILED,
                start_time=start_time,
                end_time=datetime.utcnow(),
                error_message=str(error),
            )

    async def _execute_batch_pipeline(
        self, config: PipelineConfig, run_id: int, session: AsyncSession
    ) -> dict[str, Any]:
        """Execute batch feature pipeline."""
        features_processed = 0
        records_processed = 0
        metrics = {}

        try:
            # Load data from source
            data = await self._load_data_from_source(config.source_config)
            logger.info(
                "Data loaded from source",
                records=len(data),
                pipeline_name=config.name,
                run_id=run_id,
            )

            # Process in batches
            batch_size = config.batch_size
            feature_values = []

            for i in range(0, len(data), batch_size):
                batch_data = data.iloc[i : i + batch_size]

                # Apply feature transformations
                batch_features = await self._apply_transformations(
                    batch_data, config.feature_transformations
                )

                feature_values.extend(batch_features)
                records_processed += len(batch_data)

                # Update progress periodically
                if records_processed % (batch_size * 10) == 0:
                    await self._update_pipeline_run(
                        session,
                        run_id,
                        PipelineStatus.RUNNING,
                        records_processed=records_processed,
                    )

            # Write features to store
            if feature_values:
                await self.feature_store_service.write_feature_values_batch(
                    feature_values
                )
                features_processed = len(feature_values)

            metrics = {
                "data_source_records": len(data),
                "batch_count": (len(data) + batch_size - 1) // batch_size,
                "avg_features_per_record": features_processed / records_processed
                if records_processed > 0
                else 0,
            }

            return {
                "features_processed": features_processed,
                "records_processed": records_processed,
                "metrics": metrics,
            }

        except Exception as error:
            logger.error(
                "Batch pipeline execution failed",
                pipeline_name=config.name,
                run_id=run_id,
                error=str(error),
            )
            raise

    async def _execute_streaming_pipeline(
        self, config: PipelineConfig, run_id: int, session: AsyncSession
    ) -> dict[str, Any]:
        """Execute streaming feature pipeline."""
        # Placeholder for streaming pipeline implementation
        # In a real implementation, this would connect to streaming sources
        # like Kafka, Kinesis, or PubSub

        logger.info(
            "Streaming pipeline started",
            pipeline_name=config.name,
            run_id=run_id,
        )

        # Simulate streaming processing
        await asyncio.sleep(1)

        return {
            "features_processed": 0,
            "records_processed": 0,
            "metrics": {"streaming": True},
        }

    async def _load_data_from_source(
        self, source_config: dict[str, Any]
    ) -> pd.DataFrame:
        """Load data from configured source."""
        source_type = source_config.get("type", "")

        if source_type == "csv":
            file_path = source_config.get("file_path")
            if not file_path:
                raise ValueError("CSV source requires file_path")
            return pd.read_csv(file_path)

        elif source_type == "database":
            # Placeholder for database source
            connection_string = source_config.get("connection_string")
            query = source_config.get("query")
            if not connection_string or not query:
                raise ValueError("Database source requires connection_string and query")
            # In real implementation, use pandas.read_sql
            return pd.DataFrame()

        elif source_type == "api":
            # Placeholder for API source
            url = source_config.get("url")
            if not url:
                raise ValueError("API source requires url")
            # In real implementation, fetch from API and convert to DataFrame
            return pd.DataFrame()

        else:
            raise ValueError(f"Unsupported source type: {source_type}")

    async def _apply_transformations(
        self, data: pd.DataFrame, transformations: list[dict[str, Any]]
    ) -> list[FeatureValueWrite]:
        """Apply feature transformations to data."""
        feature_values = []

        for _, row in data.iterrows():
            entity_id = str(
                row.get("entity_id", row.iloc[0])
            )  # Use first column as entity_id if not specified
            timestamp = datetime.utcnow()

            for transformation in transformations:
                feature_name = transformation.get("feature_name")
                expression = transformation.get("expression")

                if not feature_name or not expression:
                    continue

                try:
                    # Simple expression evaluation (in production, use safer evaluation)
                    # This is a simplified version - production should use proper expression parser
                    value = self._evaluate_expression(expression, row)

                    feature_values.append(
                        FeatureValueWrite(
                            feature_name=feature_name,
                            entity_id=entity_id,
                            value=value,
                            timestamp=timestamp,
                        )
                    )

                except Exception as error:
                    logger.warning(
                        "Feature transformation failed",
                        feature_name=feature_name,
                        expression=expression,
                        entity_id=entity_id,
                        error=str(error),
                    )

        return feature_values

    def _evaluate_expression(self, expression: str, row: pd.Series) -> Any:
        """Evaluate feature transformation expression."""
        # This is a simplified implementation
        # Production should use a proper expression parser like ast.literal_eval
        # or a domain-specific language

        # Replace column references with actual values
        for col_name in row.index:
            expression = expression.replace(f"${col_name}", str(row[col_name]))

        # Simple mathematical expressions
        try:
            # Basic safety check - only allow certain characters
            allowed_chars = set("0123456789+-*/.() ")
            if all(c in allowed_chars or c.isalnum() for c in expression):
                return eval(expression)  # Note: eval is dangerous in production
            else:
                return expression  # Return as string if complex
        except Exception:
            return row.iloc[0]  # Return first column value as fallback

    async def _update_pipeline_run(
        self,
        session: AsyncSession,
        run_id: int,
        status: PipelineStatus,
        end_time: datetime | None = None,
        features_processed: int | None = None,
        records_processed: int | None = None,
        error_message: str | None = None,
        metrics: dict[str, Any] | None = None,
    ) -> None:
        """Update pipeline run status."""
        try:
            # Get the run record
            run = await session.get(FeaturePipelineRun, run_id)
            if run:
                run.status = status.value
                if end_time:
                    run.end_time = end_time
                if features_processed is not None:
                    run.features_processed = features_processed
                if records_processed is not None:
                    run.records_processed = records_processed
                if error_message:
                    run.error_message = error_message
                if metrics:
                    run.metrics = json.dumps(metrics)

                await session.commit()

        except Exception as error:
            logger.error(
                "Failed to update pipeline run",
                run_id=run_id,
                error=str(error),
            )

    async def get_pipeline_runs(
        self,
        pipeline_name: str | None = None,
        status: PipelineStatus | None = None,
        limit: int = 100,
    ) -> list[PipelineExecutionResult]:
        """Get pipeline execution history."""
        async with self.feature_store_service.db_manager.get_session() as session:
            from sqlalchemy import select

            stmt = select(FeaturePipelineRun)

            if pipeline_name:
                stmt = stmt.where(FeaturePipelineRun.pipeline_name == pipeline_name)
            if status:
                stmt = stmt.where(FeaturePipelineRun.status == status.value)

            stmt = stmt.order_by(FeaturePipelineRun.created_at.desc()).limit(limit)

            result = await session.execute(stmt)
            runs = result.scalars().all()

            return [
                PipelineExecutionResult(
                    run_id=run.id,
                    pipeline_name=run.pipeline_name,
                    status=PipelineStatus(run.status),
                    start_time=run.start_time or run.created_at,
                    end_time=run.end_time,
                    features_processed=run.features_processed,
                    records_processed=run.records_processed,
                    execution_time_seconds=(
                        (
                            run.end_time - (run.start_time or run.created_at)
                        ).total_seconds()
                        if run.end_time and run.start_time
                        else None
                    ),
                    error_message=run.error_message,
                    metrics=json.loads(run.metrics) if run.metrics else {},
                )
                for run in runs
            ]

    async def cancel_pipeline(self, pipeline_name: str) -> bool:
        """Cancel a running pipeline."""
        if pipeline_name in self._running_pipelines:
            task = self._running_pipelines[pipeline_name]
            task.cancel()
            del self._running_pipelines[pipeline_name]

            logger.info(
                "Pipeline cancelled",
                pipeline_name=pipeline_name,
            )

            return True

        return False

    def get_registered_pipelines(self) -> list[str]:
        """Get list of registered pipeline names."""
        return list(self._pipeline_configs.keys())

    def get_pipeline_config(self, pipeline_name: str) -> PipelineConfig | None:
        """Get pipeline configuration."""
        return self._pipeline_configs.get(pipeline_name)
