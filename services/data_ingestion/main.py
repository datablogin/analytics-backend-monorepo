"""Data ingestion service with quality validation checkpoints."""

import asyncio
import json
from datetime import datetime
from typing import Any

import pandas as pd
import structlog
from celery import Celery
from pydantic import BaseModel
from redis import Redis

from libs.data_processing import (
    DataLineageTracker,
    DataProfiler,
    DataQualityConfig,
    DataQualityFramework,
    create_common_expectations,
)

logger = structlog.get_logger(__name__)

# Initialize components
celery_app = Celery("data_ingestion")
redis_client = Redis(host="localhost", port=6379, db=0)
dq_framework = DataQualityFramework(DataQualityConfig())
profiler = DataProfiler()
lineage_tracker = DataLineageTracker()


class IngestionConfig(BaseModel):
    """Configuration for data ingestion."""

    enable_validation: bool = True
    enable_profiling: bool = True
    enable_lineage: bool = True
    validation_suite: str = "default_suite"
    fail_on_validation_error: bool = False
    max_validation_failures: int = 10


class IngestionRequest(BaseModel):
    """Data ingestion request."""

    dataset_name: str
    source_type: str  # file, api, database, stream
    source_location: str
    target_location: str
    transformation_config: dict[str, Any] | None = None
    validation_config: IngestionConfig | None = None


class IngestionResult(BaseModel):
    """Result of data ingestion process."""

    request_id: str
    dataset_name: str
    status: str  # success, failed, warning
    start_time: datetime
    end_time: datetime
    records_processed: int
    validation_result: dict[str, Any] | None = None
    profile_result: dict[str, Any] | None = None
    lineage_info: dict[str, Any] | None = None
    errors: list[str] = []
    warnings: list[str] = []


class DataIngestionService:
    """Data ingestion service with quality checkpoints."""

    def __init__(self):
        self.logger = structlog.get_logger(__name__)

    async def ingest_data(self, request: IngestionRequest) -> IngestionResult:
        """Main data ingestion pipeline with quality checkpoints."""
        start_time = datetime.utcnow()
        request_id = f"ingest_{int(start_time.timestamp())}"
        config = request.validation_config or IngestionConfig()

        result = IngestionResult(
            request_id=request_id,
            dataset_name=request.dataset_name,
            status="in_progress",
            start_time=start_time,
            end_time=start_time,
            records_processed=0,
        )

        try:
            self.logger.info(
                "Starting data ingestion",
                request_id=request_id,
                dataset_name=request.dataset_name,
                source_type=request.source_type,
            )

            # Step 1: Load data
            df = await self._load_data(request)
            result.records_processed = len(df)

            # Step 2: Pre-ingestion validation
            if config.enable_validation:
                validation_result = await self._validate_data(
                    df, request.dataset_name, config
                )
                result.validation_result = validation_result.model_dump()

                if not validation_result.success and config.fail_on_validation_error:
                    result.status = "failed"
                    result.errors.append("Data validation failed")
                    return result
                elif not validation_result.success:
                    result.warnings.append("Data validation warnings detected")

            # Step 3: Data profiling
            if config.enable_profiling:
                profile_result = await self._profile_data(df, request.dataset_name)
                result.profile_result = profile_result.model_dump()

            # Step 4: Register lineage
            if config.enable_lineage:
                lineage_info = await self._register_lineage(request)
                result.lineage_info = lineage_info

            # Step 5: Store data
            await self._store_data(df, request.target_location)

            # Step 6: Post-ingestion tasks
            await self._post_ingestion_tasks(request, result)

            result.status = "success"
            result.end_time = datetime.utcnow()

            self.logger.info(
                "Data ingestion completed",
                request_id=request_id,
                dataset_name=request.dataset_name,
                records_processed=result.records_processed,
                status=result.status,
            )

        except Exception as e:
            result.status = "failed"
            result.end_time = datetime.utcnow()
            result.errors.append(str(e))

            self.logger.error(
                "Data ingestion failed",
                request_id=request_id,
                dataset_name=request.dataset_name,
                error=str(e),
            )

        return result

    async def _load_data(self, request: IngestionRequest) -> pd.DataFrame:
        """Load data from source."""
        if request.source_type == "file":
            if request.source_location.endswith(".csv"):
                return pd.read_csv(request.source_location)
            elif request.source_location.endswith(".parquet"):
                return pd.read_parquet(request.source_location)
            elif request.source_location.endswith(".json"):
                return pd.read_json(request.source_location)
            else:
                raise ValueError(f"Unsupported file format: {request.source_location}")
        elif request.source_type == "database":
            # Implementation would depend on database connection
            raise NotImplementedError("Database ingestion not implemented")
        elif request.source_type == "api":
            # Implementation would depend on API specification
            raise NotImplementedError("API ingestion not implemented")
        else:
            raise ValueError(f"Unsupported source type: {request.source_type}")

    async def _validate_data(
        self, df: pd.DataFrame, dataset_name: str, config: IngestionConfig
    ):
        """Validate data using Great Expectations."""
        # Create or get expectation suite
        suite_name = f"{dataset_name}_{config.validation_suite}"

        try:
            # Try to get existing suite - simplified check
            if suite_name not in dq_framework.expectation_suites:
                raise ValueError("Suite not found")
        except Exception:
            # Create default expectations if suite doesn't exist
            expectations = create_common_expectations(
                table_name=dataset_name,
                columns=df.columns.tolist(),
                primary_key_columns=None,  # Would be configured per dataset
                not_null_columns=None,  # Would be configured per dataset
                unique_columns=None,  # Would be configured per dataset
            )
            dq_framework.create_expectation_suite(suite_name, expectations)

        return dq_framework.validate_dataframe(df, suite_name, dataset_name)

    async def _profile_data(self, df: pd.DataFrame, dataset_name: str):
        """Profile data for statistical analysis."""
        return profiler.profile_dataframe(df, dataset_name)

    async def _register_lineage(self, request: IngestionRequest) -> dict[str, Any]:
        """Register data lineage information."""
        # Register source asset
        source_id = lineage_tracker.register_asset(
            name=request.source_location,
            asset_type=request.source_type,
            location=request.source_location,
        )

        # Register target asset
        target_id = lineage_tracker.register_asset(
            name=request.dataset_name,
            asset_type="table",
            location=request.target_location,
        )

        # Register transformation
        transformation_id = lineage_tracker.register_transformation(
            name=f"ingest_{request.dataset_name}",
            transformation_type="ingestion",
            description=f"Data ingestion from {request.source_type} to table",
            parameters=request.transformation_config or {},
        )

        # Add lineage relationship
        lineage_tracker.add_lineage(source_id, target_id, transformation_id)

        return {
            "source_asset_id": source_id,
            "target_asset_id": target_id,
            "transformation_id": transformation_id,
        }

    async def _store_data(self, df: pd.DataFrame, target_location: str):
        """Store processed data to target location."""
        if target_location.endswith(".csv"):
            df.to_csv(target_location, index=False)
        elif target_location.endswith(".parquet"):
            df.to_parquet(target_location, index=False)
        elif target_location.endswith(".json"):
            df.to_json(target_location, orient="records")
        else:
            # For databases, would use SQLAlchemy
            raise NotImplementedError("Database storage not implemented")

    async def _post_ingestion_tasks(
        self, request: IngestionRequest, result: IngestionResult
    ):
        """Execute post-ingestion tasks."""
        # Store ingestion metadata
        metadata = {
            "request_id": result.request_id,
            "dataset_name": request.dataset_name,
            "timestamp": result.end_time.isoformat(),
            "records_processed": result.records_processed,
            "status": result.status,
        }

        redis_client.setex(
            f"ingestion:{result.request_id}",
            3600 * 24,  # 24 hours TTL
            json.dumps(metadata),
        )

        # Trigger downstream notifications if configured
        if result.validation_result and not result.validation_result["success"]:
            await self._send_quality_alert(request, result)

    async def _send_quality_alert(
        self, request: IngestionRequest, result: IngestionResult
    ):
        """Send data quality alerts."""
        alert_data = {
            "type": "data_quality_violation",
            "dataset": request.dataset_name,
            "timestamp": result.end_time.isoformat(),
            "validation_failures": result.validation_result["failure_count"],
            "success_rate": result.validation_result["success_rate"],
        }

        # In a real implementation, this would integrate with alerting systems
        # like Slack, PagerDuty, email, etc.
        self.logger.warning("Data quality alert", **alert_data)


# Celery tasks for async processing
@celery_app.task
def process_ingestion_async(request_data: dict[str, Any]) -> dict[str, Any]:
    """Async task for data ingestion processing."""
    service = DataIngestionService()
    request = IngestionRequest(**request_data)

    # Run async function in sync context
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    result = loop.run_until_complete(service.ingest_data(request))
    loop.close()

    return result.model_dump()


@celery_app.task
def validate_dataset_async(
    dataset_path: str, dataset_name: str, suite_name: str = "default_suite"
) -> dict[str, Any]:
    """Async task for standalone dataset validation."""
    df = pd.read_csv(dataset_path)  # Simplified - would handle multiple formats

    validation_result = dq_framework.validate_dataframe(df, suite_name, dataset_name)
    return validation_result.model_dump()


@celery_app.task
def profile_dataset_async(dataset_path: str, dataset_name: str) -> dict[str, Any]:
    """Async task for standalone dataset profiling."""
    df = pd.read_csv(dataset_path)  # Simplified - would handle multiple formats

    profile_result = profiler.profile_dataframe(df, dataset_name)
    return profile_result.model_dump()


if __name__ == "__main__":
    # Example usage
    import asyncio

    async def main():
        service = DataIngestionService()

        request = IngestionRequest(
            dataset_name="sample_dataset",
            source_type="file",
            source_location="sample_data.csv",
            target_location="processed/sample_dataset.parquet",
        )

        result = await service.ingest_data(request)
        print(f"Ingestion result: {result.status}")
        print(f"Records processed: {result.records_processed}")

    asyncio.run(main())
