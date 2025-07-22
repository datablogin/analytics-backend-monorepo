"""Advanced Batch Processing Service with Workflow Orchestration.

This service implements sophisticated pipeline orchestration using the workflow
orchestration library, providing DAG-based workflow execution, intelligent
retry mechanisms, resource management, and comprehensive monitoring.
"""

import asyncio
import os
import signal
import sys
from datetime import UTC, datetime  # type: ignore[attr-defined]
from pathlib import Path
from typing import Any

import structlog
from celery import Celery
from pydantic import BaseModel, Field

# Ensure the project root is in Python path (safe approach)
# Note: This is needed for the service to import libs when run standalone
project_root = Path(__file__).parent.parent.parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

# Import after path setup (required for standalone execution)
from libs.workflow_orchestration import (  # noqa: E402
    DAG,
    AlertRule,
    ResourceRequirement,
    Task,
    TaskConfig,
    TaskType,
    TriggerConfig,
    TriggerType,
    WorkflowDefinition,
    WorkflowEngine,
    WorkflowMonitor,
    WorkflowScheduler,
    WorkflowStorage,
    create_aggressive_retry_policy,
    create_default_retry_policy,
)

logger = structlog.get_logger(__name__)


class BatchProcessorConfig(BaseModel):
    """Configuration for the batch processor service."""

    # Service configuration
    service_name: str = Field(default="batch-processor", description="Service name")
    service_version: str = Field(default="1.0.0", description="Service version")
    environment: str = Field(default="production", description="Environment")

    # Celery configuration
    celery_broker_url: str = Field(
        default="redis://localhost:6379/0", description="Celery broker URL"
    )
    celery_result_backend: str = Field(
        default="redis://localhost:6379/0", description="Celery result backend"
    )

    # Resource limits
    max_cpu_cores: float = Field(default=8.0, description="Maximum CPU cores")
    max_memory_mb: int = Field(default=16384, description="Maximum memory in MB")
    max_concurrent_workflows: int = Field(
        default=10, description="Maximum concurrent workflows"
    )
    max_concurrent_tasks: int = Field(
        default=50, description="Maximum concurrent tasks"
    )

    # Monitoring configuration
    enable_monitoring: bool = Field(
        default=True, description="Enable workflow monitoring"
    )
    metrics_retention_days: int = Field(
        default=30, description="Metrics retention period"
    )
    alert_notification_channels: list[str] = Field(
        default_factory=list, description="Alert channels"
    )

    # Scheduler configuration
    enable_scheduler: bool = Field(
        default=True, description="Enable workflow scheduler"
    )
    scheduler_check_interval: int = Field(
        default=30, description="Scheduler check interval in seconds"
    )

    # Storage configuration
    storage_backend: str = Field(default="memory", description="Storage backend")
    enable_artifacts: bool = Field(default=True, description="Enable artifact storage")
    artifact_retention_days: int = Field(
        default=7, description="Artifact retention period"
    )


class BatchProcessor:
    """Advanced batch processing service with workflow orchestration."""

    def __init__(self, config: BatchProcessorConfig):
        self.config = config
        self.running = False

        # Initialize Celery app
        self.celery_app = Celery(
            "batch_processor",
            broker=config.celery_broker_url,
            backend=config.celery_result_backend,
        )
        self._configure_celery()

        # Initialize workflow components
        self.workflow_storage = WorkflowStorage(storage_backend=config.storage_backend)

        self.workflow_engine = WorkflowEngine(
            celery_app=self.celery_app,
            max_cpu=config.max_cpu_cores,
            max_memory_mb=config.max_memory_mb,
            max_concurrent_tasks=config.max_concurrent_tasks,
        )

        self.workflow_monitor = WorkflowMonitor(self.workflow_engine.workflow_state)
        self.workflow_scheduler = WorkflowScheduler(self.workflow_engine)

        # Register built-in task functions
        self._register_builtin_functions()

        # Setup default monitoring and alerts
        if config.enable_monitoring:
            self._setup_default_alerts()

        logger.info(
            "Batch processor initialized",
            service_name=config.service_name,
            max_cpu=config.max_cpu_cores,
            max_memory_mb=config.max_memory_mb,
            max_concurrent_workflows=config.max_concurrent_workflows,
            max_concurrent_tasks=config.max_concurrent_tasks,
        )

    def _configure_celery(self) -> None:
        """Configure Celery application."""
        self.celery_app.conf.update(
            task_serializer="json",
            accept_content=["json"],
            result_serializer="json",
            timezone="UTC",
            enable_utc=True,
            task_track_started=True,
            task_time_limit=3600,  # 1 hour default timeout
            task_soft_time_limit=3300,  # 55 minutes soft timeout
            worker_prefetch_multiplier=1,
            task_acks_late=True,
            worker_disable_rate_limits=False,
            task_compression="gzip",
            result_compression="gzip",
        )

    def _register_builtin_functions(self) -> None:
        """Register built-in task functions."""
        # Data processing functions
        self.workflow_engine.register_task_function(
            "data_ingestion", self.data_ingestion_task
        )
        self.workflow_engine.register_task_function(
            "data_validation", self.data_validation_task
        )
        self.workflow_engine.register_task_function(
            "data_transformation", self.data_transformation_task
        )
        self.workflow_engine.register_task_function(
            "data_quality_check", self.data_quality_check_task
        )

        # Machine learning functions
        self.workflow_engine.register_task_function(
            "model_training", self.model_training_task
        )
        self.workflow_engine.register_task_function(
            "model_inference", self.model_inference_task
        )
        self.workflow_engine.register_task_function(
            "model_evaluation", self.model_evaluation_task
        )

        # Reporting functions
        self.workflow_engine.register_task_function(
            "generate_report", self.generate_report_task
        )
        self.workflow_engine.register_task_function(
            "send_notification", self.send_notification_task
        )

        # Utility functions
        self.workflow_engine.register_task_function(
            "data_export", self.data_export_task
        )
        self.workflow_engine.register_task_function("cleanup", self.cleanup_task)

        logger.info("Built-in task functions registered")

    def _setup_default_alerts(self) -> None:
        """Setup default monitoring alerts."""
        # High-level system alerts
        self.workflow_monitor.add_alert_rule(
            AlertRule(
                name="system_success_rate_low",
                workflow_name=None,  # Apply to all workflows
                success_rate_threshold=90.0,
                evaluation_period_minutes=60,
                severity="warning",
                notification_channels=self.config.alert_notification_channels,
            )
        )

        self.workflow_monitor.add_alert_rule(
            AlertRule(
                name="system_error_rate_high",
                workflow_name=None,
                error_count_threshold=10,
                evaluation_period_minutes=30,
                severity="error",
                notification_channels=self.config.alert_notification_channels,
            )
        )

        # Performance alerts
        self.workflow_monitor.add_alert_rule(
            AlertRule(
                name="workflow_duration_high",
                workflow_name=None,
                duration_threshold_seconds=7200,  # 2 hours
                evaluation_period_minutes=60,
                severity="warning",
                notification_channels=self.config.alert_notification_channels,
            )
        )

        # Failure streak alerts
        self.workflow_monitor.add_alert_rule(
            AlertRule(
                name="consecutive_failures",
                workflow_name=None,
                failure_streak_threshold=3,
                evaluation_period_minutes=120,
                severity="critical",
                notification_channels=self.config.alert_notification_channels,
            )
        )

        logger.info("Default monitoring alerts configured")

    async def start(self) -> None:
        """Start the batch processor service."""
        if self.running:
            return

        self.running = True

        # Start scheduler if enabled
        if self.config.enable_scheduler:
            await self.workflow_scheduler.start()

        logger.info("Batch processor service started")

    async def stop(self) -> None:
        """Stop the batch processor service."""
        if not self.running:
            return

        self.running = False

        # Stop scheduler
        if self.config.enable_scheduler:
            await self.workflow_scheduler.stop()

        # Shutdown workflow engine
        await self.workflow_engine.shutdown()

        logger.info("Batch processor service stopped")

    def create_data_pipeline_workflow(
        self,
        name: str,
        description: str = "Data processing pipeline",
        owner: str = "system",
    ) -> DAG:
        """Create a standard data processing pipeline workflow."""
        # Create workflow definition
        definition = WorkflowDefinition(
            name=name,
            version="1.0.0",
            description=description,
            owner=owner,
            tags=["data-processing", "pipeline"],
            max_parallel_tasks=5,
            timeout_seconds=14400,  # 4 hours
            retry_policy=create_default_retry_policy().dict(),
        )

        # Create DAG
        dag = DAG(definition)

        # Add data ingestion task
        ingestion_task = Task(
            config=TaskConfig(
                name="data_ingestion",
                description="Ingest raw data from sources",
                task_type=TaskType.DATA_INGESTION,
                function_name="data_ingestion",
                parameters={"source_type": "database", "batch_size": 1000},
                resources=ResourceRequirement(
                    cpu=1.0, memory_mb=1024, timeout_seconds=3600
                ),
            )
        )
        dag.add_task(ingestion_task)

        # Add data validation task
        validation_task = Task(
            config=TaskConfig(
                name="data_validation",
                description="Validate ingested data quality",
                task_type=TaskType.DATA_VALIDATION,
                function_name="data_validation",
                parameters={"validation_rules": "standard"},
                resources=ResourceRequirement(
                    cpu=0.5, memory_mb=512, timeout_seconds=1800
                ),
            ),
            upstream_tasks=["data_ingestion"],
        )
        dag.add_task(validation_task)

        # Add data transformation task
        transformation_task = Task(
            config=TaskConfig(
                name="data_transformation",
                description="Transform and clean data",
                task_type=TaskType.DATA_TRANSFORMATION,
                function_name="data_transformation",
                parameters={"transformation_type": "standardize"},
                resources=ResourceRequirement(
                    cpu=2.0, memory_mb=2048, timeout_seconds=3600
                ),
            ),
            upstream_tasks=["data_validation"],
        )
        dag.add_task(transformation_task)

        # Add data quality check task
        quality_check_task = Task(
            config=TaskConfig(
                name="data_quality_check",
                description="Final data quality assessment",
                task_type=TaskType.DATA_QUALITY,
                function_name="data_quality_check",
                parameters={"quality_threshold": 0.95},
                resources=ResourceRequirement(
                    cpu=0.5, memory_mb=512, timeout_seconds=900
                ),
            ),
            upstream_tasks=["data_transformation"],
        )
        dag.add_task(quality_check_task)

        # Add report generation task
        report_task = Task(
            config=TaskConfig(
                name="generate_report",
                description="Generate processing report",
                task_type=TaskType.REPORTING,
                function_name="generate_report",
                parameters={"report_type": "pipeline_summary"},
                resources=ResourceRequirement(
                    cpu=0.5, memory_mb=256, timeout_seconds=600
                ),
            ),
            upstream_tasks=["data_quality_check"],
        )
        dag.add_task(report_task)

        # Set dependencies
        dag.set_dependency("data_ingestion", "data_validation")
        dag.set_dependency("data_validation", "data_transformation")
        dag.set_dependency("data_transformation", "data_quality_check")
        dag.set_dependency("data_quality_check", "generate_report")

        return dag

    def create_ml_training_workflow(
        self,
        name: str,
        model_type: str = "classification",
        description: str = "Machine learning training pipeline",
        owner: str = "data-science",
    ) -> DAG:
        """Create a machine learning training workflow."""
        # Create workflow definition
        definition = WorkflowDefinition(
            name=name,
            version="1.0.0",
            description=description,
            owner=owner,
            tags=["machine-learning", "training"],
            max_parallel_tasks=3,
            timeout_seconds=28800,  # 8 hours
            retry_policy=create_aggressive_retry_policy().dict(),
        )

        # Create DAG
        dag = DAG(definition)

        # Add data preparation task
        data_prep_task = Task(
            config=TaskConfig(
                name="data_preparation",
                description="Prepare training data",
                task_type=TaskType.DATA_TRANSFORMATION,
                function_name="data_transformation",
                parameters={
                    "transformation_type": "ml_preprocessing",
                    "model_type": model_type,
                },
                resources=ResourceRequirement(
                    cpu=2.0, memory_mb=4096, timeout_seconds=7200
                ),
            )
        )
        dag.add_task(data_prep_task)

        # Add model training task
        training_task = Task(
            config=TaskConfig(
                name="model_training",
                description=f"Train {model_type} model",
                task_type=TaskType.MACHINE_LEARNING,
                function_name="model_training",
                parameters={"model_type": model_type, "hyperparameters": "auto"},
                resources=ResourceRequirement(
                    cpu=4.0, memory_mb=8192, timeout_seconds=14400, gpu_count=1
                ),
            ),
            upstream_tasks=["data_preparation"],
        )
        dag.add_task(training_task)

        # Add model evaluation task
        evaluation_task = Task(
            config=TaskConfig(
                name="model_evaluation",
                description="Evaluate trained model",
                task_type=TaskType.MACHINE_LEARNING,
                function_name="model_evaluation",
                parameters={
                    "evaluation_metrics": ["accuracy", "precision", "recall", "f1"]
                },
                resources=ResourceRequirement(
                    cpu=1.0, memory_mb=2048, timeout_seconds=3600
                ),
            ),
            upstream_tasks=["model_training"],
        )
        dag.add_task(evaluation_task)

        # Add model deployment preparation task
        deployment_prep_task = Task(
            config=TaskConfig(
                name="deployment_preparation",
                description="Prepare model for deployment",
                task_type=TaskType.CUSTOM,
                function_name="data_export",
                parameters={
                    "export_type": "model_artifacts",
                    "destination": "model_registry",
                },
                resources=ResourceRequirement(
                    cpu=0.5, memory_mb=1024, timeout_seconds=1800
                ),
            ),
            upstream_tasks=["model_evaluation"],
        )
        dag.add_task(deployment_prep_task)

        # Add notification task
        notification_task = Task(
            config=TaskConfig(
                name="training_notification",
                description="Notify about training completion",
                task_type=TaskType.NOTIFICATION,
                function_name="send_notification",
                parameters={
                    "notification_type": "ml_training_complete",
                    "recipients": ["data-science-team"],
                },
                resources=ResourceRequirement(
                    cpu=0.1, memory_mb=128, timeout_seconds=300
                ),
            ),
            upstream_tasks=["deployment_preparation"],
        )
        dag.add_task(notification_task)

        # Set dependencies
        dag.set_dependency("data_preparation", "model_training")
        dag.set_dependency("model_training", "model_evaluation")
        dag.set_dependency("model_evaluation", "deployment_preparation")
        dag.set_dependency("deployment_preparation", "training_notification")

        return dag

    # Built-in task implementations
    async def data_ingestion_task(
        self, context: dict[str, Any], **kwargs
    ) -> dict[str, Any]:
        """Data ingestion task implementation."""
        source_type = kwargs.get("source_type", "database")
        batch_size = kwargs.get("batch_size", 1000)

        logger.info(
            "Starting data ingestion", source_type=source_type, batch_size=batch_size
        )

        # Simulate data ingestion
        await asyncio.sleep(2)  # Simulate processing time

        # Simulate ingesting records
        ingested_records = batch_size

        return {
            "records_ingested": ingested_records,
            "source_type": source_type,
            "ingestion_timestamp": datetime.now(UTC).isoformat(),
            "status": "success",
        }

    async def data_validation_task(
        self, context: dict[str, Any], **kwargs
    ) -> dict[str, Any]:
        """Data validation task implementation."""
        validation_rules = kwargs.get("validation_rules", "standard")
        upstream_results = kwargs.get("upstream_results", {})

        logger.info("Starting data validation", validation_rules=validation_rules)

        # Get ingested data count from upstream task
        ingestion_result = upstream_results.get("data_ingestion", {})
        records_to_validate = ingestion_result.get("result_data", {}).get(
            "records_ingested", 0
        )

        # Simulate validation
        await asyncio.sleep(1)

        # Simulate validation results
        valid_records = int(records_to_validate * 0.95)  # 95% valid
        invalid_records = records_to_validate - valid_records

        return {
            "records_validated": records_to_validate,
            "valid_records": valid_records,
            "invalid_records": invalid_records,
            "validation_rate": valid_records / records_to_validate
            if records_to_validate > 0
            else 0,
            "validation_rules": validation_rules,
            "status": "success",
        }

    async def data_transformation_task(
        self, context: dict[str, Any], **kwargs
    ) -> dict[str, Any]:
        """Data transformation task implementation."""
        transformation_type = kwargs.get("transformation_type", "standardize")
        upstream_results = kwargs.get("upstream_results", {})

        logger.info(
            "Starting data transformation", transformation_type=transformation_type
        )

        # Get validation results
        validation_result = upstream_results.get("data_validation", {})
        valid_records = validation_result.get("result_data", {}).get("valid_records", 0)

        # Simulate transformation
        await asyncio.sleep(3)  # Longer processing time

        # Simulate transformation results
        transformed_records = valid_records

        return {
            "records_transformed": transformed_records,
            "transformation_type": transformation_type,
            "transformation_timestamp": datetime.now(UTC).isoformat(),
            "status": "success",
        }

    async def data_quality_check_task(
        self, context: dict[str, Any], **kwargs
    ) -> dict[str, Any]:
        """Data quality check task implementation."""
        quality_threshold = kwargs.get("quality_threshold", 0.95)
        upstream_results = kwargs.get("upstream_results", {})

        logger.info("Starting data quality check", quality_threshold=quality_threshold)

        # Get transformation results
        transformation_result = upstream_results.get("data_transformation", {})
        transformed_records = transformation_result.get("result_data", {}).get(
            "records_transformed", 0
        )

        # Simulate quality check
        await asyncio.sleep(1)

        # Simulate quality score
        quality_score = 0.97  # Good quality
        quality_passed = quality_score >= quality_threshold

        return {
            "records_checked": transformed_records,
            "quality_score": quality_score,
            "quality_threshold": quality_threshold,
            "quality_passed": quality_passed,
            "status": "success" if quality_passed else "warning",
        }

    async def model_training_task(
        self, context: dict[str, Any], **kwargs
    ) -> dict[str, Any]:
        """Model training task implementation."""
        model_type = kwargs.get("model_type", "classification")
        hyperparameters = kwargs.get("hyperparameters", "auto")

        logger.info("Starting model training", model_type=model_type)

        # Simulate training time
        await asyncio.sleep(5)  # Longer training time

        return {
            "model_type": model_type,
            "hyperparameters": hyperparameters,
            "training_accuracy": 0.92,
            "training_loss": 0.08,
            "training_epochs": 100,
            "model_id": f"model_{datetime.now().strftime('%Y%m%d_%H%M%S')}",
            "status": "success",
        }

    async def model_inference_task(
        self, context: dict[str, Any], **kwargs
    ) -> dict[str, Any]:
        """Model inference task implementation."""
        logger.info("Starting model inference")

        # Simulate inference
        await asyncio.sleep(1)

        return {
            "predictions_generated": 1000,
            "inference_timestamp": datetime.now(UTC).isoformat(),
            "status": "success",
        }

    async def model_evaluation_task(
        self, context: dict[str, Any], **kwargs
    ) -> dict[str, Any]:
        """Model evaluation task implementation."""
        evaluation_metrics = kwargs.get("evaluation_metrics", ["accuracy"])
        upstream_results = kwargs.get("upstream_results", {})

        logger.info("Starting model evaluation", metrics=evaluation_metrics)

        # Get training results
        training_result = upstream_results.get("model_training", {})
        model_id = training_result.get("result_data", {}).get("model_id", "unknown")

        # Simulate evaluation
        await asyncio.sleep(2)

        return {
            "model_id": model_id,
            "evaluation_metrics": {
                "accuracy": 0.91,
                "precision": 0.89,
                "recall": 0.93,
                "f1": 0.91,
            },
            "test_samples": 5000,
            "status": "success",
        }

    async def generate_report_task(
        self, context: dict[str, Any], **kwargs
    ) -> dict[str, Any]:
        """Report generation task implementation."""
        report_type = kwargs.get("report_type", "summary")

        logger.info("Generating report", report_type=report_type)

        # Simulate report generation
        await asyncio.sleep(1)

        return {
            "report_type": report_type,
            "report_id": f"report_{datetime.now().strftime('%Y%m%d_%H%M%S')}",
            "report_generated": True,
            "generation_timestamp": datetime.now(UTC).isoformat(),
            "status": "success",
        }

    async def send_notification_task(
        self, context: dict[str, Any], **kwargs
    ) -> dict[str, Any]:
        """Notification task implementation."""
        notification_type = kwargs.get("notification_type", "general")
        recipients = kwargs.get("recipients", [])

        logger.info(
            "Sending notification", type=notification_type, recipients=recipients
        )

        # Simulate notification sending
        await asyncio.sleep(0.5)

        return {
            "notification_type": notification_type,
            "recipients": recipients,
            "sent_timestamp": datetime.now(UTC).isoformat(),
            "status": "success",
        }

    async def data_export_task(
        self, context: dict[str, Any], **kwargs
    ) -> dict[str, Any]:
        """Data export task implementation."""
        export_type = kwargs.get("export_type", "csv")
        destination = kwargs.get("destination", "filesystem")

        logger.info("Exporting data", type=export_type, destination=destination)

        # Simulate export
        await asyncio.sleep(1.5)

        return {
            "export_type": export_type,
            "destination": destination,
            "exported_records": 1000,
            "export_timestamp": datetime.now(UTC).isoformat(),
            "status": "success",
        }

    async def cleanup_task(self, context: dict[str, Any], **kwargs) -> dict[str, Any]:
        """Cleanup task implementation."""
        logger.info("Starting cleanup")

        # Simulate cleanup
        await asyncio.sleep(0.5)

        return {
            "cleaned_files": 5,
            "freed_space_mb": 1024,
            "cleanup_timestamp": datetime.now(UTC).isoformat(),
            "status": "success",
        }


async def main():
    """Main entry point for the batch processor service."""
    # Load configuration from environment
    config = BatchProcessorConfig(
        celery_broker_url=os.getenv("CELERY_BROKER_URL", "redis://localhost:6379/0"),
        celery_result_backend=os.getenv(
            "CELERY_RESULT_BACKEND", "redis://localhost:6379/0"
        ),
        max_cpu_cores=float(os.getenv("MAX_CPU_CORES", "8.0")),
        max_memory_mb=int(os.getenv("MAX_MEMORY_MB", "16384")),
        max_concurrent_workflows=int(os.getenv("MAX_CONCURRENT_WORKFLOWS", "10")),
        max_concurrent_tasks=int(os.getenv("MAX_CONCURRENT_TASKS", "50")),
        environment=os.getenv("ENVIRONMENT", "production"),
    )

    # Initialize batch processor
    processor = BatchProcessor(config)

    # Setup signal handlers for graceful shutdown
    def signal_handler(signum, frame):
        logger.info("Received shutdown signal", signal=signum)
        asyncio.create_task(processor.stop())

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    try:
        # Start the service
        await processor.start()

        # Create and register example workflows
        logger.info("Creating example workflows...")

        # Create data pipeline workflow
        data_pipeline_dag = processor.create_data_pipeline_workflow(
            name="daily_data_pipeline",
            description="Daily data processing pipeline with quality checks",
            owner="data-engineering",
        )
        processor.workflow_engine.register_workflow(data_pipeline_dag)

        # Create ML training workflow
        ml_training_dag = processor.create_ml_training_workflow(
            name="weekly_model_training",
            model_type="classification",
            description="Weekly model training and evaluation",
            owner="data-science",
        )
        processor.workflow_engine.register_workflow(ml_training_dag)

        # Setup scheduled triggers for the workflows
        if config.enable_scheduler:
            # Daily data pipeline at 2 AM
            daily_trigger = TriggerConfig(
                name="daily_data_pipeline_trigger",
                trigger_type=TriggerType.CRON,
                workflow_name="daily_data_pipeline",
                workflow_version="1.0.0",
                cron_expression="0 2 * * *",  # Every day at 2 AM
                enabled=True,
            )
            processor.workflow_scheduler.create_trigger(daily_trigger)

            # Weekly ML training on Sundays at 3 AM
            weekly_trigger = TriggerConfig(
                name="weekly_ml_training_trigger",
                trigger_type=TriggerType.CRON,
                workflow_name="weekly_model_training",
                workflow_version="1.0.0",
                cron_expression="0 3 * * 0",  # Every Sunday at 3 AM
                enabled=True,
            )
            processor.workflow_scheduler.create_trigger(weekly_trigger)

        logger.info("Batch processor service is running. Press Ctrl+C to stop.")

        # Run until shutdown signal
        while processor.running:
            await asyncio.sleep(1)

            # Periodic monitoring tasks
            if config.enable_monitoring:
                # Check alert conditions every 5 minutes
                await asyncio.sleep(300)  # 5 minutes
                alerts = processor.workflow_monitor.check_alert_conditions()
                if alerts:
                    logger.warning("Active alerts detected", alert_count=len(alerts))

    except Exception as e:
        logger.error("Batch processor service error", error=str(e))
        raise
    finally:
        await processor.stop()
        logger.info("Batch processor service stopped")


if __name__ == "__main__":
    # Configure structured logging
    structlog.configure(
        processors=[
            structlog.processors.TimeStamper(fmt="ISO"),
            structlog.processors.add_log_level,
            structlog.processors.JSONRenderer(),
        ],
        logger_factory=structlog.WriteLoggerFactory(),
        wrapper_class=structlog.BoundLogger,
        cache_logger_on_first_use=True,
    )

    # Run the service
    asyncio.run(main())
