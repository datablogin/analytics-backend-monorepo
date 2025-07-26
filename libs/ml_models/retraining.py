"""Automated model retraining triggers and orchestration."""

import asyncio
from collections.abc import Callable
from datetime import datetime, timedelta, timezone
from enum import Enum
from typing import Any

import structlog
from pydantic import BaseModel, Field

from ..observability.metrics import MLOpsMetrics
from .config import BaseConfig
from .monitoring import ModelMonitor
from .registry import ModelRegistry

logger = structlog.get_logger(__name__)


class RetrainingTriggerType(str, Enum):
    """Types of retraining triggers."""

    SCHEDULED = "scheduled"
    PERFORMANCE_DEGRADATION = "performance_degradation"
    DATA_DRIFT = "data_drift"
    MANUAL = "manual"
    DATA_VOLUME = "data_volume"
    MODEL_STALENESS = "model_staleness"
    CONCEPT_DRIFT = "concept_drift"


class RetrainingStatus(str, Enum):
    """Status of retraining jobs."""

    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


class RetrainingPriority(str, Enum):
    """Priority levels for retraining jobs."""

    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"
    CRITICAL = "critical"


class RetrainingConfig(BaseConfig):
    """Configuration for automated retraining."""

    # Trigger thresholds
    performance_degradation_threshold: float = Field(
        default=0.05, description="Performance degradation threshold (5%)"
    )
    drift_detection_threshold: float = Field(
        default=0.05, description="Data drift detection p-value threshold"
    )
    data_volume_threshold: int = Field(
        default=10000, description="Minimum new data samples for retraining"
    )

    # Timing settings
    max_model_age_days: int = Field(
        default=30, description="Maximum model age before retraining"
    )
    min_time_between_retraining_hours: int = Field(
        default=24, description="Minimum time between retraining attempts"
    )

    # Retraining behavior
    auto_approve_low_priority: bool = Field(
        default=True, description="Auto-approve low priority retraining jobs"
    )
    auto_deploy_after_retraining: bool = Field(
        default=False, description="Auto-deploy models after successful retraining"
    )

    # Resource limits
    max_concurrent_retraining_jobs: int = Field(
        default=3, description="Maximum concurrent retraining jobs"
    )
    retraining_timeout_hours: int = Field(
        default=12, description="Timeout for retraining jobs"
    )

    # Monitoring intervals
    trigger_check_interval_minutes: int = Field(
        default=60, description="Interval for checking retraining triggers"
    )


class RetrainingTrigger(BaseModel):
    """Retraining trigger definition."""

    trigger_id: str = Field(description="Unique trigger identifier")
    model_name: str = Field(description="Model name")
    trigger_type: RetrainingTriggerType = Field(description="Type of trigger")

    # Trigger conditions
    conditions: dict[str, Any] = Field(description="Trigger condition parameters")
    priority: RetrainingPriority = Field(description="Trigger priority")

    # Trigger metadata
    description: str | None = Field(default=None, description="Trigger description")
    created_by: str | None = Field(default=None, description="User who created trigger")
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))

    # Status
    is_active: bool = Field(default=True, description="Whether trigger is active")
    last_triggered: datetime | None = Field(
        default=None, description="Last trigger time"
    )
    trigger_count: int = Field(default=0, description="Number of times triggered")

    # Tags
    tags: dict[str, str] = Field(default_factory=dict, description="Trigger tags")


class RetrainingJob(BaseModel):
    """Retraining job definition."""

    job_id: str = Field(description="Unique job identifier")
    model_name: str = Field(description="Model name")
    trigger_id: str | None = Field(default=None, description="Triggering trigger ID")

    # Job configuration
    training_config: dict[str, Any] = Field(description="Training configuration")
    data_config: dict[str, Any] = Field(description="Data configuration")

    # Job lifecycle
    status: RetrainingStatus = Field(default=RetrainingStatus.PENDING)
    priority: RetrainingPriority = Field(description="Job priority")

    # Timing
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    started_at: datetime | None = Field(default=None, description="Job start time")
    completed_at: datetime | None = Field(
        default=None, description="Job completion time"
    )

    # Results
    new_model_version: str | None = Field(
        default=None, description="New model version created"
    )
    performance_metrics: dict[str, float] = Field(
        default_factory=dict, description="New model performance metrics"
    )
    improvement_metrics: dict[str, float] = Field(
        default_factory=dict, description="Performance improvement over previous model"
    )

    # Execution details
    execution_log: list[str] = Field(
        default_factory=list, description="Execution log messages"
    )
    error_message: str | None = Field(
        default=None, description="Error message if failed"
    )

    # Metadata
    created_by: str | None = Field(default=None, description="User who created job")
    approved_by: str | None = Field(default=None, description="User who approved job")
    tags: dict[str, str] = Field(default_factory=dict, description="Job tags")


class RetrainingOrchestrator:
    """Orchestrates automated model retraining."""

    def __init__(
        self,
        config: RetrainingConfig | None = None,
        model_registry: ModelRegistry | None = None,
        model_monitor: ModelMonitor | None = None,
    ):
        """Initialize retraining orchestrator."""
        self.config = config or RetrainingConfig()
        self.model_registry = model_registry or ModelRegistry()
        self.model_monitor = model_monitor or ModelMonitor()
        self.metrics = MLOpsMetrics()

        # Trigger and job storage (in production, use persistent storage)
        self.triggers: dict[str, RetrainingTrigger] = {}
        self.jobs: dict[str, RetrainingJob] = {}
        self.running_jobs: set[str] = set()

        # Event callbacks
        self.job_callbacks: list[Callable[[RetrainingJob], None]] = []

        # Background tasks
        self._monitoring_task: asyncio.Task | None = None
        self._is_running = False

        logger.info(
            "Retraining orchestrator initialized",
            performance_threshold=self.config.performance_degradation_threshold,
            drift_threshold=self.config.drift_detection_threshold,
            max_concurrent_jobs=self.config.max_concurrent_retraining_jobs,
        )

    async def start(self) -> None:
        """Start the retraining orchestrator."""
        self._is_running = True

        # Start background monitoring
        self._monitoring_task = asyncio.create_task(self._monitor_triggers())

        logger.info("Retraining orchestrator started")

    async def stop(self) -> None:
        """Stop the retraining orchestrator."""
        self._is_running = False

        # Stop background monitoring
        if self._monitoring_task:
            self._monitoring_task.cancel()
            try:
                await self._monitoring_task
            except asyncio.CancelledError:
                pass

        logger.info("Retraining orchestrator stopped")

    def create_trigger(
        self,
        model_name: str,
        trigger_type: RetrainingTriggerType,
        conditions: dict[str, Any],
        priority: RetrainingPriority = RetrainingPriority.MEDIUM,
        description: str | None = None,
        created_by: str | None = None,
        tags: dict[str, str] | None = None,
    ) -> RetrainingTrigger:
        """Create a retraining trigger."""
        try:
            # Generate trigger ID
            trigger_id = self._generate_trigger_id(model_name, trigger_type)

            # Validate conditions
            self._validate_trigger_conditions(trigger_type, conditions)

            # Create trigger
            trigger = RetrainingTrigger(
                trigger_id=trigger_id,
                model_name=model_name,
                trigger_type=trigger_type,
                conditions=conditions,
                priority=priority,
                description=description,
                created_by=created_by,
                tags=tags or {},
            )

            # Store trigger
            self.triggers[trigger_id] = trigger

            # Record metrics (placeholder - extend MLOpsMetrics as needed)
            # TODO: Add proper metrics tracking for retraining triggers

            logger.info(
                "Retraining trigger created",
                trigger_id=trigger_id,
                model_name=model_name,
                trigger_type=trigger_type,
                priority=priority,
            )

            return trigger

        except Exception as error:
            logger.error(
                "Failed to create retraining trigger",
                model_name=model_name,
                trigger_type=trigger_type,
                error=str(error),
            )
            raise

    def _generate_trigger_id(
        self, model_name: str, trigger_type: RetrainingTriggerType
    ) -> str:
        """Generate unique trigger ID."""
        timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        return f"trigger_{model_name}_{trigger_type.value}_{timestamp}"

    def _validate_trigger_conditions(
        self, trigger_type: RetrainingTriggerType, conditions: dict[str, Any]
    ) -> None:
        """Validate trigger conditions."""
        if trigger_type == RetrainingTriggerType.SCHEDULED:
            required_fields = ["schedule"]
            if not all(field in conditions for field in required_fields):
                raise ValueError(f"Scheduled trigger requires: {required_fields}")

        elif trigger_type == RetrainingTriggerType.PERFORMANCE_DEGRADATION:
            required_fields = ["metric", "threshold"]
            if not all(field in conditions for field in required_fields):
                raise ValueError(
                    f"Performance degradation trigger requires: {required_fields}"
                )

        elif trigger_type == RetrainingTriggerType.DATA_DRIFT:
            required_fields = ["features"]
            if not all(field in conditions for field in required_fields):
                raise ValueError(f"Data drift trigger requires: {required_fields}")

    async def _monitor_triggers(self) -> None:
        """Monitor triggers and execute when conditions are met."""
        while self._is_running:
            try:
                # Check all active triggers
                for trigger_id, trigger in self.triggers.items():
                    if not trigger.is_active:
                        continue

                    try:
                        should_trigger = await self._evaluate_trigger(trigger)

                        if should_trigger:
                            await self._execute_trigger(trigger)

                    except Exception as trigger_error:
                        logger.error(
                            "Error evaluating trigger",
                            trigger_id=trigger_id,
                            error=str(trigger_error),
                        )

                # Wait before next check
                await asyncio.sleep(self.config.trigger_check_interval_minutes * 60)

            except asyncio.CancelledError:
                break
            except Exception as error:
                logger.error("Error in trigger monitoring", error=str(error))
                await asyncio.sleep(60)  # Backoff on error

    async def _evaluate_trigger(self, trigger: RetrainingTrigger) -> bool:
        """Evaluate whether a trigger should fire."""
        try:
            # Check cooldown period
            if trigger.last_triggered:
                time_since_last = datetime.now(timezone.utc) - trigger.last_triggered
                cooldown = timedelta(
                    hours=self.config.min_time_between_retraining_hours
                )

                if time_since_last < cooldown:
                    return False

            # Evaluate based on trigger type
            if trigger.trigger_type == RetrainingTriggerType.SCHEDULED:
                return self._evaluate_scheduled_trigger(trigger)

            elif trigger.trigger_type == RetrainingTriggerType.PERFORMANCE_DEGRADATION:
                return await self._evaluate_performance_trigger(trigger)

            elif trigger.trigger_type == RetrainingTriggerType.DATA_DRIFT:
                return await self._evaluate_drift_trigger(trigger)

            elif trigger.trigger_type == RetrainingTriggerType.MODEL_STALENESS:
                return self._evaluate_staleness_trigger(trigger)

            elif trigger.trigger_type == RetrainingTriggerType.DATA_VOLUME:
                return await self._evaluate_data_volume_trigger(trigger)

            else:
                logger.warning(
                    "Unknown trigger type",
                    trigger_id=trigger.trigger_id,
                    trigger_type=trigger.trigger_type,
                )
                return False

        except Exception as error:
            logger.error(
                "Error evaluating trigger",
                trigger_id=trigger.trigger_id,
                error=str(error),
            )
            return False

    def _evaluate_scheduled_trigger(self, trigger: RetrainingTrigger) -> bool:
        """Evaluate scheduled trigger."""
        schedule = trigger.conditions.get("schedule")

        if schedule == "daily":
            # Check if we should trigger daily
            if trigger.last_triggered:
                last_date = trigger.last_triggered.date()
                current_date = datetime.now(timezone.utc).date()
                return current_date > last_date
            return True

        elif schedule == "weekly":
            # Check if we should trigger weekly
            if trigger.last_triggered:
                days_since = (datetime.now(timezone.utc) - trigger.last_triggered).days
                return days_since >= 7
            return True

        elif schedule == "monthly":
            # Check if we should trigger monthly
            if trigger.last_triggered:
                days_since = (datetime.now(timezone.utc) - trigger.last_triggered).days
                return days_since >= 30
            return True

        return False

    async def _evaluate_performance_trigger(self, trigger: RetrainingTrigger) -> bool:
        """Evaluate performance degradation trigger."""
        try:
            metric_name = trigger.conditions.get("metric")
            threshold = trigger.conditions.get(
                "threshold", self.config.performance_degradation_threshold
            )

            # Get current model performance (placeholder)
            # In production, this would query recent performance metrics
            current_performance = await self._get_current_performance(
                trigger.model_name, metric_name
            )
            baseline_performance = await self._get_baseline_performance(
                trigger.model_name, metric_name
            )

            if current_performance is None or baseline_performance is None:
                return False

            # Calculate degradation
            degradation = (
                baseline_performance - current_performance
            ) / baseline_performance

            return degradation > threshold

        except Exception as error:
            logger.error(
                "Error evaluating performance trigger",
                trigger_id=trigger.trigger_id,
                error=str(error),
            )
            return False

    async def _evaluate_drift_trigger(self, trigger: RetrainingTrigger) -> bool:
        """Evaluate data drift trigger."""
        try:
            features = trigger.conditions.get("features", [])

            # Get recent data for drift detection (placeholder)
            current_data = await self._get_recent_data(trigger.model_name)

            if current_data is None or len(current_data) < 100:
                return False

            # Detect drift
            drift_results = self.model_monitor.detect_data_drift(
                model_name=trigger.model_name,
                current_data=current_data,
                feature_subset=features if features else None,
            )

            # Check if significant drift detected
            significant_drift_count = sum(
                1 for result in drift_results if result.drift_detected
            )
            drift_percentage = (
                significant_drift_count / len(drift_results) if drift_results else 0
            )

            threshold = trigger.conditions.get(
                "drift_threshold", 0.2
            )  # 20% of features

            return drift_percentage > threshold

        except Exception as error:
            logger.error(
                "Error evaluating drift trigger",
                trigger_id=trigger.trigger_id,
                error=str(error),
            )
            return False

    def _evaluate_staleness_trigger(self, trigger: RetrainingTrigger) -> bool:
        """Evaluate model staleness trigger."""
        try:
            max_age_days = trigger.conditions.get(
                "max_age_days", self.config.max_model_age_days
            )

            # Get model creation date
            model_metadata = self.model_registry.get_model_metadata(
                trigger.model_name, "latest"
            )

            model_age = datetime.now(timezone.utc) - model_metadata.created_at

            return model_age.days > max_age_days

        except Exception as error:
            logger.error(
                "Error evaluating staleness trigger",
                trigger_id=trigger.trigger_id,
                error=str(error),
            )
            return False

    async def _evaluate_data_volume_trigger(self, trigger: RetrainingTrigger) -> bool:
        """Evaluate data volume trigger."""
        try:
            min_samples = trigger.conditions.get(
                "min_samples", self.config.data_volume_threshold
            )

            # Get count of new data since last training (placeholder)
            new_data_count = await self._get_new_data_count(trigger.model_name)

            return new_data_count >= min_samples

        except Exception as error:
            logger.error(
                "Error evaluating data volume trigger",
                trigger_id=trigger.trigger_id,
                error=str(error),
            )
            return False

    async def _execute_trigger(self, trigger: RetrainingTrigger) -> None:
        """Execute a triggered retraining."""
        try:
            # Update trigger state
            trigger.last_triggered = datetime.now(timezone.utc)
            trigger.trigger_count += 1

            # Create retraining job
            job = await self.create_retraining_job(
                model_name=trigger.model_name,
                trigger_id=trigger.trigger_id,
                priority=trigger.priority,
                training_config=trigger.conditions.get("training_config", {}),
                created_by="system",
                tags={"triggered": "true", "trigger_type": trigger.trigger_type.value},
            )

            # Auto-approve if configured
            if (
                trigger.priority == RetrainingPriority.LOW
                and self.config.auto_approve_low_priority
            ):
                await self.approve_retraining_job(job.job_id, approved_by="system")

            logger.info(
                "Trigger executed",
                trigger_id=trigger.trigger_id,
                job_id=job.job_id,
                model_name=trigger.model_name,
            )

        except Exception as error:
            logger.error(
                "Failed to execute trigger",
                trigger_id=trigger.trigger_id,
                error=str(error),
            )

    async def create_retraining_job(
        self,
        model_name: str,
        training_config: dict[str, Any] | None = None,
        data_config: dict[str, Any] | None = None,
        priority: RetrainingPriority = RetrainingPriority.MEDIUM,
        trigger_id: str | None = None,
        created_by: str | None = None,
        tags: dict[str, str] | None = None,
    ) -> RetrainingJob:
        """Create a retraining job."""
        try:
            # Generate job ID
            job_id = self._generate_job_id(model_name)

            # Set default configurations
            default_training_config = {
                "experiment_name": f"retrain_{model_name}_{datetime.now(timezone.utc).strftime('%Y%m%d')}",
                "max_epochs": 100,
                "early_stopping": True,
                "validation_split": 0.2,
            }

            default_data_config = {
                "data_source": "latest",
                "feature_engineering": "standard",
                "data_validation": True,
            }

            # Create job
            job = RetrainingJob(
                job_id=job_id,
                model_name=model_name,
                trigger_id=trigger_id,
                training_config={**default_training_config, **(training_config or {})},
                data_config={**default_data_config, **(data_config or {})},
                priority=priority,
                created_by=created_by,
                tags=tags or {},
            )

            # Store job
            self.jobs[job_id] = job

            # Record metrics (placeholder - extend MLOpsMetrics as needed)
            # TODO: Add proper metrics tracking for retraining jobs

            logger.info(
                "Retraining job created",
                job_id=job_id,
                model_name=model_name,
                priority=priority,
                trigger_id=trigger_id,
            )

            return job

        except Exception as error:
            logger.error(
                "Failed to create retraining job",
                model_name=model_name,
                error=str(error),
            )
            raise

    def _generate_job_id(self, model_name: str) -> str:
        """Generate unique job ID."""
        timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        return f"retrain_{model_name}_{timestamp}"

    async def approve_retraining_job(self, job_id: str, approved_by: str) -> None:
        """Approve a retraining job for execution."""
        try:
            if job_id not in self.jobs:
                raise ValueError(f"Job {job_id} not found")

            job = self.jobs[job_id]

            if job.status != RetrainingStatus.PENDING:
                raise ValueError(
                    f"Can only approve pending jobs, current status: {job.status}"
                )

            # Update job
            job.approved_by = approved_by

            # Start job if resources available
            if len(self.running_jobs) < self.config.max_concurrent_retraining_jobs:
                await self._start_retraining_job(job)

            logger.info(
                "Retraining job approved",
                job_id=job_id,
                approved_by=approved_by,
            )

        except Exception as error:
            logger.error(
                "Failed to approve retraining job",
                job_id=job_id,
                error=str(error),
            )
            raise

    async def _start_retraining_job(self, job: RetrainingJob) -> None:
        """Start executing a retraining job."""
        try:
            # Update job status
            job.status = RetrainingStatus.RUNNING
            job.started_at = datetime.now(timezone.utc)
            self.running_jobs.add(job.job_id)

            # Start job execution asynchronously
            asyncio.create_task(self._execute_retraining_job(job))

            logger.info(
                "Retraining job started",
                job_id=job.job_id,
                model_name=job.model_name,
            )

        except Exception as error:
            logger.error(
                "Failed to start retraining job",
                job_id=job.job_id,
                error=str(error),
            )
            job.status = RetrainingStatus.FAILED
            job.error_message = str(error)
            self.running_jobs.discard(job.job_id)

    async def _execute_retraining_job(self, job: RetrainingJob) -> None:
        """Execute the actual retraining job."""
        try:
            job.execution_log.append(f"Starting retraining for {job.model_name}")

            # Step 1: Prepare data
            job.execution_log.append("Preparing training data")
            await self._prepare_training_data(job)

            # Step 2: Train model
            job.execution_log.append("Training new model")
            new_model_version = await self._train_model(job)

            # Step 3: Validate model
            job.execution_log.append("Validating new model")
            performance_metrics = await self._validate_model(job, new_model_version)

            # Step 4: Compare with existing model
            job.execution_log.append("Comparing with existing model")
            improvement_metrics = await self._compare_models(job, new_model_version)

            # Update job with results
            job.new_model_version = new_model_version
            job.performance_metrics = performance_metrics
            job.improvement_metrics = improvement_metrics
            job.status = RetrainingStatus.COMPLETED
            job.completed_at = datetime.now(timezone.utc)

            # Auto-deploy if configured and improvement is significant
            if self.config.auto_deploy_after_retraining and self._should_auto_deploy(
                improvement_metrics
            ):
                job.execution_log.append("Auto-deploying new model")
                await self._deploy_model(job, new_model_version)

            # Call callbacks
            for callback in self.job_callbacks:
                try:
                    callback(job)
                except Exception as callback_error:
                    logger.warning(
                        "Error in job callback",
                        job_id=job.job_id,
                        error=str(callback_error),
                    )

            # Record metrics (placeholder - extend MLOpsMetrics as needed)
            duration = (job.completed_at - job.started_at).total_seconds()
            # TODO: Add proper metrics tracking for retraining job completion

            logger.info(
                "Retraining job completed successfully",
                job_id=job.job_id,
                model_name=job.model_name,
                new_version=new_model_version,
                duration_seconds=duration,
            )

        except Exception as error:
            # Handle job failure
            job.status = RetrainingStatus.FAILED
            job.error_message = str(error)
            job.completed_at = datetime.now(timezone.utc)
            job.execution_log.append(f"Job failed: {error}")

            # Record metrics (placeholder - extend MLOpsMetrics as needed)
            duration = (
                (job.completed_at - job.started_at).total_seconds()
                if job.started_at
                else 0
            )
            # TODO: Add proper metrics tracking for retraining job failures

            logger.error(
                "Retraining job failed",
                job_id=job.job_id,
                model_name=job.model_name,
                error=str(error),
            )

        finally:
            # Clean up
            self.running_jobs.discard(job.job_id)

    async def _prepare_training_data(self, job: RetrainingJob) -> None:
        """Prepare training data for retraining."""
        # Placeholder for data preparation logic
        await asyncio.sleep(1)  # Simulate data preparation

    async def _train_model(self, job: RetrainingJob) -> str:
        """Train the new model."""
        # Placeholder for model training logic
        await asyncio.sleep(5)  # Simulate training

        # Generate new version
        timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        return f"retrained_{timestamp}"

    async def _validate_model(
        self, job: RetrainingJob, model_version: str
    ) -> dict[str, float]:
        """Validate the newly trained model."""
        # Placeholder for model validation
        await asyncio.sleep(2)  # Simulate validation

        return {
            "accuracy": 0.89,
            "precision": 0.86,
            "recall": 0.92,
            "f1_score": 0.89,
        }

    async def _compare_models(
        self, job: RetrainingJob, new_version: str
    ) -> dict[str, float]:
        """Compare new model with existing model."""
        # Placeholder for model comparison
        await asyncio.sleep(1)  # Simulate comparison

        return {
            "accuracy_improvement": 0.03,
            "precision_improvement": 0.02,
            "recall_improvement": 0.04,
            "f1_score_improvement": 0.03,
        }

    def _should_auto_deploy(self, improvement_metrics: dict[str, float]) -> bool:
        """Determine if model should be auto-deployed."""
        # Check if there's significant improvement
        min_improvement = 0.01  # 1% minimum improvement

        return any(
            improvement > min_improvement
            for improvement in improvement_metrics.values()
            if improvement > 0
        )

    async def _deploy_model(self, job: RetrainingJob, model_version: str) -> None:
        """Deploy the new model."""
        # Placeholder for model deployment
        try:
            self.model_registry.transition_model_stage(
                name=job.model_name,
                version=model_version,
                stage="Production",
                archive_existing=True,
            )
        except Exception as error:
            logger.warning(
                "Failed to auto-deploy model",
                job_id=job.job_id,
                model_version=model_version,
                error=str(error),
            )

    # Placeholder methods for data access (implement based on your data infrastructure)
    async def _get_current_performance(
        self, model_name: str, metric_name: str
    ) -> float | None:
        """Get current model performance."""
        return 0.85  # Placeholder

    async def _get_baseline_performance(
        self, model_name: str, metric_name: str
    ) -> float | None:
        """Get baseline model performance."""
        return 0.88  # Placeholder

    async def _get_recent_data(self, model_name: str):
        """Get recent data for drift detection."""
        return None  # Placeholder

    async def _get_new_data_count(self, model_name: str) -> int:
        """Get count of new data samples."""
        return 5000  # Placeholder

    def add_job_callback(self, callback: Callable[[RetrainingJob], None]) -> None:
        """Add callback for job completion events."""
        self.job_callbacks.append(callback)

    def get_job(self, job_id: str) -> RetrainingJob | None:
        """Get job by ID."""
        return self.jobs.get(job_id)

    def list_jobs(
        self,
        model_name: str | None = None,
        status: RetrainingStatus | None = None,
    ) -> list[RetrainingJob]:
        """List retraining jobs."""
        jobs = list(self.jobs.values())

        if model_name:
            jobs = [job for job in jobs if job.model_name == model_name]

        if status:
            jobs = [job for job in jobs if job.status == status]

        return sorted(jobs, key=lambda x: x.created_at, reverse=True)

    def get_trigger(self, trigger_id: str) -> RetrainingTrigger | None:
        """Get trigger by ID."""
        return self.triggers.get(trigger_id)

    def list_triggers(self, model_name: str | None = None) -> list[RetrainingTrigger]:
        """List retraining triggers."""
        triggers = list(self.triggers.values())

        if model_name:
            triggers = [
                trigger for trigger in triggers if trigger.model_name == model_name
            ]

        return sorted(triggers, key=lambda x: x.created_at, reverse=True)
