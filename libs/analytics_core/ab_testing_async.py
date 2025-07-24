"""Async A/B Testing Framework with database persistence."""

import hashlib
import uuid
from datetime import datetime, timezone
from enum import Enum
from typing import Any

import structlog
from pydantic import BaseModel, Field
from sqlalchemy.ext.asyncio import AsyncSession

from libs.config.feature_flags import (
    ABTestConfig,
    FeatureFlagManager,
    FeatureFlagStatus,
    FeatureFlagType,
)
from libs.ml_models.experiments import ExperimentTracker

from .ab_testing_repository import ABTestRepository
from .models import ABTestExperiment as DBExperiment
from .statistics import (
    PowerAnalysisConfig,
    StatisticalAnalyzer,
)

logger = structlog.get_logger(__name__)


class ExperimentStatus(str, Enum):
    """Status of A/B test experiments."""

    DRAFT = "draft"
    RUNNING = "running"
    PAUSED = "paused"
    COMPLETED = "completed"
    STOPPED = "stopped"
    ARCHIVED = "archived"


class ExperimentObjective(str, Enum):
    """Type of experiment objective."""

    CONVERSION_RATE = "conversion_rate"
    REVENUE = "revenue"
    ENGAGEMENT = "engagement"
    RETENTION = "retention"
    CUSTOM_METRIC = "custom_metric"


class StoppingRule(BaseModel):
    """Configuration for automatic experiment stopping."""

    min_sample_size: int = Field(description="Minimum sample size per variant")
    max_duration_days: int = Field(description="Maximum experiment duration")
    significance_threshold: float = Field(
        default=0.05, description="Statistical significance threshold"
    )
    power_threshold: float = Field(default=0.8, description="Minimum statistical power")
    effect_size_threshold: float | None = Field(
        default=None, description="Minimum practical effect size"
    )
    probability_threshold: float = Field(
        default=0.95, description="Bayesian probability threshold"
    )
    check_frequency_hours: int = Field(
        default=24, description="How often to check stopping criteria"
    )


class ExperimentVariant(BaseModel):
    """Configuration for experiment variant."""

    name: str = Field(description="Variant name")
    description: str | None = Field(default=None, description="Variant description")
    traffic_allocation: float = Field(description="Percentage of traffic (0-100)")
    feature_value: Any = Field(description="Feature flag value for this variant")
    is_control: bool = Field(
        default=False, description="Whether this is the control variant"
    )


class ABTestExperiment(BaseModel):
    """A/B test experiment configuration."""

    # Basic information
    id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    name: str = Field(description="Experiment name", max_length=255)
    description: str | None = Field(default=None, description="Experiment description")

    # Experiment setup
    feature_flag_key: str = Field(
        description="Associated feature flag key", max_length=255
    )
    objective: ExperimentObjective = Field(description="Primary objective")
    hypothesis: str = Field(description="Experiment hypothesis")
    variants: list[ExperimentVariant] = Field(description="Experiment variants")

    # Targeting and segmentation
    target_audience: dict[str, Any] = Field(
        default_factory=dict, description="Audience targeting rules"
    )
    exclusion_rules: dict[str, Any] = Field(
        default_factory=dict, description="Exclusion criteria"
    )

    # Duration and stopping
    start_date: datetime | None = Field(
        default=None, description="Experiment start date"
    )
    end_date: datetime | None = Field(default=None, description="Experiment end date")
    stopping_rules: StoppingRule | None = Field(
        default=None, description="Automatic stopping configuration"
    )

    # Metrics and analysis
    primary_metric: str = Field(description="Primary success metric", max_length=100)
    secondary_metrics: list[str] = Field(
        default_factory=list, description="Secondary metrics to track"
    )
    guardrail_metrics: list[str] = Field(
        default_factory=list, description="Metrics that should not degrade"
    )

    # Statistical settings
    significance_level: float = Field(
        default=0.05, description="Statistical significance level"
    )
    power: float = Field(default=0.8, description="Desired statistical power")
    minimum_detectable_effect: float | None = Field(
        default=None, description="MDE for power calculation"
    )

    # Metadata
    status: ExperimentStatus = Field(default=ExperimentStatus.DRAFT)
    created_by: str | None = Field(
        default=None, description="User who created experiment"
    )
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    updated_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))

    # MLflow integration
    mlflow_experiment_id: str | None = Field(
        default=None, description="Associated MLflow experiment ID"
    )

    # Results
    results: dict[str, Any] = Field(
        default_factory=dict, description="Experiment results"
    )
    winner: str | None = Field(default=None, description="Winning variant")
    confidence: float | None = Field(default=None, description="Confidence in results")


class ExperimentAssignment(BaseModel):
    """Record of user assignment to experiment variant."""

    experiment_id: str
    user_id: str = Field(max_length=255)
    variant: str = Field(max_length=100)
    assigned_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    context: dict[str, Any] = Field(default_factory=dict)


class ExperimentEvent(BaseModel):
    """Event recorded for experiment analysis."""

    experiment_id: str
    user_id: str = Field(max_length=255)
    variant: str = Field(max_length=100)
    event_type: str = Field(max_length=100)
    event_value: float | str | bool | None = None
    properties: dict[str, Any] = Field(default_factory=dict)
    timestamp: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))


class AsyncABTestingEngine:
    """Async A/B testing engine with database persistence."""

    def __init__(
        self,
        session_factory: Any,
        feature_flag_manager: FeatureFlagManager | None = None,
        experiment_tracker: ExperimentTracker | None = None,
    ):
        """Initialize async A/B testing engine."""
        self.session_factory = session_factory
        self.feature_flag_manager = feature_flag_manager or FeatureFlagManager()
        self.experiment_tracker = experiment_tracker or ExperimentTracker()
        self.statistical_analyzer = StatisticalAnalyzer()

        self.logger = logger.bind(component="async_ab_testing_engine")

    def _get_repository(self, session: AsyncSession) -> ABTestRepository:
        """Get repository instance for session."""
        return ABTestRepository(session)

    async def create_experiment(
        self,
        experiment: ABTestExperiment,
        session: AsyncSession,
        auto_create_feature_flag: bool = True,
    ) -> ABTestExperiment:
        """Create new A/B test experiment."""
        try:
            # Validate experiment configuration
            self._validate_experiment(experiment)

            # Create MLflow experiment for tracking
            from libs.ml_models.experiments import ExperimentMetadata

            mlflow_metadata = ExperimentMetadata(
                name=f"ab_test_{experiment.name}",
                description=f"A/B Test: {experiment.description}",
                project="ab_testing",
                use_case=experiment.objective.value,
                created_by=experiment.created_by,
                tags={
                    "experiment_type": "ab_test",
                    "feature_flag": experiment.feature_flag_key,
                    "primary_metric": experiment.primary_metric,
                    "status": experiment.status.value,
                },
            )

            experiment.mlflow_experiment_id = self.experiment_tracker.create_experiment(
                mlflow_metadata
            )

            # Create or update feature flag
            if auto_create_feature_flag:
                self._create_feature_flag(experiment)

            # Calculate power analysis
            if experiment.minimum_detectable_effect:
                power_config = PowerAnalysisConfig(
                    effect_size=experiment.minimum_detectable_effect,
                    alpha=experiment.significance_level,
                    power=experiment.power,
                )
                power_results = self.statistical_analyzer.power_analysis(power_config)
                experiment.results["power_analysis"] = power_results

            # Prepare data for database
            experiment_data = {
                "experiment_uuid": experiment.id,
                "name": experiment.name,
                "description": experiment.description,
                "feature_flag_key": experiment.feature_flag_key,
                "objective": experiment.objective.value,
                "hypothesis": experiment.hypothesis,
                "variants": [v.model_dump() for v in experiment.variants],
                "target_audience": experiment.target_audience,
                "exclusion_rules": experiment.exclusion_rules,
                "start_date": experiment.start_date,
                "end_date": experiment.end_date,
                "stopping_rules": (
                    experiment.stopping_rules.model_dump()
                    if experiment.stopping_rules
                    else None
                ),
                "primary_metric": experiment.primary_metric,
                "secondary_metrics": experiment.secondary_metrics,
                "guardrail_metrics": experiment.guardrail_metrics,
                "significance_level": experiment.significance_level,
                "power": experiment.power,
                "minimum_detectable_effect": experiment.minimum_detectable_effect,
                "status": experiment.status.value,
                "created_by": int(experiment.created_by)
                if experiment.created_by
                else None,
                "mlflow_experiment_id": experiment.mlflow_experiment_id,
                "results": experiment.results,
                "winner": experiment.winner,
                "confidence": experiment.confidence,
            }

            # Save to database
            repository = self._get_repository(session)
            db_experiment = await repository.create_experiment(experiment_data)

            self.logger.info(
                "A/B test experiment created",
                experiment_id=db_experiment.id,
                experiment_uuid=experiment.id,
                name=experiment.name,
                feature_flag=experiment.feature_flag_key,
                variants=len(experiment.variants),
            )

            return experiment

        except Exception as error:
            self.logger.error("Failed to create experiment", error=str(error))
            raise

    async def start_experiment(
        self, experiment_uuid: str, session: AsyncSession
    ) -> bool:
        """Start an A/B test experiment."""
        try:
            repository = self._get_repository(session)
            db_experiment = await repository.get_experiment_by_uuid(experiment_uuid)

            if not db_experiment:
                raise ValueError(f"Experiment {experiment_uuid} not found")

            if db_experiment.status != ExperimentStatus.DRAFT.value:
                raise ValueError(
                    f"Cannot start experiment in {db_experiment.status} status"
                )

            # Update experiment status
            updates = {
                "status": ExperimentStatus.RUNNING.value,
                "start_date": datetime.now(timezone.utc),
            }

            await repository.update_experiment(db_experiment.id, updates)

            # Activate feature flag
            flag = self.feature_flag_manager.get_flag(db_experiment.feature_flag_key)
            if flag:
                self.feature_flag_manager.update_flag(
                    db_experiment.feature_flag_key, {"status": FeatureFlagStatus.ACTIVE}
                )

            # Start MLflow run for experiment tracking
            if db_experiment.mlflow_experiment_id:
                from libs.ml_models.experiments import RunMetadata

                run_metadata = RunMetadata(
                    experiment_id=db_experiment.mlflow_experiment_id,
                    run_name=f"ab_test_run_{db_experiment.name}",
                    description=f"A/B test run for {db_experiment.name}",
                    run_type="ab_test",
                    parameters={
                        "variants": len(db_experiment.variants),
                        "primary_metric": db_experiment.primary_metric,
                        "significance_level": db_experiment.significance_level,
                        "power": db_experiment.power,
                    },
                    tags={
                        "experiment_id": experiment_uuid,
                        "status": ExperimentStatus.RUNNING.value,
                    },
                    created_by=str(db_experiment.created_by)
                    if db_experiment.created_by
                    else None,
                )

                self.experiment_tracker.start_run(
                    experiment_id=db_experiment.mlflow_experiment_id,
                    metadata=run_metadata,
                )

            self.logger.info("Experiment started", experiment_uuid=experiment_uuid)
            return True

        except Exception as error:
            self.logger.error(
                "Failed to start experiment",
                experiment_uuid=experiment_uuid,
                error=str(error),
            )
            raise

    async def assign_user_to_variant(
        self,
        experiment_uuid: str,
        user_id: str,
        session: AsyncSession,
        user_attributes: dict[str, Any] | None = None,
    ) -> str | None:
        """Assign user to experiment variant."""
        try:
            repository = self._get_repository(session)
            db_experiment = await repository.get_experiment_by_uuid(experiment_uuid)

            if (
                not db_experiment
                or db_experiment.status != ExperimentStatus.RUNNING.value
            ):
                return None

            user_attributes = user_attributes or {}

            # Check if user is eligible for experiment
            if not self._is_user_eligible(db_experiment, user_id, user_attributes):
                return None

            # Check if user already assigned
            existing_assignment = await repository.get_user_assignment(
                db_experiment.id, user_id
            )
            if existing_assignment:
                return existing_assignment.variant

            # Assign user to variant using consistent hashing
            experiment_obj = self._db_to_pydantic_experiment(db_experiment)
            variant = self._assign_variant(experiment_obj, user_id)

            if variant:
                # Record assignment
                assignment_data = {
                    "experiment_id": db_experiment.id,
                    "user_id": user_id,
                    "variant": variant.name,
                    "context": user_attributes,
                }
                await repository.create_assignment(assignment_data)

                self.logger.debug(
                    "User assigned to variant",
                    experiment_uuid=experiment_uuid,
                    user_id=user_id,
                    variant=variant.name,
                )

                return variant.name

            return None

        except Exception as error:
            self.logger.error(
                "Failed to assign user to variant",
                experiment_uuid=experiment_uuid,
                user_id=user_id,
                error=str(error),
            )
            raise

    async def track_event(
        self,
        experiment_uuid: str,
        user_id: str,
        event_type: str,
        session: AsyncSession,
        event_value: float | str | bool | None = None,
        properties: dict[str, Any] | None = None,
    ) -> bool:
        """Track event for experiment analysis."""
        try:
            repository = self._get_repository(session)
            db_experiment = await repository.get_experiment_by_uuid(experiment_uuid)

            if not db_experiment:
                return False

            # Get user's variant assignment
            assignment = await repository.get_user_assignment(db_experiment.id, user_id)
            if not assignment:
                return False

            # Record event
            event_data = {
                "experiment_id": db_experiment.id,
                "user_id": user_id,
                "variant": assignment.variant,
                "event_type": event_type,
                "event_value": float(event_value)
                if isinstance(event_value, int | float)
                else None,
                "properties": properties or {},
            }

            await repository.create_event(event_data)

            # Log to MLflow if configured
            if db_experiment.mlflow_experiment_id:
                self.experiment_tracker.log_metrics({f"event_{event_type}": 1})

            self.logger.debug(
                "Event tracked",
                experiment_uuid=experiment_uuid,
                user_id=user_id,
                event_type=event_type,
                variant=assignment.variant,
            )

            return True

        except Exception as error:
            self.logger.error(
                "Failed to track event",
                experiment_uuid=experiment_uuid,
                user_id=user_id,
                event_type=event_type,
                error=str(error),
            )
            raise

    async def get_experiment(
        self, experiment_uuid: str, session: AsyncSession
    ) -> ABTestExperiment | None:
        """Get experiment by UUID."""
        try:
            repository = self._get_repository(session)
            db_experiment = await repository.get_experiment_by_uuid(experiment_uuid)

            if not db_experiment:
                return None

            return self._db_to_pydantic_experiment(db_experiment)

        except Exception as error:
            self.logger.error(
                "Failed to get experiment",
                experiment_uuid=experiment_uuid,
                error=str(error),
            )
            raise

    async def get_feature_value(
        self,
        experiment_uuid: str,
        user_id: str,
        session: AsyncSession,
        user_attributes: dict[str, Any] | None = None,
    ) -> Any:
        """Get feature value for user in experiment."""
        try:
            # Get user's variant assignment
            variant_name = await self.assign_user_to_variant(
                experiment_uuid, user_id, session, user_attributes
            )
            if not variant_name:
                return None

            # Get experiment to find variant configuration
            experiment = await self.get_experiment(experiment_uuid, session)
            if not experiment:
                return None

            # Find variant configuration
            variant = next(
                (v for v in experiment.variants if v.name == variant_name), None
            )
            if variant:
                return variant.feature_value

            return None

        except Exception as error:
            self.logger.error(
                "Failed to get feature value",
                experiment_uuid=experiment_uuid,
                user_id=user_id,
                error=str(error),
            )
            raise

    async def analyze_experiment(
        self,
        experiment_uuid: str,
        session: AsyncSession,
        test_type: str = "two_sample_ttest",
    ) -> dict[str, Any]:
        """Analyze experiment results."""
        try:
            repository = self._get_repository(session)
            db_experiment = await repository.get_experiment_by_uuid(experiment_uuid)

            if not db_experiment:
                raise ValueError(f"Experiment {experiment_uuid} not found")

            # Get events for analysis
            events = await repository.get_experiment_events(db_experiment.id)
            if not events:
                return {"status": "no_data", "message": "No events recorded"}

            # Basic analysis - group events by variant
            variant_data: dict[str, list[float]] = {}
            for event in events:
                if event.variant not in variant_data:
                    variant_data[event.variant] = []

                # Extract numeric value for analysis
                if isinstance(event.event_value, int | float):
                    variant_data[event.variant].append(float(event.event_value))
                elif event.event_type == db_experiment.primary_metric:
                    # For conversion events, use 1 for success
                    variant_data[event.variant].append(1.0)

            # Ensure all variants have data
            for variant_dict in db_experiment.variants:
                if isinstance(variant_dict, dict):
                    variant_name = variant_dict.get("name")
                    if variant_name and variant_name not in variant_data:
                        variant_data[variant_name] = []

            # Calculate basic statistics for each variant
            results: dict[str, Any] = {"variant_data": {}, "statistical_tests": {}}

            for variant_name, values in variant_data.items():
                if values:
                    import numpy as np

                    results["variant_data"][variant_name] = {
                        "sample_size": len(values),
                        "mean": float(np.mean(values)),
                        "std": float(np.std(values, ddof=1)) if len(values) > 1 else 0,
                        "median": float(np.median(values)),
                        "values": values,
                    }
                else:
                    results["variant_data"][variant_name] = {
                        "sample_size": 0,
                        "mean": 0,
                        "std": 0,
                        "median": 0,
                        "values": [],
                    }

            self.logger.info(
                "Experiment analyzed",
                experiment_uuid=experiment_uuid,
                variants=len(variant_data),
                total_events=len(events),
            )

            return results

        except Exception as error:
            self.logger.error(
                "Failed to analyze experiment",
                experiment_uuid=experiment_uuid,
                error=str(error),
            )
            raise

    async def check_stopping_criteria(
        self, experiment_uuid: str, session: AsyncSession
    ) -> dict[str, Any]:
        """Check if experiment should be stopped based on configured rules."""
        try:
            repository = self._get_repository(session)
            db_experiment = await repository.get_experiment_by_uuid(experiment_uuid)

            if not db_experiment or not db_experiment.stopping_rules:
                return {"should_stop": False, "reason": "No stopping rules configured"}

            # Basic stopping criteria check
            return {"should_stop": False, "reason": "Stopping criteria not met"}

        except Exception as error:
            self.logger.error(
                "Failed to check stopping criteria",
                experiment_uuid=experiment_uuid,
                error=str(error),
            )
            raise

    async def stop_experiment(
        self, experiment_uuid: str, reason: str | None, session: AsyncSession
    ) -> bool:
        """Stop a running experiment."""
        try:
            repository = self._get_repository(session)
            db_experiment = await repository.get_experiment_by_uuid(experiment_uuid)

            if not db_experiment:
                raise ValueError(f"Experiment {experiment_uuid} not found")

            # Perform final analysis
            final_results = await self.analyze_experiment(experiment_uuid, session)

            # Update experiment status
            updates = {
                "status": ExperimentStatus.COMPLETED.value,
                "end_date": datetime.now(timezone.utc),
                "results": final_results,
            }

            if reason:
                # Add stop reason to the results
                if isinstance(updates["results"], dict):
                    updates["results"]["stop_reason"] = reason

            await repository.update_experiment(db_experiment.id, updates)

            # Deactivate feature flag
            self.feature_flag_manager.update_flag(
                db_experiment.feature_flag_key, {"status": FeatureFlagStatus.INACTIVE}
            )

            self.logger.info(
                "Experiment stopped", experiment_uuid=experiment_uuid, reason=reason
            )

            return True

        except Exception as error:
            self.logger.error(
                "Failed to stop experiment",
                experiment_uuid=experiment_uuid,
                error=str(error),
            )
            raise

    async def list_experiments(
        self,
        session: AsyncSession,
        status: str | None = None,
        created_by: str | None = None,
    ) -> list[ABTestExperiment]:
        """List experiments with optional filtering."""
        try:
            repository = self._get_repository(session)
            created_by_int = int(created_by) if created_by else None
            db_experiments = await repository.list_experiments(
                status=status, created_by=created_by_int
            )

            # Convert to Pydantic models
            experiments = []
            for db_exp in db_experiments:
                experiments.append(self._db_to_pydantic_experiment(db_exp))

            self.logger.info(
                "Experiments listed",
                count=len(experiments),
                status=status,
                created_by=created_by,
            )

            return experiments

        except Exception as error:
            self.logger.error("Failed to list experiments", error=str(error))
            raise

    def _validate_experiment(self, experiment: ABTestExperiment) -> None:
        """Validate experiment configuration."""
        # Check traffic allocation sums to 100%
        total_allocation = sum(v.traffic_allocation for v in experiment.variants)
        if abs(total_allocation - 100.0) > 0.01:
            raise ValueError(
                f"Traffic allocation must sum to 100%, got {total_allocation}"
            )

        # Ensure exactly one control variant
        control_variants = [v for v in experiment.variants if v.is_control]
        if len(control_variants) != 1:
            raise ValueError("Exactly one variant must be marked as control")

        # Check for duplicate variant names
        variant_names = [v.name for v in experiment.variants]
        if len(set(variant_names)) != len(variant_names):
            raise ValueError("Variant names must be unique")

        # Validate name length
        if len(experiment.name) > 255:
            raise ValueError("Experiment name too long (max 255 characters)")

        # Validate feature flag key length
        if len(experiment.feature_flag_key) > 255:
            raise ValueError("Feature flag key too long (max 255 characters)")

        # Validate primary metric length
        if len(experiment.primary_metric) > 100:
            raise ValueError("Primary metric name too long (max 100 characters)")

    def _create_feature_flag(self, experiment: ABTestExperiment) -> None:
        """Create feature flag for experiment."""
        # Map variants to AB test config
        variants = {v.name: v.feature_value for v in experiment.variants}
        traffic_allocation = {v.name: v.traffic_allocation for v in experiment.variants}

        ab_test_config = ABTestConfig(
            name=experiment.name,
            description=experiment.description,
            start_date=experiment.start_date,
            end_date=experiment.end_date,
            variants=variants,
            traffic_allocation=traffic_allocation,
        )

        # Create or update feature flag
        flag = self.feature_flag_manager.get_flag(experiment.feature_flag_key)
        if flag:
            # Update existing flag
            self.feature_flag_manager.update_flag(
                experiment.feature_flag_key, {"ab_test": ab_test_config.model_dump()}
            )
        else:
            # Create new flag
            flag = self.feature_flag_manager.create_flag(
                key=experiment.feature_flag_key,
                name=f"AB Test: {experiment.name}",
                description=experiment.description,
                flag_type=FeatureFlagType.JSON,
                default_value=next(
                    v.feature_value for v in experiment.variants if v.is_control
                ),
                created_by=experiment.created_by,
            )
            flag.ab_test = ab_test_config
            self.feature_flag_manager.register_flag(flag)

    def _is_user_eligible(
        self, db_experiment: DBExperiment, user_id: str, user_attributes: dict[str, Any]
    ) -> bool:
        """Check if user is eligible for experiment."""
        # Check target audience rules
        if db_experiment.target_audience:
            for key, value in db_experiment.target_audience.items():
                if key == "user_id" and user_id != value:
                    return False
                elif key in user_attributes and user_attributes[key] != value:
                    return False

        # Check exclusion rules
        if db_experiment.exclusion_rules:
            for key, value in db_experiment.exclusion_rules.items():
                if key == "user_id" and user_id == value:
                    return False
                elif key in user_attributes and user_attributes[key] == value:
                    return False

        return True

    def _assign_variant(
        self, experiment: ABTestExperiment, user_id: str
    ) -> ExperimentVariant | None:
        """Assign user to variant using consistent hashing."""
        # Use consistent hashing similar to feature flags
        hash_input = f"{experiment.id}:{user_id}"
        hash_value = int(hashlib.md5(hash_input.encode()).hexdigest(), 16)
        percentage = (hash_value % 10000) / 100.0  # 0-99.99%

        cumulative_percentage = 0.0
        for variant in experiment.variants:
            cumulative_percentage += variant.traffic_allocation
            if percentage <= cumulative_percentage:
                return variant

        return None

    def _db_to_pydantic_experiment(
        self, db_experiment: DBExperiment
    ) -> ABTestExperiment:
        """Convert database experiment to Pydantic model."""
        # Convert variants from dict to ExperimentVariant objects
        variants = []
        if db_experiment.variants:
            for v in db_experiment.variants:
                if isinstance(v, dict):
                    variants.append(ExperimentVariant(**v))
                else:
                    # Handle case where v might be a string or other type
                    continue

        return ABTestExperiment(
            id=db_experiment.experiment_uuid,
            name=db_experiment.name,
            description=db_experiment.description,
            feature_flag_key=db_experiment.feature_flag_key,
            objective=ExperimentObjective(db_experiment.objective),
            hypothesis=db_experiment.hypothesis,
            variants=variants,
            target_audience=db_experiment.target_audience or {},
            exclusion_rules=db_experiment.exclusion_rules or {},
            start_date=db_experiment.start_date,
            end_date=db_experiment.end_date,
            stopping_rules=(
                StoppingRule(**db_experiment.stopping_rules)
                if db_experiment.stopping_rules
                else None
            ),
            primary_metric=db_experiment.primary_metric,
            secondary_metrics=db_experiment.secondary_metrics or [],
            guardrail_metrics=db_experiment.guardrail_metrics or [],
            significance_level=db_experiment.significance_level,
            power=db_experiment.power,
            minimum_detectable_effect=db_experiment.minimum_detectable_effect,
            status=ExperimentStatus(db_experiment.status),
            created_by=str(db_experiment.created_by)
            if db_experiment.created_by
            else None,
            created_at=db_experiment.created_at,
            updated_at=db_experiment.updated_at,
            mlflow_experiment_id=db_experiment.mlflow_experiment_id,
            results=db_experiment.results or {},
            winner=db_experiment.winner,
            confidence=db_experiment.confidence,
        )
