"""A/B Testing Framework integrating feature flags with experiment tracking."""

import hashlib
import uuid
from datetime import datetime, timezone
from enum import Enum
from typing import Any

import numpy as np
import structlog
from pydantic import BaseModel, Field

from libs.config.feature_flags import (
    ABTestConfig,
    FeatureFlagManager,
    FeatureFlagStatus,
    FeatureFlagType,
)

# MLflow integration - using the experiments.py module instead
from libs.ml_models.experiments import ExperimentTracker

from .statistics import (
    MultipleComparisonMethod,
    PowerAnalysisConfig,
    StatisticalAnalyzer,
    TestType,
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
    significance_threshold: float = Field(default=0.05, description="Statistical significance threshold")
    power_threshold: float = Field(default=0.8, description="Minimum statistical power")
    effect_size_threshold: float | None = Field(default=None, description="Minimum practical effect size")
    probability_threshold: float = Field(default=0.95, description="Bayesian probability threshold")
    check_frequency_hours: int = Field(default=24, description="How often to check stopping criteria")


class ExperimentVariant(BaseModel):
    """Configuration for experiment variant."""

    name: str = Field(description="Variant name")
    description: str | None = Field(default=None, description="Variant description")
    traffic_allocation: float = Field(description="Percentage of traffic (0-100)")
    feature_value: Any = Field(description="Feature flag value for this variant")
    is_control: bool = Field(default=False, description="Whether this is the control variant")


class ABTestExperiment(BaseModel):
    """A/B test experiment configuration."""

    # Basic information
    id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    name: str = Field(description="Experiment name")
    description: str | None = Field(default=None, description="Experiment description")

    # Experiment setup
    feature_flag_key: str = Field(description="Associated feature flag key")
    objective: ExperimentObjective = Field(description="Primary objective")
    hypothesis: str = Field(description="Experiment hypothesis")
    variants: list[ExperimentVariant] = Field(description="Experiment variants")

    # Targeting and segmentation
    target_audience: dict[str, Any] = Field(default_factory=dict, description="Audience targeting rules")
    exclusion_rules: dict[str, Any] = Field(default_factory=dict, description="Exclusion criteria")

    # Duration and stopping
    start_date: datetime | None = Field(default=None, description="Experiment start date")
    end_date: datetime | None = Field(default=None, description="Experiment end date")
    stopping_rules: StoppingRule | None = Field(default=None, description="Automatic stopping configuration")

    # Metrics and analysis
    primary_metric: str = Field(description="Primary success metric")
    secondary_metrics: list[str] = Field(default_factory=list, description="Secondary metrics to track")
    guardrail_metrics: list[str] = Field(default_factory=list, description="Metrics that should not degrade")

    # Statistical settings
    significance_level: float = Field(default=0.05, description="Statistical significance level")
    power: float = Field(default=0.8, description="Desired statistical power")
    minimum_detectable_effect: float | None = Field(default=None, description="MDE for power calculation")

    # Metadata
    status: ExperimentStatus = Field(default=ExperimentStatus.DRAFT)
    created_by: str | None = Field(default=None, description="User who created experiment")
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    updated_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))

    # MLflow integration
    mlflow_experiment_id: str | None = Field(default=None, description="Associated MLflow experiment ID")

    # Results
    results: dict[str, Any] = Field(default_factory=dict, description="Experiment results")
    winner: str | None = Field(default=None, description="Winning variant")
    confidence: float | None = Field(default=None, description="Confidence in results")


class ExperimentAssignment(BaseModel):
    """Record of user assignment to experiment variant."""

    experiment_id: str
    user_id: str
    variant: str
    assigned_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    context: dict[str, Any] = Field(default_factory=dict)


class ExperimentEvent(BaseModel):
    """Event recorded for experiment analysis."""

    experiment_id: str
    user_id: str
    variant: str
    event_type: str
    event_value: float | str | bool | None = None
    properties: dict[str, Any] = Field(default_factory=dict)
    timestamp: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))


class ABTestingEngine:
    """Core A/B testing engine."""

    def __init__(
        self,
        feature_flag_manager: FeatureFlagManager | None = None,
        experiment_tracker: ExperimentTracker | None = None
    ):
        """Initialize A/B testing engine."""
        self.feature_flag_manager = feature_flag_manager or FeatureFlagManager()
        self.experiment_tracker = experiment_tracker or ExperimentTracker()
        self.statistical_analyzer = StatisticalAnalyzer()

        # Storage for experiments and events (in production, use database)
        self.experiments: dict[str, ABTestExperiment] = {}
        self.assignments: dict[str, list[ExperimentAssignment]] = {}
        self.events: dict[str, list[ExperimentEvent]] = {}

        self.logger = logger.bind(component="ab_testing_engine")

    def create_experiment(
        self,
        experiment: ABTestExperiment,
        auto_create_feature_flag: bool = True
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
                    "status": experiment.status.value
                }
            )

            experiment.mlflow_experiment_id = self.experiment_tracker.create_experiment(mlflow_metadata)

            # Create or update feature flag
            if auto_create_feature_flag:
                self._create_feature_flag(experiment)

            # Calculate power analysis
            if experiment.minimum_detectable_effect:
                power_config = PowerAnalysisConfig(
                    effect_size=experiment.minimum_detectable_effect,
                    alpha=experiment.significance_level,
                    power=experiment.power
                )
                power_results = self.statistical_analyzer.power_analysis(power_config)
                experiment.results["power_analysis"] = power_results

            # Store experiment
            self.experiments[experiment.id] = experiment
            self.assignments[experiment.id] = []
            self.events[experiment.id] = []

            self.logger.info(
                "A/B test experiment created",
                experiment_id=experiment.id,
                name=experiment.name,
                feature_flag=experiment.feature_flag_key,
                variants=len(experiment.variants)
            )

            return experiment

        except Exception as error:
            self.logger.error("Failed to create experiment", error=str(error))
            raise

    def start_experiment(self, experiment_id: str) -> bool:
        """Start an A/B test experiment."""
        try:
            experiment = self.experiments.get(experiment_id)
            if not experiment:
                raise ValueError(f"Experiment {experiment_id} not found")

            if experiment.status != ExperimentStatus.DRAFT:
                raise ValueError(f"Cannot start experiment in {experiment.status} status")

            # Update experiment status
            experiment.status = ExperimentStatus.RUNNING
            experiment.start_date = datetime.now(timezone.utc)
            experiment.updated_at = datetime.now(timezone.utc)

            # Activate feature flag
            flag = self.feature_flag_manager.get_flag(experiment.feature_flag_key)
            if flag:
                self.feature_flag_manager.update_flag(
                    experiment.feature_flag_key,
                    {"status": FeatureFlagStatus.ACTIVE}
                )

            # Start MLflow run for experiment tracking
            if experiment.mlflow_experiment_id:
                from libs.ml_models.experiments import RunMetadata
                run_metadata = RunMetadata(
                    experiment_id=experiment.mlflow_experiment_id,
                    run_name=f"ab_test_run_{experiment.name}",
                    description=f"A/B test run for {experiment.name}",
                    run_type="ab_test",
                    parameters={
                        "variants": len(experiment.variants),
                        "primary_metric": experiment.primary_metric,
                        "significance_level": experiment.significance_level,
                        "power": experiment.power
                    },
                    tags={
                        "experiment_id": experiment.id,
                        "status": experiment.status.value
                    },
                    created_by=experiment.created_by
                )

                self.experiment_tracker.start_run(
                    experiment_id=experiment.mlflow_experiment_id,
                    metadata=run_metadata
                )

            self.logger.info("Experiment started", experiment_id=experiment_id)
            return True

        except Exception as error:
            self.logger.error("Failed to start experiment",
                            experiment_id=experiment_id, error=str(error))
            raise

    def assign_user_to_variant(
        self,
        experiment_id: str,
        user_id: str,
        user_attributes: dict[str, Any] | None = None
    ) -> str | None:
        """Assign user to experiment variant."""
        try:
            experiment = self.experiments.get(experiment_id)
            if not experiment or experiment.status != ExperimentStatus.RUNNING:
                return None

            user_attributes = user_attributes or {}

            # Check if user is eligible for experiment
            if not self._is_user_eligible(experiment, user_id, user_attributes):
                return None

            # Check if user already assigned
            existing_assignment = self._get_user_assignment(experiment_id, user_id)
            if existing_assignment:
                return existing_assignment.variant

            # Assign user to variant using consistent hashing
            variant = self._assign_variant(experiment, user_id)

            if variant:
                # Record assignment
                assignment = ExperimentAssignment(
                    experiment_id=experiment_id,
                    user_id=user_id,
                    variant=variant.name,
                    context=user_attributes
                )
                self.assignments[experiment_id].append(assignment)

                self.logger.debug(
                    "User assigned to variant",
                    experiment_id=experiment_id,
                    user_id=user_id,
                    variant=variant.name
                )

                return variant.name

            return None

        except Exception as error:
            self.logger.error(
                "Failed to assign user to variant",
                experiment_id=experiment_id,
                user_id=user_id,
                error=str(error)
            )
            raise

    def get_feature_value(
        self,
        experiment_id: str,
        user_id: str,
        user_attributes: dict[str, Any] | None = None
    ) -> Any:
        """Get feature value for user in experiment."""
        try:
            experiment = self.experiments.get(experiment_id)
            if not experiment:
                return None

            # Get user's variant assignment
            variant_name = self.assign_user_to_variant(experiment_id, user_id, user_attributes)
            if not variant_name:
                return None

            # Find variant configuration
            variant = next((v for v in experiment.variants if v.name == variant_name), None)
            if variant:
                return variant.feature_value

            return None

        except Exception as error:
            self.logger.error(
                "Failed to get feature value",
                experiment_id=experiment_id,
                user_id=user_id,
                error=str(error)
            )
            raise

    def track_event(
        self,
        experiment_id: str,
        user_id: str,
        event_type: str,
        event_value: float | str | bool | None = None,
        properties: dict[str, Any] | None = None
    ) -> bool:
        """Track event for experiment analysis."""
        try:
            experiment = self.experiments.get(experiment_id)
            if not experiment:
                return False

            # Get user's variant assignment
            assignment = self._get_user_assignment(experiment_id, user_id)
            if not assignment:
                return False

            # Record event
            event = ExperimentEvent(
                experiment_id=experiment_id,
                user_id=user_id,
                variant=assignment.variant,
                event_type=event_type,
                event_value=event_value,
                properties=properties or {}
            )

            self.events[experiment_id].append(event)

            # Log to MLflow if configured
            if experiment.mlflow_experiment_id:
                self.experiment_tracker.log_metrics({
                    f"event_{event_type}": 1
                })

            self.logger.debug(
                "Event tracked",
                experiment_id=experiment_id,
                user_id=user_id,
                event_type=event_type,
                variant=assignment.variant
            )

            return True

        except Exception as error:
            self.logger.error(
                "Failed to track event",
                experiment_id=experiment_id,
                user_id=user_id,
                event_type=event_type,
                error=str(error)
            )
            raise

    def analyze_experiment(
        self,
        experiment_id: str,
        test_type: TestType = TestType.TWO_SAMPLE_TTEST
    ) -> dict[str, Any]:
        """Analyze experiment results."""
        try:
            experiment = self.experiments.get(experiment_id)
            if not experiment:
                raise ValueError(f"Experiment {experiment_id} not found")

            # Get events for analysis
            events = self.events.get(experiment_id, [])
            if not events:
                return {"status": "no_data", "message": "No events recorded"}

            # Group events by variant
            variant_data: dict[str, list[float]] = {}
            for event in events:
                if event.variant not in variant_data:
                    variant_data[event.variant] = []

                # Extract numeric value for analysis
                if isinstance(event.event_value, int | float):
                    variant_data[event.variant].append(float(event.event_value))
                elif event.event_type == experiment.primary_metric:
                    # For conversion events, use 1 for success, 0 for no event
                    variant_data[event.variant].append(1.0)

            # Ensure all variants have data
            for variant in experiment.variants:
                if variant.name not in variant_data:
                    variant_data[variant.name] = []

            # Perform statistical analysis
            results: dict[str, Any] = {"variant_data": {}, "statistical_tests": {}}

            # Calculate basic statistics for each variant
            for variant_name, values in variant_data.items():
                if values:
                    results["variant_data"][variant_name] = {
                        "sample_size": len(values),
                        "mean": float(np.mean(values)),
                        "std": float(np.std(values, ddof=1)) if len(values) > 1 else 0,
                        "median": float(np.median(values)),
                        "values": values
                    }
                else:
                    results["variant_data"][variant_name] = {
                        "sample_size": 0,
                        "mean": 0,
                        "std": 0,
                        "median": 0,
                        "values": []
                    }

            # Pairwise comparisons between variants
            variant_names = list(variant_data.keys())
            for i in range(len(variant_names)):
                for j in range(i + 1, len(variant_names)):
                    variant_a = variant_names[i]
                    variant_b = variant_names[j]

                    data_a = variant_data[variant_a]
                    data_b = variant_data[variant_b]

                    if data_a and data_b:
                        # Perform statistical test
                        if test_type == TestType.TWO_SAMPLE_TTEST:
                            test_result = self.statistical_analyzer.two_sample_ttest(
                                data_a, data_b, alpha=experiment.significance_level
                            )
                        elif test_type == TestType.MANN_WHITNEY:
                            test_result = self.statistical_analyzer.mann_whitney_u(
                                data_a, data_b, alpha=experiment.significance_level
                            )
                        else:
                            continue

                        results["statistical_tests"][f"{variant_a}_vs_{variant_b}"] = {
                            "test_type": test_result.test_type.value,
                            "statistic": test_result.statistic,
                            "p_value": test_result.p_value,
                            "effect_size": test_result.effect_size,
                            "is_significant": test_result.is_significant,
                            "confidence_interval": test_result.confidence_interval
                        }

            # Multiple comparison correction if more than 2 variants
            if len(variant_names) > 2:
                p_values = [
                    test["p_value"]
                    for test in results["statistical_tests"].values()
                ]
                if p_values:
                    correction = self.statistical_analyzer.multiple_comparison_correction(
                        p_values,
                        method=MultipleComparisonMethod.BENJAMINI_HOCHBERG,
                        alpha=experiment.significance_level
                    )
                    results["multiple_comparison_correction"] = correction

            # Update experiment results
            experiment.results.update(results)
            experiment.updated_at = datetime.now(timezone.utc)

            # Log results to MLflow
            if experiment.mlflow_experiment_id:
                for variant_name, stats in results["variant_data"].items():
                    self.experiment_tracker.log_metrics({
                        f"{variant_name}_sample_size": stats["sample_size"],
                        f"{variant_name}_mean": stats["mean"],
                        f"{variant_name}_std": stats["std"]
                    })

            self.logger.info(
                "Experiment analyzed",
                experiment_id=experiment_id,
                variants=len(variant_names),
                total_events=len(events)
            )

            return results

        except Exception as error:
            self.logger.error(
                "Failed to analyze experiment",
                experiment_id=experiment_id,
                error=str(error)
            )
            raise

    def check_stopping_criteria(self, experiment_id: str) -> dict[str, Any]:
        """Check if experiment should be stopped based on configured rules."""
        try:
            experiment = self.experiments.get(experiment_id)
            if not experiment or not experiment.stopping_rules:
                return {"should_stop": False, "reason": "No stopping rules configured"}

            rules = experiment.stopping_rules
            results = self.analyze_experiment(experiment_id)

            # Check minimum sample size
            for variant_name, stats in results.get("variant_data", {}).items():
                if stats["sample_size"] < rules.min_sample_size:
                    return {
                        "should_stop": False,
                        "reason": f"Insufficient sample size for {variant_name}: {stats['sample_size']} < {rules.min_sample_size}"
                    }

            # Check maximum duration
            if experiment.start_date:
                duration = datetime.now(timezone.utc) - experiment.start_date
                if duration.days >= rules.max_duration_days:
                    return {
                        "should_stop": True,
                        "reason": f"Maximum duration reached: {duration.days} days"
                    }

            # Check statistical significance
            statistical_tests = results.get("statistical_tests", {})
            for comparison, test_result in statistical_tests.items():
                if test_result["is_significant"]:
                    return {
                        "should_stop": True,
                        "reason": f"Statistical significance achieved for {comparison}",
                        "p_value": test_result["p_value"],
                        "effect_size": test_result["effect_size"]
                    }

            return {"should_stop": False, "reason": "Stopping criteria not met"}

        except Exception as error:
            self.logger.error(
                "Failed to check stopping criteria",
                experiment_id=experiment_id,
                error=str(error)
            )
            raise

    def stop_experiment(self, experiment_id: str, reason: str | None = None) -> bool:
        """Stop a running experiment."""
        try:
            experiment = self.experiments.get(experiment_id)
            if not experiment:
                raise ValueError(f"Experiment {experiment_id} not found")

            # Perform final analysis
            final_results = self.analyze_experiment(experiment_id)

            # Update experiment status
            experiment.status = ExperimentStatus.COMPLETED
            experiment.end_date = datetime.now(timezone.utc)
            experiment.updated_at = datetime.now(timezone.utc)
            experiment.results["final_analysis"] = final_results
            experiment.results["stop_reason"] = reason

            # Determine winner (simplified logic)
            if final_results.get("statistical_tests"):
                # Find variant with best performance
                best_variant = None
                best_mean = float('-inf')

                for variant_name, stats in final_results["variant_data"].items():
                    if stats["mean"] > best_mean:
                        best_mean = stats["mean"]
                        best_variant = variant_name

                experiment.winner = best_variant

                # Calculate confidence based on statistical significance
                p_values = [
                    test["p_value"]
                    for test in final_results["statistical_tests"].values()
                ]
                if p_values:
                    min_p_value = min(p_values)
                    experiment.confidence = 1 - min_p_value

            # Deactivate feature flag
            self.feature_flag_manager.update_flag(
                experiment.feature_flag_key,
                {"status": FeatureFlagStatus.INACTIVE}
            )

            # End MLflow run
            if experiment.mlflow_experiment_id:
                self.experiment_tracker.log_metrics({
                    "experiment_duration_days": (experiment.end_date - experiment.start_date).days if experiment.start_date else 0,
                    "total_assignments": len(self.assignments.get(experiment_id, [])),
                    "total_events": len(self.events.get(experiment_id, []))
                })
                self.experiment_tracker.end_run("FINISHED")

            self.logger.info(
                "Experiment stopped",
                experiment_id=experiment_id,
                reason=reason,
                winner=experiment.winner,
                confidence=experiment.confidence
            )

            return True

        except Exception as error:
            self.logger.error(
                "Failed to stop experiment",
                experiment_id=experiment_id,
                error=str(error)
            )
            raise

    def get_experiment(self, experiment_id: str) -> ABTestExperiment | None:
        """Get experiment by ID."""
        return self.experiments.get(experiment_id)

    def list_experiments(
        self,
        status: ExperimentStatus | None = None,
        created_by: str | None = None
    ) -> list[ABTestExperiment]:
        """List experiments with optional filtering."""
        experiments = list(self.experiments.values())

        if status:
            experiments = [e for e in experiments if e.status == status]

        if created_by:
            experiments = [e for e in experiments if e.created_by == created_by]

        return experiments

    def _validate_experiment(self, experiment: ABTestExperiment) -> None:
        """Validate experiment configuration."""
        # Check traffic allocation sums to 100%
        total_allocation = sum(v.traffic_allocation for v in experiment.variants)
        if abs(total_allocation - 100.0) > 0.01:
            raise ValueError(f"Traffic allocation must sum to 100%, got {total_allocation}")

        # Ensure exactly one control variant
        control_variants = [v for v in experiment.variants if v.is_control]
        if len(control_variants) != 1:
            raise ValueError("Exactly one variant must be marked as control")

        # Check for duplicate variant names
        variant_names = [v.name for v in experiment.variants]
        if len(set(variant_names)) != len(variant_names):
            raise ValueError("Variant names must be unique")

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
            traffic_allocation=traffic_allocation
        )

        # Create or update feature flag
        flag = self.feature_flag_manager.get_flag(experiment.feature_flag_key)
        if flag:
            # Update existing flag
            self.feature_flag_manager.update_flag(
                experiment.feature_flag_key,
                {"ab_test": ab_test_config.model_dump()}
            )
        else:
            # Create new flag
            flag = self.feature_flag_manager.create_flag(
                key=experiment.feature_flag_key,
                name=f"AB Test: {experiment.name}",
                description=experiment.description,
                flag_type=FeatureFlagType.JSON,
                default_value=next(v.feature_value for v in experiment.variants if v.is_control),
                created_by=experiment.created_by
            )
            flag.ab_test = ab_test_config
            self.feature_flag_manager.register_flag(flag)

    def _is_user_eligible(
        self,
        experiment: ABTestExperiment,
        user_id: str,
        user_attributes: dict[str, Any]
    ) -> bool:
        """Check if user is eligible for experiment."""
        # Check target audience rules
        for key, value in experiment.target_audience.items():
            if key == "user_id" and user_id != value:
                return False
            elif key in user_attributes and user_attributes[key] != value:
                return False

        # Check exclusion rules
        for key, value in experiment.exclusion_rules.items():
            if key == "user_id" and user_id == value:
                return False
            elif key in user_attributes and user_attributes[key] == value:
                return False

        return True

    def _assign_variant(
        self,
        experiment: ABTestExperiment,
        user_id: str
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

    def _get_user_assignment(
        self,
        experiment_id: str,
        user_id: str
    ) -> ExperimentAssignment | None:
        """Get existing user assignment for experiment."""
        assignments = self.assignments.get(experiment_id, [])
        for assignment in assignments:
            if assignment.user_id == user_id:
                return assignment
        return None


# Global A/B testing engine instance
ab_testing_engine = ABTestingEngine()
