"""A/B testing framework for model deployments."""

import hashlib
import random
from datetime import datetime, timedelta, timezone
from enum import Enum
from typing import Any

import structlog
from pydantic import BaseModel, Field

from ..observability.metrics import MLOpsMetrics
from .config import BaseConfig
from .registry import ModelRegistry

logger = structlog.get_logger(__name__)


class ExperimentStatus(str, Enum):
    """A/B experiment status."""

    DRAFT = "draft"
    ACTIVE = "active"
    PAUSED = "paused"
    COMPLETED = "completed"
    TERMINATED = "terminated"


class TrafficSplitStrategy(str, Enum):
    """Traffic splitting strategies."""

    RANDOM = "random"
    HASH_BASED = "hash_based"
    WEIGHTED_RANDOM = "weighted_random"
    GRADUAL_ROLLOUT = "gradual_rollout"


class ABTestingConfig(BaseConfig):
    """Configuration for A/B testing."""

    # Default experiment settings
    default_experiment_duration_days: int = Field(
        default=14, description="Default experiment duration in days"
    )
    min_sample_size: int = Field(
        default=1000, description="Minimum sample size per variant"
    )
    significance_level: float = Field(
        default=0.05, description="Statistical significance level"
    )

    # Traffic splitting
    default_traffic_split_strategy: TrafficSplitStrategy = Field(
        default=TrafficSplitStrategy.HASH_BASED,
        description="Default traffic split strategy",
    )
    max_variants: int = Field(
        default=5, description="Maximum number of variants per experiment"
    )

    # Safety settings
    enable_automatic_stop: bool = Field(
        default=True, description="Enable automatic experiment stopping"
    )
    performance_degradation_threshold: float = Field(
        default=0.05, description="Threshold for performance degradation"
    )

    # Monitoring
    monitoring_interval_minutes: int = Field(
        default=60, description="Interval for monitoring experiments"
    )
    alert_on_significant_difference: bool = Field(
        default=True, description="Alert when significant difference is detected"
    )


class ModelVariant(BaseModel):
    """Model variant in an A/B experiment."""

    variant_id: str = Field(description="Unique variant identifier")
    model_name: str = Field(description="Model name")
    model_version: str = Field(description="Model version")
    traffic_percentage: float = Field(
        description="Percentage of traffic to route to this variant"
    )

    # Variant metadata
    description: str | None = Field(default=None, description="Variant description")
    created_by: str | None = Field(default=None, description="User who created variant")
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))

    # Configuration overrides
    config_overrides: dict[str, Any] = Field(
        default_factory=dict, description="Configuration overrides for this variant"
    )

    # Tags and labels
    tags: dict[str, str] = Field(default_factory=dict, description="Variant tags")


class ABExperiment(BaseModel):
    """A/B testing experiment."""

    experiment_id: str = Field(description="Unique experiment identifier")
    name: str = Field(description="Experiment name")
    description: str | None = Field(default=None, description="Experiment description")

    # Experiment configuration
    variants: list[ModelVariant] = Field(description="Experiment variants")
    control_variant_id: str = Field(description="Control variant identifier")
    traffic_split_strategy: TrafficSplitStrategy = Field(
        description="Traffic splitting strategy"
    )

    # Success metrics
    primary_metric: str = Field(description="Primary success metric")
    secondary_metrics: list[str] = Field(
        default_factory=list, description="Secondary metrics"
    )
    success_criteria: dict[str, Any] = Field(
        default_factory=dict, description="Success criteria configuration"
    )

    # Experiment lifecycle
    status: ExperimentStatus = Field(default=ExperimentStatus.DRAFT)
    start_time: datetime | None = Field(
        default=None, description="Experiment start time"
    )
    end_time: datetime | None = Field(default=None, description="Experiment end time")
    planned_duration_days: int = Field(description="Planned experiment duration")

    # Targeting and filtering
    audience_filter: dict[str, Any] = Field(
        default_factory=dict, description="Audience filtering rules"
    )
    feature_flags: dict[str, bool] = Field(
        default_factory=dict, description="Feature flags for experiment"
    )

    # Metadata
    created_by: str | None = Field(
        default=None, description="User who created experiment"
    )
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    updated_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))

    # Tags
    tags: dict[str, str] = Field(default_factory=dict, description="Experiment tags")


class ExperimentResult(BaseModel):
    """Results of an A/B experiment."""

    experiment_id: str = Field(description="Experiment identifier")
    variant_results: dict[str, dict[str, float]] = Field(
        description="Results by variant and metric"
    )

    # Statistical analysis
    statistical_significance: dict[str, dict[str, float]] = Field(
        default_factory=dict,
        description="Statistical significance by metric and variant comparison",
    )
    confidence_intervals: dict[str, dict[str, tuple[float, float]]] = Field(
        default_factory=dict, description="Confidence intervals by variant and metric"
    )

    # Winner determination
    winning_variant_id: str | None = Field(default=None, description="Winning variant")
    win_probability: float | None = Field(
        default=None, description="Probability that winner is correct"
    )

    # Sample sizes
    sample_sizes: dict[str, int] = Field(description="Sample sizes by variant")

    # Analysis metadata
    analysis_timestamp: datetime = Field(
        default_factory=lambda: datetime.now(timezone.utc)
    )
    analysis_method: str = Field(description="Statistical analysis method used")
    is_conclusive: bool = Field(description="Whether results are conclusive")

    # Recommendations
    recommendation: str | None = Field(
        default=None, description="Analysis recommendation"
    )
    next_steps: list[str] = Field(
        default_factory=list, description="Recommended next steps"
    )


class ABTestingManager:
    """A/B testing manager for model deployments."""

    def __init__(
        self,
        config: ABTestingConfig | None = None,
        model_registry: ModelRegistry | None = None,
    ):
        """Initialize A/B testing manager."""
        self.config = config or ABTestingConfig()
        self.model_registry = model_registry or ModelRegistry()
        self.metrics = MLOpsMetrics()

        # Experiment storage (in production, use persistent storage)
        self.experiments: dict[str, ABExperiment] = {}
        self.experiment_results: dict[str, ExperimentResult] = {}

        # Traffic assignment cache
        self._traffic_assignment_cache: dict[str, str] = {}

        logger.info(
            "A/B testing manager initialized",
            default_duration_days=self.config.default_experiment_duration_days,
            min_sample_size=self.config.min_sample_size,
        )

    def create_experiment(
        self,
        name: str,
        control_model_name: str,
        control_model_version: str,
        treatment_model_name: str,
        treatment_model_version: str,
        primary_metric: str,
        traffic_split: float = 0.5,
        description: str | None = None,
        secondary_metrics: list[str] | None = None,
        planned_duration_days: int | None = None,
        created_by: str | None = None,
        tags: dict[str, str] | None = None,
    ) -> ABExperiment:
        """Create a new A/B experiment."""
        try:
            # Generate experiment ID
            experiment_id = self._generate_experiment_id(name)

            # Validate models exist
            self._validate_model_exists(control_model_name, control_model_version)
            self._validate_model_exists(treatment_model_name, treatment_model_version)

            # Create variants
            control_variant = ModelVariant(
                variant_id="control",
                model_name=control_model_name,
                model_version=control_model_version,
                traffic_percentage=(1 - traffic_split) * 100,
                description="Control variant",
                created_by=created_by,
                tags={"type": "control"},
            )

            treatment_variant = ModelVariant(
                variant_id="treatment",
                model_name=treatment_model_name,
                model_version=treatment_model_version,
                traffic_percentage=traffic_split * 100,
                description="Treatment variant",
                created_by=created_by,
                tags={"type": "treatment"},
            )

            # Create experiment
            experiment = ABExperiment(
                experiment_id=experiment_id,
                name=name,
                description=description,
                variants=[control_variant, treatment_variant],
                control_variant_id="control",
                traffic_split_strategy=self.config.default_traffic_split_strategy,
                primary_metric=primary_metric,
                secondary_metrics=secondary_metrics or [],
                planned_duration_days=planned_duration_days
                or self.config.default_experiment_duration_days,
                created_by=created_by,
                tags=tags or {},
            )

            # Store experiment
            self.experiments[experiment_id] = experiment

            # Record metrics (placeholder - extend MLOpsMetrics as needed)
            # TODO: Add proper metrics tracking for A/B experiment creation

            logger.info(
                "A/B experiment created",
                experiment_id=experiment_id,
                name=name,
                control_model=f"{control_model_name}:{control_model_version}",
                treatment_model=f"{treatment_model_name}:{treatment_model_version}",
                traffic_split=traffic_split,
            )

            return experiment

        except Exception as error:
            logger.error(
                "Failed to create A/B experiment",
                name=name,
                error=str(error),
            )
            raise

    def _generate_experiment_id(self, name: str) -> str:
        """Generate unique experiment ID."""
        timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        name_hash = hashlib.md5(name.encode()).hexdigest()[:8]
        return f"exp_{timestamp}_{name_hash}"

    def _validate_model_exists(self, model_name: str, model_version: str) -> None:
        """Validate that a model version exists."""
        try:
            self.model_registry.get_model_metadata(model_name, model_version)
        except Exception as e:
            raise ValueError(f"Model {model_name}:{model_version} not found: {e}")

    def start_experiment(self, experiment_id: str) -> None:
        """Start an A/B experiment."""
        try:
            if experiment_id not in self.experiments:
                raise ValueError(f"Experiment {experiment_id} not found")

            experiment = self.experiments[experiment_id]

            if experiment.status != ExperimentStatus.DRAFT:
                raise ValueError(
                    f"Can only start experiments in DRAFT status, current: {experiment.status}"
                )

            # Validate experiment configuration
            self._validate_experiment_config(experiment)

            # Start experiment
            experiment.status = ExperimentStatus.ACTIVE
            experiment.start_time = datetime.now(timezone.utc)
            experiment.end_time = experiment.start_time + timedelta(
                days=experiment.planned_duration_days
            )
            experiment.updated_at = datetime.now(timezone.utc)

            # Record metrics (placeholder - extend MLOpsMetrics as needed)
            # TODO: Add proper metrics tracking for A/B experiment start

            logger.info(
                "A/B experiment started",
                experiment_id=experiment_id,
                name=experiment.name,
                planned_end_time=experiment.end_time.isoformat(),
            )

        except Exception as error:
            logger.error(
                "Failed to start A/B experiment",
                experiment_id=experiment_id,
                error=str(error),
            )
            raise

    def _validate_experiment_config(self, experiment: ABExperiment) -> None:
        """Validate experiment configuration."""
        # Check traffic percentages sum to 100
        total_traffic = sum(
            variant.traffic_percentage for variant in experiment.variants
        )
        if abs(total_traffic - 100.0) > 0.01:
            raise ValueError(
                f"Traffic percentages must sum to 100%, got {total_traffic}%"
            )

        # Check control variant exists
        control_variant_exists = any(
            variant.variant_id == experiment.control_variant_id
            for variant in experiment.variants
        )
        if not control_variant_exists:
            raise ValueError(
                f"Control variant {experiment.control_variant_id} not found"
            )

        # Check maximum variants
        if len(experiment.variants) > self.config.max_variants:
            raise ValueError(
                f"Too many variants: {len(experiment.variants)}, max: {self.config.max_variants}"
            )

    def assign_variant(
        self,
        experiment_id: str,
        user_id: str,
        context: dict[str, Any] | None = None,
    ) -> str:
        """Assign a user to a variant."""
        try:
            if experiment_id not in self.experiments:
                raise ValueError(f"Experiment {experiment_id} not found")

            experiment = self.experiments[experiment_id]

            if experiment.status != ExperimentStatus.ACTIVE:
                # Return control variant if experiment is not active
                return experiment.control_variant_id

            # Check cache first
            cache_key = f"{experiment_id}:{user_id}"
            if cache_key in self._traffic_assignment_cache:
                return self._traffic_assignment_cache[cache_key]

            # Apply audience filter if configured
            if experiment.audience_filter and not self._passes_audience_filter(
                user_id, experiment.audience_filter, context
            ):
                return experiment.control_variant_id

            # Assign variant based on strategy
            variant_id = self._assign_variant_by_strategy(experiment, user_id, context)

            # Cache assignment
            self._traffic_assignment_cache[cache_key] = variant_id

            return variant_id

        except Exception as error:
            logger.error(
                "Failed to assign variant",
                experiment_id=experiment_id,
                user_id=user_id,
                error=str(error),
            )
            # Return control variant on error
            return self.experiments.get(
                experiment_id,
                ABExperiment(
                    experiment_id="",
                    name="",
                    variants=[],
                    control_variant_id="control",
                    traffic_split_strategy=TrafficSplitStrategy.RANDOM,
                    primary_metric="",
                    planned_duration_days=1,
                ),
            ).control_variant_id

    def _passes_audience_filter(
        self,
        user_id: str,
        audience_filter: dict[str, Any],
        context: dict[str, Any] | None,
    ) -> bool:
        """Check if user passes audience filter."""
        try:
            # Simple audience filtering logic (extend as needed)
            if "user_segments" in audience_filter and context:
                required_segments = audience_filter["user_segments"]
                user_segments = context.get("user_segments", [])

                if not any(segment in user_segments for segment in required_segments):
                    return False

            if "percentage" in audience_filter:
                # Hash-based percentage filtering
                user_hash = int(hashlib.md5(user_id.encode()).hexdigest(), 16)
                user_percentage = (user_hash % 100) + 1

                if user_percentage > audience_filter["percentage"]:
                    return False

            return True

        except Exception as error:
            logger.warning(
                "Error applying audience filter",
                user_id=user_id,
                error=str(error),
            )
            return True  # Default to including user

    def _assign_variant_by_strategy(
        self,
        experiment: ABExperiment,
        user_id: str,
        context: dict[str, Any] | None,
    ) -> str:
        """Assign variant based on traffic split strategy."""
        if experiment.traffic_split_strategy == TrafficSplitStrategy.HASH_BASED:
            return self._hash_based_assignment(experiment, user_id)

        elif experiment.traffic_split_strategy == TrafficSplitStrategy.RANDOM:
            return self._random_assignment(experiment)

        elif experiment.traffic_split_strategy == TrafficSplitStrategy.WEIGHTED_RANDOM:
            return self._weighted_random_assignment(experiment)

        elif experiment.traffic_split_strategy == TrafficSplitStrategy.GRADUAL_ROLLOUT:
            return self._gradual_rollout_assignment(experiment, user_id)

        else:
            logger.warning(
                "Unknown traffic split strategy, using hash-based",
                strategy=experiment.traffic_split_strategy,
            )
            return self._hash_based_assignment(experiment, user_id)

    def _hash_based_assignment(self, experiment: ABExperiment, user_id: str) -> str:
        """Assign variant using hash-based deterministic assignment."""
        # Create deterministic hash
        hash_input = f"{experiment.experiment_id}:{user_id}"
        user_hash = int(hashlib.md5(hash_input.encode()).hexdigest(), 16)

        # Convert to percentage (0-100)
        user_percentage = user_hash % 100

        # Assign based on cumulative traffic percentages
        cumulative_percentage = 0
        for variant in experiment.variants:
            cumulative_percentage += variant.traffic_percentage
            if user_percentage < cumulative_percentage:
                return variant.variant_id

        # Fallback to control
        return experiment.control_variant_id

    def _random_assignment(self, experiment: ABExperiment) -> str:
        """Assign variant using random assignment."""
        rand_value = random.random() * 100

        cumulative_percentage = 0
        for variant in experiment.variants:
            cumulative_percentage += variant.traffic_percentage
            if rand_value < cumulative_percentage:
                return variant.variant_id

        return experiment.control_variant_id

    def _weighted_random_assignment(self, experiment: ABExperiment) -> str:
        """Assign variant using weighted random assignment."""
        weights = [variant.traffic_percentage for variant in experiment.variants]
        chosen_variant = random.choices(experiment.variants, weights=weights)[0]
        return chosen_variant.variant_id

    def _gradual_rollout_assignment(
        self, experiment: ABExperiment, user_id: str
    ) -> str:
        """Assign variant using gradual rollout strategy."""
        # Calculate rollout progress (0-1)
        if not experiment.start_time:
            return experiment.control_variant_id

        elapsed_time = datetime.now(timezone.utc) - experiment.start_time
        total_duration = timedelta(days=experiment.planned_duration_days)
        rollout_progress = min(
            elapsed_time.total_seconds() / total_duration.total_seconds(), 1.0
        )

        # Adjust traffic percentages based on rollout progress
        adjusted_experiment = ABExperiment(**experiment.dict())

        for variant in adjusted_experiment.variants:
            if variant.variant_id != experiment.control_variant_id:
                # Gradually increase treatment traffic
                original_percentage = variant.traffic_percentage
                adjusted_percentage = original_percentage * rollout_progress
                variant.traffic_percentage = adjusted_percentage

        # Recalculate control percentage
        treatment_total = sum(
            v.traffic_percentage
            for v in adjusted_experiment.variants
            if v.variant_id != experiment.control_variant_id
        )

        for variant in adjusted_experiment.variants:
            if variant.variant_id == experiment.control_variant_id:
                variant.traffic_percentage = 100 - treatment_total
                break

        return self._hash_based_assignment(adjusted_experiment, user_id)

    def get_variant_model(self, experiment_id: str, variant_id: str) -> tuple[str, str]:
        """Get model name and version for a variant."""
        if experiment_id not in self.experiments:
            raise ValueError(f"Experiment {experiment_id} not found")

        experiment = self.experiments[experiment_id]

        for variant in experiment.variants:
            if variant.variant_id == variant_id:
                return variant.model_name, variant.model_version

        raise ValueError(
            f"Variant {variant_id} not found in experiment {experiment_id}"
        )

    def record_experiment_metric(
        self,
        experiment_id: str,
        variant_id: str,
        user_id: str,
        metric_name: str,
        metric_value: float,
        timestamp: datetime | None = None,
    ) -> None:
        """Record a metric value for an experiment."""
        try:
            if experiment_id not in self.experiments:
                logger.warning(
                    f"Recording metric for unknown experiment: {experiment_id}"
                )
                return

            # Record metric (placeholder - extend MLOpsMetrics as needed)
            # TODO: Add proper metrics tracking for A/B experiment metrics

            logger.debug(
                "Experiment metric recorded",
                experiment_id=experiment_id,
                variant_id=variant_id,
                metric_name=metric_name,
                metric_value=metric_value,
            )

        except Exception as error:
            logger.error(
                "Failed to record experiment metric",
                experiment_id=experiment_id,
                variant_id=variant_id,
                metric_name=metric_name,
                error=str(error),
            )

    def analyze_experiment(self, experiment_id: str) -> ExperimentResult:
        """Analyze experiment results."""
        try:
            if experiment_id not in self.experiments:
                raise ValueError(f"Experiment {experiment_id} not found")

            experiment = self.experiments[experiment_id]

            # Get experiment metrics (placeholder - in production, query from time-series DB)
            variant_results = self._get_experiment_metrics(experiment_id)

            # Calculate statistical significance
            statistical_significance = self._calculate_statistical_significance(
                variant_results, experiment.primary_metric
            )

            # Calculate confidence intervals
            confidence_intervals = self._calculate_confidence_intervals(variant_results)

            # Determine winner
            winning_variant_id, win_probability = self._determine_winner(
                variant_results, experiment.primary_metric, statistical_significance
            )

            # Get sample sizes
            sample_sizes = self._get_sample_sizes(experiment_id)

            # Check if results are conclusive
            is_conclusive = self._is_result_conclusive(
                sample_sizes, statistical_significance, experiment.primary_metric
            )

            # Generate recommendation
            recommendation, next_steps = self._generate_recommendation(
                experiment, winning_variant_id, is_conclusive, statistical_significance
            )

            result = ExperimentResult(
                experiment_id=experiment_id,
                variant_results=variant_results,
                statistical_significance=statistical_significance,
                confidence_intervals=confidence_intervals,
                winning_variant_id=winning_variant_id,
                win_probability=win_probability,
                sample_sizes=sample_sizes,
                analysis_method="t_test",
                is_conclusive=is_conclusive,
                recommendation=recommendation,
                next_steps=next_steps,
            )

            # Store result
            self.experiment_results[experiment_id] = result

            logger.info(
                "Experiment analysis completed",
                experiment_id=experiment_id,
                winning_variant=winning_variant_id,
                is_conclusive=is_conclusive,
                win_probability=win_probability,
            )

            return result

        except Exception as error:
            logger.error(
                "Failed to analyze experiment",
                experiment_id=experiment_id,
                error=str(error),
            )
            raise

    def _get_experiment_metrics(
        self, experiment_id: str
    ) -> dict[str, dict[str, float]]:
        """Get experiment metrics (placeholder implementation)."""
        # In production, query from time-series database
        # For now, return mock data
        return {
            "control": {
                "accuracy": 0.85,
                "precision": 0.82,
                "recall": 0.88,
                "f1_score": 0.85,
                "latency_ms": 120.5,
            },
            "treatment": {
                "accuracy": 0.87,
                "precision": 0.84,
                "recall": 0.90,
                "f1_score": 0.87,
                "latency_ms": 115.2,
            },
        }

    def _calculate_statistical_significance(
        self, variant_results: dict[str, dict[str, float]], primary_metric: str
    ) -> dict[str, dict[str, float]]:
        """Calculate statistical significance between variants."""
        significance = {}

        # Simple t-test implementation (placeholder)
        # In production, use proper statistical libraries
        control_value = variant_results.get("control", {}).get(primary_metric, 0)

        for variant_id, metrics in variant_results.items():
            if variant_id == "control":
                continue

            treatment_value = metrics.get(primary_metric, 0)

            # Mock p-value calculation
            difference = abs(treatment_value - control_value)
            mock_p_value = max(0.001, 0.1 - difference * 2)  # Simplified calculation

            significance[f"control_vs_{variant_id}"] = {
                "p_value": mock_p_value,
                "significant": mock_p_value < self.config.significance_level,
                "effect_size": (treatment_value - control_value) / control_value
                if control_value != 0
                else 0,
            }

        return significance

    def _calculate_confidence_intervals(
        self, variant_results: dict[str, dict[str, float]]
    ) -> dict[str, dict[str, tuple[float, float]]]:
        """Calculate confidence intervals for metrics."""
        confidence_intervals: dict[str, dict[str, tuple[float, float]]] = {}

        for variant_id, metrics in variant_results.items():
            confidence_intervals[variant_id] = {}

            for metric_name, value in metrics.items():
                # Mock confidence interval (placeholder)
                margin_of_error = value * 0.05  # 5% margin
                lower_bound = value - margin_of_error
                upper_bound = value + margin_of_error

                confidence_intervals[variant_id][metric_name] = (
                    lower_bound,
                    upper_bound,
                )

        return confidence_intervals

    def _determine_winner(
        self,
        variant_results: dict[str, dict[str, float]],
        primary_metric: str,
        statistical_significance: dict[str, dict[str, float]],
    ) -> tuple[str | None, float | None]:
        """Determine winning variant."""
        control_value = variant_results.get("control", {}).get(primary_metric, 0)

        best_variant = "control"
        best_value = control_value

        for variant_id, metrics in variant_results.items():
            if variant_id == "control":
                continue

            value = metrics.get(primary_metric, 0)

            # Check if statistically significant and better
            sig_key = f"control_vs_{variant_id}"
            if (
                sig_key in statistical_significance
                and statistical_significance[sig_key]["significant"]
                and value > best_value
            ):
                best_variant = variant_id
                best_value = value

        # Calculate win probability (simplified)
        if best_variant == "control":
            win_probability = 0.6  # Default confidence for control
        else:
            sig_key = f"control_vs_{best_variant}"
            p_value = statistical_significance[sig_key]["p_value"]
            win_probability = 1 - p_value  # Simplified calculation

        return best_variant, win_probability

    def _get_sample_sizes(self, experiment_id: str) -> dict[str, int]:
        """Get sample sizes for each variant."""
        # Mock sample sizes (in production, query from database)
        return {
            "control": 2500,
            "treatment": 2400,
        }

    def _is_result_conclusive(
        self,
        sample_sizes: dict[str, int],
        statistical_significance: dict[str, dict[str, float]],
        primary_metric: str,
    ) -> bool:
        """Check if experiment results are conclusive."""
        # Check minimum sample size
        min_samples_met = all(
            size >= self.config.min_sample_size for size in sample_sizes.values()
        )

        # Check if any variant shows significant difference
        has_significant_result = any(
            result["significant"] for result in statistical_significance.values()
        )

        return min_samples_met and has_significant_result

    def _generate_recommendation(
        self,
        experiment: ABExperiment,
        winning_variant_id: str | None,
        is_conclusive: bool,
        statistical_significance: dict[str, dict[str, float]],
    ) -> tuple[str, list[str]]:
        """Generate analysis recommendation and next steps."""
        if not is_conclusive:
            recommendation = (
                "Continue experiment - insufficient data for conclusive results"
            )
            next_steps = [
                "Continue running the experiment",
                "Monitor for sufficient sample size",
                "Check back in 3-5 days",
            ]
        elif winning_variant_id == experiment.control_variant_id:
            recommendation = "No significant improvement found - keep current model"
            next_steps = [
                "Stop the experiment",
                "Keep using the control model",
                "Consider trying different model variations",
            ]
        else:
            recommendation = f"Deploy winning variant: {winning_variant_id}"
            next_steps = [
                f"Gradually roll out {winning_variant_id} to 100% of traffic",
                "Monitor performance metrics closely",
                "Prepare rollback plan if needed",
                "Stop the experiment after successful deployment",
            ]

        return recommendation, next_steps

    def stop_experiment(self, experiment_id: str, reason: str = "Manual stop") -> None:
        """Stop an active experiment."""
        try:
            if experiment_id not in self.experiments:
                raise ValueError(f"Experiment {experiment_id} not found")

            experiment = self.experiments[experiment_id]

            if experiment.status != ExperimentStatus.ACTIVE:
                raise ValueError(
                    f"Can only stop active experiments, current status: {experiment.status}"
                )

            # Stop experiment
            experiment.status = ExperimentStatus.COMPLETED
            experiment.end_time = datetime.now(timezone.utc)
            experiment.updated_at = datetime.now(timezone.utc)

            # Clear traffic assignment cache for this experiment
            cache_keys_to_remove = [
                key
                for key in self._traffic_assignment_cache.keys()
                if key.startswith(f"{experiment_id}:")
            ]
            for key in cache_keys_to_remove:
                del self._traffic_assignment_cache[key]

            # Record metrics (placeholder - extend MLOpsMetrics as needed)
            # TODO: Add proper metrics tracking for A/B experiment stop

            logger.info(
                "A/B experiment stopped",
                experiment_id=experiment_id,
                name=experiment.name,
                reason=reason,
            )

        except Exception as error:
            logger.error(
                "Failed to stop A/B experiment",
                experiment_id=experiment_id,
                error=str(error),
            )
            raise

    def get_experiment(self, experiment_id: str) -> ABExperiment | None:
        """Get experiment by ID."""
        return self.experiments.get(experiment_id)

    def list_experiments(
        self, status: ExperimentStatus | None = None
    ) -> list[ABExperiment]:
        """List experiments, optionally filtered by status."""
        experiments = list(self.experiments.values())

        if status:
            experiments = [exp for exp in experiments if exp.status == status]

        return sorted(experiments, key=lambda x: x.created_at, reverse=True)

    def get_experiment_result(self, experiment_id: str) -> ExperimentResult | None:
        """Get experiment result by ID."""
        return self.experiment_results.get(experiment_id)
