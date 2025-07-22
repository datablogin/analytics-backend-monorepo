"""Model monitoring and drift detection for production ML systems."""

from datetime import datetime, timezone
from typing import Any

import numpy as np
import pandas as pd
import structlog
from pydantic import BaseModel, Field
from scipy import stats

from ..observability.metrics import MLOpsMetrics
from .config import BaseConfig

logger = structlog.get_logger(__name__)


class ModelMonitoringConfig(BaseConfig):
    """Configuration for model monitoring."""

    # Drift detection settings
    drift_detection_method: str = Field(
        default="ks_test",
        description="Method for drift detection (ks_test, psi, evidently)",
    )
    drift_threshold: float = Field(
        default=0.05, description="P-value threshold for drift detection"
    )
    min_samples_for_drift: int = Field(
        default=100, description="Minimum samples required for drift detection"
    )

    # Performance monitoring
    performance_threshold: float = Field(
        default=0.05, description="Performance degradation threshold"
    )
    min_samples_for_performance: int = Field(
        default=50, description="Minimum samples for performance monitoring"
    )

    # Data quality monitoring
    missing_value_threshold: float = Field(
        default=0.1, description="Threshold for missing value alerts"
    )
    outlier_threshold: float = Field(
        default=0.05, description="Threshold for outlier detection"
    )

    # Monitoring intervals
    monitoring_interval_minutes: int = Field(
        default=60, description="Interval for monitoring checks"
    )
    reporting_interval_hours: int = Field(
        default=24, description="Interval for generating reports"
    )

    # Storage settings
    store_monitoring_data: bool = Field(
        default=True, description="Store monitoring data for analysis"
    )
    monitoring_data_retention_days: int = Field(
        default=90, description="Days to retain monitoring data"
    )


class DriftDetectionResult(BaseModel):
    """Result of drift detection analysis."""

    # Detection metadata
    model_name: str = Field(description="Model name")
    feature_name: str | None = Field(
        default=None, description="Feature name for feature-level drift"
    )
    detection_method: str = Field(description="Drift detection method used")
    timestamp: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))

    # Statistical test results
    statistic: float = Field(description="Test statistic value")
    p_value: float = Field(description="P-value from statistical test")
    threshold: float = Field(description="Threshold used for detection")
    drift_detected: bool = Field(description="Whether drift was detected")

    # Sample information
    reference_samples: int = Field(description="Number of reference samples")
    current_samples: int = Field(description="Number of current samples")

    # Additional metrics
    drift_magnitude: float | None = Field(
        default=None, description="Magnitude of drift (0-1 scale)"
    )
    drift_score: float | None = Field(default=None, description="Overall drift score")

    # Recommendations
    severity: str = Field(
        default="low", description="Drift severity (low, medium, high, critical)"
    )
    recommended_action: str | None = Field(
        default=None, description="Recommended action to take"
    )


class PerformanceMonitoringResult(BaseModel):
    """Result of performance monitoring analysis."""

    # Monitoring metadata
    model_name: str = Field(description="Model name")
    metric_name: str = Field(description="Performance metric name")
    timestamp: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))

    # Performance metrics
    current_value: float = Field(description="Current metric value")
    baseline_value: float = Field(description="Baseline metric value")
    change_percentage: float = Field(description="Percentage change from baseline")

    # Statistical significance
    p_value: float | None = Field(
        default=None, description="P-value for significance test"
    )
    confidence_interval: tuple[float, float] | None = Field(
        default=None, description="Confidence interval for current value"
    )

    # Sample information
    current_samples: int = Field(description="Number of current samples")
    baseline_samples: int = Field(description="Number of baseline samples")

    # Alert status
    performance_degraded: bool = Field(description="Whether performance has degraded")
    threshold: float = Field(description="Threshold used for detection")
    severity: str = Field(default="low", description="Alert severity")


class DataQualityResult(BaseModel):
    """Result of data quality monitoring."""

    # Metadata
    model_name: str = Field(description="Model name")
    timestamp: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))

    # Quality metrics
    missing_values: dict[str, float] = Field(
        default_factory=dict, description="Missing value percentages by feature"
    )
    outlier_percentages: dict[str, float] = Field(
        default_factory=dict, description="Outlier percentages by feature"
    )
    data_types: dict[str, str] = Field(
        default_factory=dict, description="Data types by feature"
    )

    # Statistical summary
    feature_statistics: dict[str, dict[str, float]] = Field(
        default_factory=dict, description="Statistical summary by feature"
    )

    # Quality flags
    quality_issues: list[str] = Field(
        default_factory=list, description="List of detected quality issues"
    )
    overall_score: float = Field(description="Overall data quality score (0-1)")

    # Sample information
    total_samples: int = Field(description="Total number of samples analyzed")


class ModelMonitor:
    """Comprehensive model monitoring system."""

    def __init__(self, config: ModelMonitoringConfig | None = None):
        """Initialize model monitor."""
        self.config = config or ModelMonitoringConfig()
        self.metrics = MLOpsMetrics()

        # Reference data storage (in production, use persistent storage)
        self.reference_data: dict[str, pd.DataFrame] = {}
        self.baseline_performance: dict[str, dict[str, float]] = {}

        logger.info(
            "Model monitor initialized",
            drift_method=self.config.drift_detection_method,
            drift_threshold=self.config.drift_threshold,
        )

    def set_reference_data(self, model_name: str, reference_data: pd.DataFrame) -> None:
        """Set reference data for drift detection."""
        try:
            self.reference_data[model_name] = reference_data.copy()

            logger.info(
                "Reference data set",
                model_name=model_name,
                samples=len(reference_data),
                features=len(reference_data.columns),
            )

        except Exception as error:
            logger.error(
                "Failed to set reference data",
                model_name=model_name,
                error=str(error),
            )
            raise

    def set_baseline_performance(
        self, model_name: str, performance_metrics: dict[str, float]
    ) -> None:
        """Set baseline performance metrics."""
        try:
            self.baseline_performance[model_name] = performance_metrics.copy()

            logger.info(
                "Baseline performance set",
                model_name=model_name,
                metrics=list(performance_metrics.keys()),
            )

        except Exception as error:
            logger.error(
                "Failed to set baseline performance",
                model_name=model_name,
                error=str(error),
            )
            raise

    def detect_data_drift(
        self,
        model_name: str,
        current_data: pd.DataFrame,
        feature_subset: list[str] | None = None,
    ) -> list[DriftDetectionResult]:
        """Detect data drift for model inputs."""
        try:
            if model_name not in self.reference_data:
                raise ValueError(f"No reference data found for model {model_name}")

            reference_data = self.reference_data[model_name]

            # Validate sample sizes
            if len(current_data) < self.config.min_samples_for_drift:
                logger.warning(
                    "Insufficient samples for drift detection",
                    model_name=model_name,
                    current_samples=len(current_data),
                    required=self.config.min_samples_for_drift,
                )
                return []

            # Determine features to check
            if feature_subset:
                features = [f for f in feature_subset if f in current_data.columns]
            else:
                features = list(current_data.columns)

            # Align features between reference and current data
            common_features = list(set(features) & set(reference_data.columns))

            if not common_features:
                logger.warning(
                    "No common features found",
                    model_name=model_name,
                    current_features=list(current_data.columns),
                    reference_features=list(reference_data.columns),
                )
                return []

            # Detect drift for each feature
            drift_results = []

            for feature in common_features:
                try:
                    drift_result = self._detect_feature_drift(
                        model_name=model_name,
                        feature_name=feature,
                        reference_values=reference_data[feature].dropna(),
                        current_values=current_data[feature].dropna(),
                    )

                    if drift_result:
                        drift_results.append(drift_result)

                        # Record metrics
                        self.metrics.record_drift_detection(
                            model_name=model_name,
                            feature_name=feature,
                            drift_detected=drift_result.drift_detected,
                            drift_magnitude=drift_result.drift_magnitude or 0,
                            p_value=drift_result.p_value,
                        )

                except Exception as feature_error:
                    logger.warning(
                        "Failed to detect drift for feature",
                        model_name=model_name,
                        feature=feature,
                        error=str(feature_error),
                    )
                    continue

            # Overall drift assessment
            drift_detected_count = sum(1 for r in drift_results if r.drift_detected)

            logger.info(
                "Data drift detection completed",
                model_name=model_name,
                features_checked=len(common_features),
                drift_detected_features=drift_detected_count,
                drift_percentage=drift_detected_count / len(common_features) * 100,
            )

            return drift_results

        except Exception as error:
            logger.error(
                "Failed to detect data drift",
                model_name=model_name,
                error=str(error),
            )
            raise

    def _detect_feature_drift(
        self,
        model_name: str,
        feature_name: str,
        reference_values: pd.Series,
        current_values: pd.Series,
    ) -> DriftDetectionResult | None:
        """Detect drift for a single feature."""
        try:
            # Skip if not enough samples
            if len(reference_values) < 10 or len(current_values) < 10:
                return None

            # Select detection method
            if self.config.drift_detection_method == "ks_test":
                statistic, p_value = self._kolmogorov_smirnov_test(
                    reference_values, current_values
                )
                drift_magnitude = statistic  # KS statistic is also the magnitude

            elif self.config.drift_detection_method == "psi":
                psi_value = self._population_stability_index(
                    reference_values, current_values
                )
                statistic = psi_value
                p_value = 1.0 if psi_value < 0.1 else 0.0  # Simple threshold
                drift_magnitude = min(psi_value / 2.0, 1.0)  # Normalize to 0-1

            else:
                raise ValueError(
                    f"Unknown drift detection method: {self.config.drift_detection_method}"
                )

            # Determine drift status
            drift_detected = p_value < self.config.drift_threshold

            # Determine severity
            if drift_magnitude < 0.1:
                severity = "low"
            elif drift_magnitude < 0.25:
                severity = "medium"
            elif drift_magnitude < 0.5:
                severity = "high"
            else:
                severity = "critical"

            # Generate recommendations
            recommended_action = None
            if drift_detected:
                if severity in ["high", "critical"]:
                    recommended_action = "Immediate retraining recommended"
                elif severity == "medium":
                    recommended_action = "Monitor closely, consider retraining"
                else:
                    recommended_action = "Continue monitoring"

            return DriftDetectionResult(
                model_name=model_name,
                feature_name=feature_name,
                detection_method=self.config.drift_detection_method,
                statistic=statistic,
                p_value=p_value,
                threshold=self.config.drift_threshold,
                drift_detected=drift_detected,
                reference_samples=len(reference_values),
                current_samples=len(current_values),
                drift_magnitude=drift_magnitude,
                drift_score=drift_magnitude,
                severity=severity,
                recommended_action=recommended_action,
            )

        except Exception as error:
            logger.error(
                "Failed to detect feature drift",
                feature=feature_name,
                error=str(error),
            )
            return None

    def _kolmogorov_smirnov_test(
        self, reference_values: pd.Series, current_values: pd.Series
    ) -> tuple[float, float]:
        """Perform Kolmogorov-Smirnov test for distribution comparison."""
        try:
            # Handle categorical data
            if reference_values.dtype == "object" or current_values.dtype == "object":
                # Use chi-square test for categorical data
                ref_counts = reference_values.value_counts()
                cur_counts = current_values.value_counts()

                # Align categories
                all_categories = set(ref_counts.index) | set(cur_counts.index)
                ref_aligned = [ref_counts.get(cat, 0) for cat in all_categories]
                cur_aligned = [cur_counts.get(cat, 0) for cat in all_categories]

                statistic, p_value = stats.chisquare(cur_aligned, ref_aligned)
                return float(statistic), float(p_value)

            # Numerical data - use KS test
            statistic, p_value = stats.ks_2samp(reference_values, current_values)
            return float(statistic), float(p_value)

        except Exception as error:
            logger.error("KS test failed", error=str(error))
            # Return neutral values if test fails
            return 0.0, 1.0

    def _population_stability_index(
        self, reference_values: pd.Series, current_values: pd.Series
    ) -> float:
        """Calculate Population Stability Index (PSI)."""
        try:
            # Handle categorical data
            if reference_values.dtype == "object":
                ref_counts = reference_values.value_counts(normalize=True)
                cur_counts = current_values.value_counts(normalize=True)

                # Align categories
                all_categories = set(ref_counts.index) | set(cur_counts.index)

                psi = 0.0
                for category in all_categories:
                    ref_pct = ref_counts.get(
                        category, 0.001
                    )  # Small value to avoid log(0)
                    cur_pct = cur_counts.get(category, 0.001)
                    psi += (cur_pct - ref_pct) * np.log(cur_pct / ref_pct)

                return abs(psi)

            # Numerical data - create bins
            try:
                # Create equal-width bins based on reference data
                bins = np.histogram_bin_edges(reference_values, bins=10)
                ref_hist, _ = np.histogram(reference_values, bins=bins)
                cur_hist, _ = np.histogram(current_values, bins=bins)

                # Normalize to probabilities
                ref_probs = ref_hist / ref_hist.sum()
                cur_probs = cur_hist / cur_hist.sum()

                # Add small constant to avoid log(0)
                ref_probs = np.where(ref_probs == 0, 0.001, ref_probs)
                cur_probs = np.where(cur_probs == 0, 0.001, cur_probs)

                # Calculate PSI
                psi = np.sum((cur_probs - ref_probs) * np.log(cur_probs / ref_probs))

                return abs(psi)

            except Exception:
                # Fallback to simple comparison
                return 0.0

        except Exception as error:
            logger.error("PSI calculation failed", error=str(error))
            return 0.0

    def monitor_performance(
        self,
        model_name: str,
        y_true: np.ndarray | pd.Series,
        y_pred: np.ndarray | pd.Series,
        metric_name: str = "accuracy",
    ) -> PerformanceMonitoringResult | None:
        """Monitor model performance degradation."""
        try:
            if model_name not in self.baseline_performance:
                logger.warning(
                    "No baseline performance found",
                    model_name=model_name,
                )
                return None

            if metric_name not in self.baseline_performance[model_name]:
                logger.warning(
                    "Metric not found in baseline",
                    model_name=model_name,
                    metric=metric_name,
                )
                return None

            # Calculate current performance
            current_value = self._calculate_metric(y_true, y_pred, metric_name)

            if current_value is None:
                return None

            # Get baseline performance
            baseline_value = self.baseline_performance[model_name][metric_name]

            # Calculate change
            change_percentage = (
                (current_value - baseline_value) / baseline_value
            ) * 100

            # Determine if performance degraded
            performance_degraded = abs(change_percentage) > (
                self.config.performance_threshold * 100
            )

            # Determine severity
            if abs(change_percentage) < 5:
                severity = "low"
            elif abs(change_percentage) < 15:
                severity = "medium"
            elif abs(change_percentage) < 30:
                severity = "high"
            else:
                severity = "critical"

            # Record metrics
            self.metrics.record_performance_monitoring(
                model_name=model_name,
                metric_name=metric_name,
                current_value=current_value,
                baseline_value=baseline_value,
                change_percentage=change_percentage,
                degraded=performance_degraded,
            )

            result = PerformanceMonitoringResult(
                model_name=model_name,
                metric_name=metric_name,
                current_value=current_value,
                baseline_value=baseline_value,
                change_percentage=change_percentage,
                current_samples=len(y_true),
                baseline_samples=0,  # Would be stored in production
                performance_degraded=performance_degraded,
                threshold=self.config.performance_threshold,
                severity=severity,
            )

            logger.info(
                "Performance monitoring completed",
                model_name=model_name,
                metric=metric_name,
                current_value=current_value,
                baseline_value=baseline_value,
                change_pct=change_percentage,
                degraded=performance_degraded,
            )

            return result

        except Exception as error:
            logger.error(
                "Failed to monitor performance",
                model_name=model_name,
                metric=metric_name,
                error=str(error),
            )
            return None

    def _calculate_metric(
        self,
        y_true: np.ndarray | pd.Series,
        y_pred: np.ndarray | pd.Series,
        metric_name: str,
    ) -> float | None:
        """Calculate performance metric."""
        try:
            from sklearn.metrics import (
                accuracy_score,
                f1_score,
                mean_absolute_error,
                mean_squared_error,
                precision_score,
                r2_score,
                recall_score,
            )

            if metric_name == "accuracy":
                return accuracy_score(y_true, y_pred)
            elif metric_name == "precision":
                return precision_score(y_true, y_pred, average="weighted")
            elif metric_name == "recall":
                return recall_score(y_true, y_pred, average="weighted")
            elif metric_name == "f1":
                return f1_score(y_true, y_pred, average="weighted")
            elif metric_name == "mse":
                return mean_squared_error(y_true, y_pred)
            elif metric_name == "mae":
                return mean_absolute_error(y_true, y_pred)
            elif metric_name == "r2":
                return r2_score(y_true, y_pred)
            else:
                logger.warning("Unknown metric", metric=metric_name)
                return None

        except Exception as error:
            logger.error(
                "Failed to calculate metric", metric=metric_name, error=str(error)
            )
            return None

    def check_data_quality(
        self, model_name: str, data: pd.DataFrame
    ) -> DataQualityResult:
        """Check data quality issues."""
        try:
            # Calculate missing values
            missing_values = {}
            for column in data.columns:
                missing_pct = data[column].isnull().sum() / len(data)
                missing_values[column] = float(missing_pct)

            # Calculate outliers (simple IQR method)
            outlier_percentages = {}
            feature_statistics = {}

            for column in data.select_dtypes(include=[np.number]).columns:
                try:
                    Q1 = data[column].quantile(0.25)
                    Q3 = data[column].quantile(0.75)
                    IQR = Q3 - Q1

                    lower_bound = Q1 - 1.5 * IQR
                    upper_bound = Q3 + 1.5 * IQR

                    outliers = (
                        (data[column] < lower_bound) | (data[column] > upper_bound)
                    ).sum()
                    outlier_pct = outliers / len(data)
                    outlier_percentages[column] = float(outlier_pct)

                    # Basic statistics
                    feature_statistics[column] = {
                        "mean": float(data[column].mean()),
                        "std": float(data[column].std()),
                        "min": float(data[column].min()),
                        "max": float(data[column].max()),
                        "q25": float(Q1),
                        "q75": float(Q3),
                    }

                except Exception:
                    outlier_percentages[column] = 0.0
                    feature_statistics[column] = {}

            # Data types
            data_types = {col: str(dtype) for col, dtype in data.dtypes.items()}

            # Identify quality issues
            quality_issues = []

            # High missing values
            high_missing = [
                col
                for col, pct in missing_values.items()
                if pct > self.config.missing_value_threshold
            ]
            if high_missing:
                quality_issues.append(
                    f"High missing values in: {', '.join(high_missing)}"
                )

            # High outlier rates
            high_outliers = [
                col
                for col, pct in outlier_percentages.items()
                if pct > self.config.outlier_threshold
            ]
            if high_outliers:
                quality_issues.append(
                    f"High outlier rates in: {', '.join(high_outliers)}"
                )

            # Calculate overall quality score
            missing_score = 1 - (sum(missing_values.values()) / len(missing_values))
            outlier_score = 1 - (
                sum(outlier_percentages.values()) / max(len(outlier_percentages), 1)
            )
            overall_score = (missing_score + outlier_score) / 2

            result = DataQualityResult(
                model_name=model_name,
                missing_values=missing_values,
                outlier_percentages=outlier_percentages,
                data_types=data_types,
                feature_statistics=feature_statistics,
                quality_issues=quality_issues,
                overall_score=float(overall_score),
                total_samples=len(data),
            )

            # Record metrics
            self.metrics.record_data_quality_check(
                model_name=model_name,
                quality_score=overall_score,
                missing_value_pct=sum(missing_values.values()) / len(missing_values),
                outlier_pct=sum(outlier_percentages.values())
                / max(len(outlier_percentages), 1),
                issues_count=len(quality_issues),
            )

            logger.info(
                "Data quality check completed",
                model_name=model_name,
                overall_score=overall_score,
                issues_count=len(quality_issues),
                samples=len(data),
            )

            return result

        except Exception as error:
            logger.error(
                "Failed to check data quality",
                model_name=model_name,
                error=str(error),
            )
            raise

    def generate_evidently_report(
        self,
        model_name: str,
        current_data: pd.DataFrame,
        target_column: str | None = None,
        prediction_column: str | None = None,
    ) -> dict[str, Any]:
        """Generate comprehensive monitoring report using Evidently."""
        # TODO: Update to use Evidently v0.7+ API
        logger.warning(
            "Evidently report generation temporarily disabled - API needs updating"
        )
        return {
            "message": "Evidently report generation temporarily disabled",
            "model_name": model_name,
            "reference_samples": len(self.reference_data.get(model_name, [])),
            "current_samples": len(current_data),
        }

    def get_monitoring_summary(self, model_name: str) -> dict[str, Any]:
        """Get monitoring summary for a model."""
        try:
            summary = {
                "model_name": model_name,
                "monitoring_status": "active",
                "last_updated": datetime.now(timezone.utc).isoformat(),
                "reference_data_available": model_name in self.reference_data,
                "baseline_performance_available": model_name
                in self.baseline_performance,
            }

            if model_name in self.reference_data:
                ref_data = self.reference_data[model_name]
                summary["reference_data"] = {
                    "samples": len(ref_data),
                    "features": len(ref_data.columns),
                    "date_range": {
                        "start": ref_data.index.min().isoformat()
                        if hasattr(ref_data.index, "min")
                        else "unknown",
                        "end": ref_data.index.max().isoformat()
                        if hasattr(ref_data.index, "max")
                        else "unknown",
                    },
                }

            if model_name in self.baseline_performance:
                summary["baseline_metrics"] = self.baseline_performance[model_name]

            return summary

        except Exception as error:
            logger.error(
                "Failed to get monitoring summary",
                model_name=model_name,
                error=str(error),
            )
            raise
