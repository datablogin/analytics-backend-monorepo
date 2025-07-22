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
        """Perform Kolmogorov-Smirnov test for distribution comparison with validation."""
        try:
            # Input validation
            if reference_values is None or current_values is None:
                logger.error("Null input data for KS test")
                return 0.0, 1.0

            # Check for empty series
            if len(reference_values) == 0 or len(current_values) == 0:
                logger.error("Empty input data for KS test")
                return 0.0, 1.0

            # Remove null values and validate remaining data
            ref_clean = reference_values.dropna()
            cur_clean = current_values.dropna()

            if len(ref_clean) == 0 or len(cur_clean) == 0:
                logger.warning(
                    "All values are null after cleaning - cannot perform KS test"
                )
                return 0.0, 1.0

            # Check minimum sample size for statistical validity
            min_samples = 5
            if len(ref_clean) < min_samples or len(cur_clean) < min_samples:
                logger.warning(
                    f"Insufficient samples for KS test: ref={len(ref_clean)}, cur={len(cur_clean)}, min={min_samples}"
                )
                return 0.0, 1.0

            # Handle categorical data
            if ref_clean.dtype == "object" or cur_clean.dtype == "object":
                return self._chi_square_test(ref_clean, cur_clean)

            # Numerical data validation
            # Check for infinite values
            if np.isinf(ref_clean).any() or np.isinf(cur_clean).any():
                logger.warning("Infinite values detected - removing for KS test")
                ref_clean = ref_clean[np.isfinite(ref_clean)]
                cur_clean = cur_clean[np.isfinite(cur_clean)]

                if len(ref_clean) < min_samples or len(cur_clean) < min_samples:
                    logger.error("Insufficient finite samples after cleaning")
                    return 0.0, 1.0

            # Check for constant values (no variation)
            if ref_clean.nunique() == 1 and cur_clean.nunique() == 1:
                # Both series are constant - compare the constants
                if ref_clean.iloc[0] == cur_clean.iloc[0]:
                    return 0.0, 1.0  # No difference
                else:
                    return 1.0, 0.0  # Maximum difference

            # Perform KS test
            statistic, p_value = stats.ks_2samp(ref_clean, cur_clean)

            # Validate results
            if np.isnan(statistic) or np.isnan(p_value):
                logger.error("KS test returned NaN values")
                return 0.0, 1.0

            return float(statistic), float(p_value)

        except Exception as error:
            logger.error("KS test failed", error=str(error))
            # Return neutral values if test fails
            return 0.0, 1.0

    def _chi_square_test(
        self, reference_values: pd.Series, current_values: pd.Series
    ) -> tuple[float, float]:
        """Perform chi-square test for categorical data."""
        try:
            ref_counts = reference_values.value_counts()
            cur_counts = current_values.value_counts()

            # Align categories
            all_categories = set(ref_counts.index) | set(cur_counts.index)
            ref_aligned = [ref_counts.get(cat, 0) for cat in all_categories]
            cur_aligned = [cur_counts.get(cat, 0) for cat in all_categories]

            # Validate counts
            if sum(ref_aligned) == 0 or sum(cur_aligned) == 0:
                logger.error("Zero total counts in chi-square test")
                return 0.0, 1.0

            # Chi-square test requires expected frequencies >= 5
            # Use approximation if this condition is violated
            min_expected = min(min(ref_aligned), min(cur_aligned))
            if min_expected < 5:
                logger.warning(
                    f"Low expected frequencies in chi-square test (min={min_expected}) - results may be unreliable"
                )

            statistic, p_value = stats.chisquare(cur_aligned, ref_aligned)

            # Validate results
            if np.isnan(statistic) or np.isnan(p_value):
                logger.error("Chi-square test returned NaN values")
                return 0.0, 1.0

            return float(statistic), float(p_value)

        except Exception as error:
            logger.error("Chi-square test failed", error=str(error))
            return 0.0, 1.0

    def _population_stability_index(
        self, reference_values: pd.Series, current_values: pd.Series
    ) -> float:
        """Calculate Population Stability Index (PSI) with validation."""
        try:
            # Input validation
            if reference_values is None or current_values is None:
                logger.error("Null input data for PSI calculation")
                return 0.0

            # Check for empty series
            if len(reference_values) == 0 or len(current_values) == 0:
                logger.error("Empty input data for PSI calculation")
                return 0.0

            # Remove null values and validate remaining data
            ref_clean = reference_values.dropna()
            cur_clean = current_values.dropna()

            if len(ref_clean) == 0 or len(cur_clean) == 0:
                logger.warning(
                    "All values are null after cleaning - cannot calculate PSI"
                )
                return 0.0

            # Check minimum sample size
            min_samples = 10
            if len(ref_clean) < min_samples or len(cur_clean) < min_samples:
                logger.warning(
                    f"Insufficient samples for PSI: ref={len(ref_clean)}, cur={len(cur_clean)}, min={min_samples}"
                )
                return 0.0

            # Handle categorical data
            if ref_clean.dtype == "object":
                return self._calculate_categorical_psi(ref_clean, cur_clean)
            else:
                return self._calculate_numerical_psi(ref_clean, cur_clean)

        except Exception as error:
            logger.error("PSI calculation failed", error=str(error))
            return 0.0

    def _calculate_categorical_psi(
        self, reference_values: pd.Series, current_values: pd.Series
    ) -> float:
        """Calculate PSI for categorical data."""
        try:
            ref_counts = reference_values.value_counts(normalize=True)
            cur_counts = current_values.value_counts(normalize=True)

            # Align categories
            all_categories = set(ref_counts.index) | set(cur_counts.index)

            psi = 0.0
            for category in all_categories:
                # Use small value to avoid log(0) but ensure it's meaningful
                ref_pct = ref_counts.get(category, 1e-6)
                cur_pct = cur_counts.get(category, 1e-6)

                # Validate percentages
                if ref_pct <= 0 or cur_pct <= 0:
                    continue

                psi_component = (cur_pct - ref_pct) * np.log(cur_pct / ref_pct)

                # Check for invalid results
                if np.isnan(psi_component) or np.isinf(psi_component):
                    logger.warning(f"Invalid PSI component for category {category}")
                    continue

                psi += psi_component

            return abs(psi)

        except Exception as error:
            logger.error("Categorical PSI calculation failed", error=str(error))
            return 0.0

    def _calculate_numerical_psi(
        self, reference_values: pd.Series, current_values: pd.Series
    ) -> float:
        """Calculate PSI for numerical data."""
        try:
            # Remove infinite values
            ref_finite = reference_values[np.isfinite(reference_values)]
            cur_finite = current_values[np.isfinite(current_values)]

            if len(ref_finite) == 0 or len(cur_finite) == 0:
                logger.error("No finite values for PSI calculation")
                return 0.0

            # Check for constant values
            if ref_finite.nunique() == 1:
                logger.warning(
                    "Reference data has no variation - PSI may not be meaningful"
                )
                if cur_finite.nunique() == 1:
                    return 0.0 if ref_finite.iloc[0] == cur_finite.iloc[0] else 1.0

            # Determine number of bins based on sample size
            n_bins = min(
                10, max(3, int(np.sqrt(min(len(ref_finite), len(cur_finite)))))
            )

            # Create bins based on reference data quantiles for better stability
            try:
                bin_edges = np.unique(
                    np.percentile(ref_finite, np.linspace(0, 100, n_bins + 1))
                )

                # Ensure we have at least 2 bins
                if len(bin_edges) < 3:
                    logger.warning(
                        "Insufficient unique values for binning - using value range"
                    )
                    bin_edges = np.linspace(
                        ref_finite.min(), ref_finite.max(), n_bins + 1
                    )

            except Exception:
                # Fallback binning
                bin_edges = np.linspace(ref_finite.min(), ref_finite.max(), n_bins + 1)

            # Calculate histograms
            ref_hist, _ = np.histogram(ref_finite, bins=bin_edges)
            cur_hist, _ = np.histogram(cur_finite, bins=bin_edges)

            # Normalize to probabilities
            ref_total = ref_hist.sum()
            cur_total = cur_hist.sum()

            if ref_total == 0 or cur_total == 0:
                logger.error("Zero histogram totals in PSI calculation")
                return 0.0

            ref_probs = ref_hist / ref_total
            cur_probs = cur_hist / cur_total

            # Add small constant to avoid log(0) - use adaptive smoothing
            smoothing_factor = 1e-6
            ref_probs = np.where(ref_probs == 0, smoothing_factor, ref_probs)
            cur_probs = np.where(cur_probs == 0, smoothing_factor, cur_probs)

            # Calculate PSI
            psi_components = (cur_probs - ref_probs) * np.log(cur_probs / ref_probs)

            # Remove invalid components
            valid_components = psi_components[np.isfinite(psi_components)]

            if len(valid_components) == 0:
                logger.error("No valid PSI components calculated")
                return 0.0

            psi = np.sum(valid_components)

            return abs(psi) if np.isfinite(psi) else 0.0

        except Exception as error:
            logger.error("Numerical PSI calculation failed", error=str(error))
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
        """Generate comprehensive monitoring report using Evidently v0.7+ API."""
        try:
            if model_name not in self.reference_data:
                logger.warning(
                    "No reference data found for Evidently report",
                    model_name=model_name,
                )
                return {
                    "message": "No reference data available for report generation",
                    "model_name": model_name,
                    "current_samples": len(current_data),
                }

            from evidently import ColumnMapping
            from evidently.metrics import (
                DataDriftTable,
                DatasetDriftMetric,
                DatasetSummaryMetric,
            )
            from evidently.report import Report

            reference_data = self.reference_data[model_name]

            # Set up column mapping
            column_mapping = ColumnMapping()
            if target_column and target_column in current_data.columns:
                column_mapping.target = target_column
            if prediction_column and prediction_column in current_data.columns:
                column_mapping.prediction = prediction_column

            # Create report with v0.7+ metrics
            report = Report(
                metrics=[
                    DatasetSummaryMetric(),
                    DatasetDriftMetric(),
                    DataDriftTable(),
                ]
            )

            # Run report
            report.run(
                reference_data=reference_data,
                current_data=current_data,
                column_mapping=column_mapping,
            )

            # Get results as dict
            results = report.as_dict()

            logger.info(
                "Evidently report generated successfully",
                model_name=model_name,
                reference_samples=len(reference_data),
                current_samples=len(current_data),
            )

            return {
                "message": "Evidently report generated successfully",
                "model_name": model_name,
                "reference_samples": len(reference_data),
                "current_samples": len(current_data),
                "report_data": results,
            }

        except ImportError as e:
            logger.error(
                "Evidently not available - install with: pip install evidently>=0.7.0",
                error=str(e),
            )
            return {
                "message": "Evidently not installed",
                "model_name": model_name,
                "error": str(e),
            }
        except Exception as error:
            logger.error(
                "Failed to generate Evidently report",
                model_name=model_name,
                error=str(error),
            )
            return {
                "message": "Failed to generate Evidently report",
                "model_name": model_name,
                "reference_samples": len(self.reference_data.get(model_name, [])),
                "current_samples": len(current_data),
                "error": str(error),
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
