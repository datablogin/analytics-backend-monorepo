"""Feature monitoring and drift detection."""

import json
import statistics
from datetime import datetime, timedelta
from enum import Enum
from typing import Any

import structlog
from libs.analytics_core.models import BaseModel as SQLBaseModel
from libs.observability.metrics import MLOpsMetrics
from pydantic import BaseModel, Field
from sqlalchemy import Boolean, Column, DateTime, Float, String, Text, select

from .core import FeatureStoreService
from .models import FeatureType

logger = structlog.get_logger(__name__)


class DriftStatus(str, Enum):
    """Drift detection status."""

    NO_DRIFT = "no_drift"
    LOW_DRIFT = "low_drift"
    MEDIUM_DRIFT = "medium_drift"
    HIGH_DRIFT = "high_drift"


class AlertLevel(str, Enum):
    """Alert severity levels."""

    INFO = "info"
    WARNING = "warning"
    ERROR = "error"
    CRITICAL = "critical"


# Database Models
class FeatureMonitoringProfile(SQLBaseModel):
    """Feature monitoring profile storage."""

    __tablename__ = "feature_monitoring_profiles"

    feature_name: str = Column(String(255), nullable=False, unique=True, index=True)
    baseline_stats: str = Column(Text, nullable=False)  # JSON string
    monitoring_config: str = Column(Text, nullable=True)  # JSON string
    last_updated: datetime = Column(DateTime(timezone=True), nullable=False)
    is_active: bool = Column(Boolean, default=True)


class FeatureDriftDetection(SQLBaseModel):
    """Feature drift detection results."""

    __tablename__ = "feature_drift_detections"

    feature_name: str = Column(String(255), nullable=False, index=True)
    drift_score: float = Column(Float, nullable=False)
    drift_status: str = Column(String(50), nullable=False)
    detection_method: str = Column(String(100), nullable=False)
    baseline_period_start: datetime = Column(DateTime(timezone=True), nullable=False)
    baseline_period_end: datetime = Column(DateTime(timezone=True), nullable=False)
    current_period_start: datetime = Column(DateTime(timezone=True), nullable=False)
    current_period_end: datetime = Column(DateTime(timezone=True), nullable=False)
    statistics: str = Column(Text, nullable=True)  # JSON string
    alert_triggered: bool = Column(Boolean, default=False)


class FeatureAlert(SQLBaseModel):
    """Feature monitoring alerts."""

    __tablename__ = "feature_alerts"

    feature_name: str = Column(String(255), nullable=False, index=True)
    alert_type: str = Column(String(100), nullable=False)
    alert_level: str = Column(String(50), nullable=False)
    message: str = Column(Text, nullable=False)
    alert_data: str = Column(Text, nullable=True)  # JSON string
    acknowledged: bool = Column(Boolean, default=False)
    acknowledged_by: str = Column(String(255), nullable=True)
    acknowledged_at: datetime = Column(DateTime(timezone=True), nullable=True)


# Pydantic Models
class MonitoringConfig(BaseModel):
    """Monitoring configuration for a feature."""

    drift_threshold_low: float = Field(default=0.1, description="Low drift threshold")
    drift_threshold_medium: float = Field(
        default=0.3, description="Medium drift threshold"
    )
    drift_threshold_high: float = Field(default=0.5, description="High drift threshold")
    baseline_window_days: int = Field(default=7, description="Baseline window in days")
    monitoring_window_days: int = Field(
        default=1, description="Current monitoring window in days"
    )
    min_samples: int = Field(
        default=100, description="Minimum samples for drift detection"
    )
    enable_alerts: bool = Field(default=True, description="Enable drift alerts")
    alert_recipients: list[str] = Field(
        default_factory=list, description="Alert recipients"
    )


class BaselineStats(BaseModel):
    """Baseline statistics for feature monitoring."""

    count: int = Field(..., description="Number of samples")
    mean: float | None = Field(None, description="Mean value (numeric features)")
    std: float | None = Field(None, description="Standard deviation (numeric features)")
    min_val: float | None = Field(None, description="Minimum value (numeric features)")
    max_val: float | None = Field(None, description="Maximum value (numeric features)")
    percentiles: dict[str, float] = Field(
        default_factory=dict, description="Percentile values"
    )
    value_counts: dict[str, int] = Field(
        default_factory=dict, description="Value counts (categorical features)"
    )
    null_percentage: float = Field(..., description="Percentage of null values")
    unique_values: int = Field(..., description="Number of unique values")
    created_at: datetime = Field(
        default_factory=datetime.utcnow, description="Creation timestamp"
    )


class DriftDetectionResult(BaseModel):
    """Drift detection result."""

    feature_name: str = Field(..., description="Feature name")
    drift_score: float = Field(..., description="Drift score (0-1)")
    drift_status: DriftStatus = Field(..., description="Drift status")
    detection_method: str = Field(..., description="Detection method used")
    baseline_stats: BaselineStats = Field(..., description="Baseline statistics")
    current_stats: BaselineStats = Field(..., description="Current period statistics")
    details: dict[str, Any] = Field(
        default_factory=dict, description="Additional details"
    )
    timestamp: datetime = Field(
        default_factory=datetime.utcnow, description="Detection timestamp"
    )


class FeatureAlertModel(BaseModel):
    """Feature alert model."""

    id: int = Field(..., description="Alert ID")
    feature_name: str = Field(..., description="Feature name")
    alert_type: str = Field(..., description="Alert type")
    alert_level: AlertLevel = Field(..., description="Alert level")
    message: str = Field(..., description="Alert message")
    alert_data: dict[str, Any] = Field(default_factory=dict, description="Alert data")
    acknowledged: bool = Field(..., description="Whether alert is acknowledged")
    acknowledged_by: str | None = Field(None, description="Who acknowledged the alert")
    acknowledged_at: datetime | None = Field(
        None, description="When alert was acknowledged"
    )
    created_at: datetime = Field(..., description="Alert creation time")


class FeatureMonitor:
    """Feature monitoring and drift detection service."""

    def __init__(self, feature_store_service: FeatureStoreService):
        """Initialize feature monitor."""
        self.feature_store_service = feature_store_service
        self.metrics = MLOpsMetrics()

    async def create_baseline(
        self,
        feature_name: str,
        start_time: datetime,
        end_time: datetime,
        config: MonitoringConfig | None = None,
    ) -> BaselineStats:
        """Create baseline statistics for a feature."""
        try:
            logger.info(
                "Creating baseline for feature",
                feature_name=feature_name,
                start_time=start_time,
                end_time=end_time,
            )

            # Get feature definition to understand the feature type
            feature_def = await self.feature_store_service.get_feature(feature_name)
            if not feature_def:
                raise ValueError(f"Feature '{feature_name}' not found")

            # Get historical feature values
            async with self.feature_store_service.db_manager.get_session() as session:
                from .models import FeatureValue

                stmt = select(FeatureValue).where(
                    FeatureValue.feature_name == feature_name,
                    FeatureValue.timestamp >= start_time,
                    FeatureValue.timestamp <= end_time,
                )

                result = await session.execute(stmt)
                feature_values = result.scalars().all()

            if not feature_values:
                raise ValueError(
                    f"No feature values found for '{feature_name}' in the specified time range"
                )

            # Extract values and convert from JSON
            values = []
            null_count = 0

            for fv in feature_values:
                try:
                    value = json.loads(fv.value)
                    if value is None:
                        null_count += 1
                    else:
                        values.append(value)
                except (json.JSONDecodeError, TypeError):
                    # Handle non-JSON values
                    if fv.value is None or fv.value == "null":
                        null_count += 1
                    else:
                        values.append(fv.value)

            total_count = len(feature_values)
            null_percentage = (null_count / total_count) * 100 if total_count > 0 else 0

            # Calculate statistics based on feature type
            if feature_def.feature_type in [FeatureType.INTEGER, FeatureType.FLOAT]:
                baseline_stats = self._calculate_numerical_stats(
                    values, total_count, null_percentage
                )
            else:
                baseline_stats = self._calculate_categorical_stats(
                    values, total_count, null_percentage
                )

            # Store baseline profile
            await self._store_baseline_profile(
                feature_name, baseline_stats, config or MonitoringConfig()
            )

            logger.info(
                "Baseline created successfully",
                feature_name=feature_name,
                sample_count=total_count,
                null_percentage=null_percentage,
            )

            return baseline_stats

        except Exception as error:
            logger.error(
                "Failed to create baseline",
                feature_name=feature_name,
                error=str(error),
            )
            raise

    def _calculate_numerical_stats(
        self, values: list[Any], total_count: int, null_percentage: float
    ) -> BaselineStats:
        """Calculate statistics for numerical features."""
        # Filter and convert to numeric values
        numeric_values = []
        for val in values:
            try:
                if isinstance(val, int | float):
                    numeric_values.append(float(val))
                else:
                    numeric_values.append(float(val))
            except (ValueError, TypeError):
                continue

        if not numeric_values:
            return BaselineStats(
                count=total_count,
                null_percentage=null_percentage,
                unique_values=0,
            )

        # Calculate statistics
        mean_val = statistics.mean(numeric_values)
        std_val = statistics.stdev(numeric_values) if len(numeric_values) > 1 else 0.0
        min_val = min(numeric_values)
        max_val = max(numeric_values)

        # Calculate percentiles
        sorted_values = sorted(numeric_values)
        percentiles = {}
        for p in [25, 50, 75, 90, 95, 99]:
            idx = int((p / 100) * (len(sorted_values) - 1))
            percentiles[f"p{p}"] = sorted_values[idx]

        return BaselineStats(
            count=total_count,
            mean=mean_val,
            std=std_val,
            min_val=min_val,
            max_val=max_val,
            percentiles=percentiles,
            null_percentage=null_percentage,
            unique_values=len(set(numeric_values)),
        )

    def _calculate_categorical_stats(
        self, values: list[Any], total_count: int, null_percentage: float
    ) -> BaselineStats:
        """Calculate statistics for categorical features."""
        # Convert values to strings for categorical analysis
        str_values = [str(val) for val in values if val is not None]

        # Count occurrences
        value_counts = {}
        for val in str_values:
            value_counts[val] = value_counts.get(val, 0) + 1

        return BaselineStats(
            count=total_count,
            value_counts=value_counts,
            null_percentage=null_percentage,
            unique_values=len(set(str_values)),
        )

    async def _store_baseline_profile(
        self, feature_name: str, baseline_stats: BaselineStats, config: MonitoringConfig
    ) -> None:
        """Store baseline profile in database."""
        async with self.feature_store_service.db_manager.get_session() as session:
            # Check if profile already exists
            stmt = select(FeatureMonitoringProfile).where(
                FeatureMonitoringProfile.feature_name == feature_name
            )
            result = await session.execute(stmt)
            existing_profile = result.scalar_one_or_none()

            if existing_profile:
                # Update existing profile
                existing_profile.baseline_stats = json.dumps(
                    baseline_stats.model_dump(), default=str
                )
                existing_profile.monitoring_config = json.dumps(config.model_dump())
                existing_profile.last_updated = datetime.utcnow()
            else:
                # Create new profile
                profile = FeatureMonitoringProfile(
                    feature_name=feature_name,
                    baseline_stats=json.dumps(baseline_stats.model_dump(), default=str),
                    monitoring_config=json.dumps(config.model_dump()),
                    last_updated=datetime.utcnow(),
                    is_active=True,
                )
                session.add(profile)

            await session.commit()

    async def _calculate_statistics_only(
        self,
        feature_name: str,
        start_time: datetime,
        end_time: datetime,
    ) -> BaselineStats:
        """Calculate statistics without storing as baseline profile."""
        try:
            # Get feature definition to understand the feature type
            feature_def = await self.feature_store_service.get_feature(feature_name)
            if not feature_def:
                raise ValueError(f"Feature '{feature_name}' not found")

            # Get historical feature values
            async with self.feature_store_service.db_manager.get_session() as session:
                from .models import FeatureValue

                stmt = select(FeatureValue).where(
                    FeatureValue.feature_name == feature_name,
                    FeatureValue.timestamp >= start_time,
                    FeatureValue.timestamp <= end_time,
                )

                result = await session.execute(stmt)
                feature_values = result.scalars().all()

            if not feature_values:
                raise ValueError(
                    f"No feature values found for '{feature_name}' in the specified time range"
                )

            # Extract values and convert from JSON
            values = []
            null_count = 0

            for fv in feature_values:
                try:
                    value = json.loads(fv.value)
                    if value is None:
                        null_count += 1
                    else:
                        values.append(value)
                except (json.JSONDecodeError, TypeError):
                    # Handle non-JSON values
                    if fv.value is None or fv.value == "null":
                        null_count += 1
                    else:
                        values.append(fv.value)

            total_count = len(feature_values)
            null_percentage = (null_count / total_count) * 100 if total_count > 0 else 0

            # Calculate statistics based on feature type
            if feature_def.feature_type in [FeatureType.INTEGER, FeatureType.FLOAT]:
                return self._calculate_numerical_stats(
                    values, total_count, null_percentage
                )
            else:
                return self._calculate_categorical_stats(
                    values, total_count, null_percentage
                )

        except Exception as error:
            logger.error(
                "Failed to calculate statistics",
                feature_name=feature_name,
                error=str(error),
            )
            raise

    async def detect_drift(
        self,
        feature_name: str,
        current_start_time: datetime,
        current_end_time: datetime,
    ) -> DriftDetectionResult:
        """Detect drift for a feature."""
        try:
            logger.info(
                "Detecting drift for feature",
                feature_name=feature_name,
                current_start_time=current_start_time,
                current_end_time=current_end_time,
            )

            # Get baseline profile
            baseline_profile = await self._get_baseline_profile(feature_name)
            if not baseline_profile:
                raise ValueError(
                    f"No baseline profile found for feature '{feature_name}'"
                )

            baseline_stats = BaselineStats(
                **json.loads(baseline_profile.baseline_stats)
            )
            config = MonitoringConfig(
                **json.loads(baseline_profile.monitoring_config or "{}")
            )

            # Get current period statistics without storing as baseline
            current_stats = await self._calculate_statistics_only(
                feature_name, current_start_time, current_end_time
            )

            # Calculate drift score
            drift_score, detection_method, details = self._calculate_drift_score(
                baseline_stats, current_stats
            )

            # Determine drift status
            drift_status = self._determine_drift_status(drift_score, config)

            # Store drift detection result
            await self._store_drift_detection(
                feature_name=feature_name,
                drift_score=drift_score,
                drift_status=drift_status,
                detection_method=detection_method,
                baseline_profile=baseline_profile,
                current_start_time=current_start_time,
                current_end_time=current_end_time,
                statistics=details,
            )

            # Trigger alerts if needed
            if config.enable_alerts and drift_status != DriftStatus.NO_DRIFT:
                await self._trigger_drift_alert(
                    feature_name, drift_status, drift_score, config
                )

            result = DriftDetectionResult(
                feature_name=feature_name,
                drift_score=drift_score,
                drift_status=drift_status,
                detection_method=detection_method,
                baseline_stats=baseline_stats,
                current_stats=current_stats,
                details=details,
            )

            logger.info(
                "Drift detection completed",
                feature_name=feature_name,
                drift_score=drift_score,
                drift_status=drift_status,
            )

            return result

        except Exception as error:
            logger.error(
                "Drift detection failed",
                feature_name=feature_name,
                error=str(error),
            )
            raise

    def _calculate_drift_score(
        self, baseline_stats: BaselineStats, current_stats: BaselineStats
    ) -> tuple[float, str, dict[str, Any]]:
        """Calculate drift score between baseline and current statistics."""
        details = {}

        # For numerical features
        if baseline_stats.mean is not None and current_stats.mean is not None:
            # Use Population Stability Index (PSI) for numerical features
            drift_score = self._calculate_psi_numerical(baseline_stats, current_stats)
            detection_method = "PSI_numerical"

            details = {
                "baseline_mean": baseline_stats.mean,
                "current_mean": current_stats.mean,
                "baseline_std": baseline_stats.std,
                "current_std": current_stats.std,
                "mean_shift": abs(current_stats.mean - baseline_stats.mean),
                "std_ratio": current_stats.std / baseline_stats.std
                if baseline_stats.std and baseline_stats.std > 0
                else 0,
            }

        # For categorical features
        elif baseline_stats.value_counts and current_stats.value_counts:
            drift_score = self._calculate_psi_categorical(baseline_stats, current_stats)
            detection_method = "PSI_categorical"

            details = {
                "baseline_unique_values": baseline_stats.unique_values,
                "current_unique_values": current_stats.unique_values,
                "new_categories": list(
                    set(current_stats.value_counts.keys())
                    - set(baseline_stats.value_counts.keys())
                ),
                "missing_categories": list(
                    set(baseline_stats.value_counts.keys())
                    - set(current_stats.value_counts.keys())
                ),
            }

        else:
            # Fallback to simple count comparison
            drift_score = (
                abs(current_stats.count - baseline_stats.count) / baseline_stats.count
            )
            detection_method = "count_comparison"
            details = {
                "baseline_count": baseline_stats.count,
                "current_count": current_stats.count,
            }

        return min(drift_score, 1.0), detection_method, details

    def _calculate_psi_numerical(
        self, baseline_stats: BaselineStats, current_stats: BaselineStats
    ) -> float:
        """Calculate Population Stability Index for numerical features."""
        # Simplified PSI calculation based on mean and std changes
        if not baseline_stats.mean or not baseline_stats.std or baseline_stats.std == 0:
            return 0.0

        mean_change = abs(current_stats.mean - baseline_stats.mean) / baseline_stats.std
        std_change = (
            abs(current_stats.std - baseline_stats.std) / baseline_stats.std
            if baseline_stats.std > 0
            else 0
        )

        # Simple PSI approximation
        psi = (mean_change + std_change) / 2

        return min(psi, 1.0)

    def _calculate_psi_categorical(
        self, baseline_stats: BaselineStats, current_stats: BaselineStats
    ) -> float:
        """Calculate Population Stability Index for categorical features."""
        if not baseline_stats.value_counts or not current_stats.value_counts:
            return 0.0

        # Calculate distributions
        baseline_total = sum(baseline_stats.value_counts.values())
        current_total = sum(current_stats.value_counts.values())

        if baseline_total == 0 or current_total == 0:
            return 0.0

        # Get all categories
        all_categories = set(baseline_stats.value_counts.keys()) | set(
            current_stats.value_counts.keys()
        )

        psi = 0.0
        for category in all_categories:
            baseline_pct = baseline_stats.value_counts.get(category, 0) / baseline_total
            current_pct = current_stats.value_counts.get(category, 0) / current_total

            # Add small epsilon to avoid division by zero
            epsilon = 1e-6
            baseline_pct = max(baseline_pct, epsilon)
            current_pct = max(current_pct, epsilon)

            # Correct PSI formula: (current - baseline) * ln(current / baseline)
            import math

            psi += (current_pct - baseline_pct) * math.log(current_pct / baseline_pct)

        return min(psi, 1.0)

    def _determine_drift_status(
        self, drift_score: float, config: MonitoringConfig
    ) -> DriftStatus:
        """Determine drift status based on score and thresholds."""
        if drift_score >= config.drift_threshold_high:
            return DriftStatus.HIGH_DRIFT
        elif drift_score >= config.drift_threshold_medium:
            return DriftStatus.MEDIUM_DRIFT
        elif drift_score >= config.drift_threshold_low:
            return DriftStatus.LOW_DRIFT
        else:
            return DriftStatus.NO_DRIFT

    async def _get_baseline_profile(
        self, feature_name: str
    ) -> FeatureMonitoringProfile | None:
        """Get baseline profile for a feature."""
        async with self.feature_store_service.db_manager.get_session() as session:
            stmt = select(FeatureMonitoringProfile).where(
                FeatureMonitoringProfile.feature_name == feature_name,
                FeatureMonitoringProfile.is_active,
            )
            result = await session.execute(stmt)
            return result.scalar_one_or_none()

    async def _store_drift_detection(
        self,
        feature_name: str,
        drift_score: float,
        drift_status: DriftStatus,
        detection_method: str,
        baseline_profile: FeatureMonitoringProfile,
        current_start_time: datetime,
        current_end_time: datetime,
        statistics: dict[str, Any],
    ) -> None:
        """Store drift detection result."""
        async with self.feature_store_service.db_manager.get_session() as session:
            detection = FeatureDriftDetection(
                feature_name=feature_name,
                drift_score=drift_score,
                drift_status=drift_status.value,
                detection_method=detection_method,
                baseline_period_start=baseline_profile.created_at,
                baseline_period_end=baseline_profile.last_updated,
                current_period_start=current_start_time,
                current_period_end=current_end_time,
                statistics=json.dumps(statistics, default=str),
                alert_triggered=False,
            )

            session.add(detection)
            await session.commit()

    async def _trigger_drift_alert(
        self,
        feature_name: str,
        drift_status: DriftStatus,
        drift_score: float,
        config: MonitoringConfig,
    ) -> None:
        """Trigger drift alert."""
        alert_level = AlertLevel.WARNING
        if drift_status == DriftStatus.HIGH_DRIFT:
            alert_level = AlertLevel.ERROR

        message = f"Drift detected for feature '{feature_name}': {drift_status.value} (score: {drift_score:.3f})"

        await self.create_alert(
            feature_name=feature_name,
            alert_type="drift_detection",
            alert_level=alert_level,
            message=message,
            alert_data={
                "drift_score": drift_score,
                "drift_status": drift_status.value,
                "recipients": config.alert_recipients,
            },
        )

    async def create_alert(
        self,
        feature_name: str,
        alert_type: str,
        alert_level: AlertLevel,
        message: str,
        alert_data: dict[str, Any] | None = None,
    ) -> int:
        """Create a feature alert."""
        async with self.feature_store_service.db_manager.get_session() as session:
            alert = FeatureAlert(
                feature_name=feature_name,
                alert_type=alert_type,
                alert_level=alert_level.value,
                message=message,
                alert_data=json.dumps(alert_data or {}, default=str),
                acknowledged=False,
            )

            session.add(alert)
            await session.commit()
            await session.refresh(alert)

            logger.info(
                "Alert created",
                feature_name=feature_name,
                alert_type=alert_type,
                alert_level=alert_level,
                alert_id=alert.id,
            )

            return alert.id

    async def get_alerts(
        self,
        feature_name: str | None = None,
        alert_level: AlertLevel | None = None,
        acknowledged: bool | None = None,
        limit: int = 100,
    ) -> list[FeatureAlertModel]:
        """Get feature alerts."""
        async with self.feature_store_service.db_manager.get_session() as session:
            stmt = select(FeatureAlert)

            if feature_name:
                stmt = stmt.where(FeatureAlert.feature_name == feature_name)
            if alert_level:
                stmt = stmt.where(FeatureAlert.alert_level == alert_level.value)
            if acknowledged is not None:
                stmt = stmt.where(FeatureAlert.acknowledged == acknowledged)

            stmt = stmt.order_by(FeatureAlert.created_at.desc()).limit(limit)

            result = await session.execute(stmt)
            alerts = result.scalars().all()

            return [
                FeatureAlertModel(
                    id=alert.id,
                    feature_name=alert.feature_name,
                    alert_type=alert.alert_type,
                    alert_level=AlertLevel(alert.alert_level),
                    message=alert.message,
                    alert_data=json.loads(alert.alert_data or "{}"),
                    acknowledged=alert.acknowledged,
                    acknowledged_by=alert.acknowledged_by,
                    acknowledged_at=alert.acknowledged_at,
                    created_at=alert.created_at,
                )
                for alert in alerts
            ]

    async def acknowledge_alert(self, alert_id: int, acknowledged_by: str) -> bool:
        """Acknowledge an alert."""
        async with self.feature_store_service.db_manager.get_session() as session:
            alert = await session.get(FeatureAlert, alert_id)
            if alert:
                alert.acknowledged = True
                alert.acknowledged_by = acknowledged_by
                alert.acknowledged_at = datetime.utcnow()
                await session.commit()

                logger.info(
                    "Alert acknowledged",
                    alert_id=alert_id,
                    acknowledged_by=acknowledged_by,
                )

                return True

            return False

    async def get_drift_history(
        self,
        feature_name: str,
        days_back: int = 30,
    ) -> list[DriftDetectionResult]:
        """Get drift detection history for a feature."""
        async with self.feature_store_service.db_manager.get_session() as session:
            cutoff_date = datetime.utcnow() - timedelta(days=days_back)

            stmt = (
                select(FeatureDriftDetection)
                .where(
                    FeatureDriftDetection.feature_name == feature_name,
                    FeatureDriftDetection.created_at >= cutoff_date,
                )
                .order_by(FeatureDriftDetection.created_at.desc())
            )

            result = await session.execute(stmt)
            detections = result.scalars().all()

            # Convert to response models (simplified)
            drift_results = []
            for detection in detections:
                # Get baseline stats (simplified - in practice would query the actual baseline)
                baseline_stats = BaselineStats(
                    count=0,
                    null_percentage=0.0,
                    unique_values=0,
                )

                current_stats = BaselineStats(
                    count=0,
                    null_percentage=0.0,
                    unique_values=0,
                )

                drift_results.append(
                    DriftDetectionResult(
                        feature_name=detection.feature_name,
                        drift_score=detection.drift_score,
                        drift_status=DriftStatus(detection.drift_status),
                        detection_method=detection.detection_method,
                        baseline_stats=baseline_stats,
                        current_stats=current_stats,
                        details=json.loads(detection.statistics or "{}"),
                        timestamp=detection.created_at,
                    )
                )

            return drift_results
