"""Tests for feature monitoring and drift detection."""

from datetime import datetime, timedelta
from unittest.mock import AsyncMock, Mock, patch

import pytest
from services.feature_store.models import FeatureType
from services.feature_store.monitoring import (
    AlertLevel,
    BaselineStats,
    DriftStatus,
    FeatureMonitor,
    MonitoringConfig,
)


@pytest.fixture
def mock_feature_store_service():
    """Mock feature store service."""
    service = Mock()
    service.db_manager = Mock()
    service.db_manager.get_session = AsyncMock()
    service.get_feature = AsyncMock()
    return service


@pytest.fixture
def feature_monitor(mock_feature_store_service):
    """Feature monitor with mocked dependencies."""
    return FeatureMonitor(mock_feature_store_service)


@pytest.fixture
def sample_feature_def():
    """Sample feature definition."""
    feature = Mock()
    feature.name = "user_age"
    feature.feature_type = FeatureType.INTEGER
    return feature


@pytest.fixture
def sample_monitoring_config():
    """Sample monitoring configuration."""
    return MonitoringConfig(
        drift_threshold_low=0.1,
        drift_threshold_medium=0.3,
        drift_threshold_high=0.5,
        baseline_window_days=7,
        enable_alerts=True,
    )


@pytest.fixture
def sample_baseline_stats():
    """Sample baseline statistics."""
    return BaselineStats(
        count=1000,
        mean=25.5,
        std=5.2,
        min_val=18.0,
        max_val=65.0,
        percentiles={"p25": 22.0, "p50": 25.0, "p75": 28.0},
        null_percentage=2.0,
        unique_values=45,
    )


class TestFeatureMonitor:
    """Tests for FeatureMonitor."""

    @pytest.mark.asyncio
    async def test_create_baseline_numerical(
        self, feature_monitor, mock_feature_store_service, sample_feature_def
    ):
        """Test creating baseline for numerical feature."""
        # Mock feature lookup
        mock_feature_store_service.get_feature.return_value = sample_feature_def

        # Mock session and feature values
        mock_session = AsyncMock()
        mock_feature_store_service.db_manager.get_session.return_value.__aenter__.return_value = mock_session

        # Mock feature values
        mock_values = []
        for i in range(100):
            mock_value = Mock()
            mock_value.value = str(20 + (i % 20))  # Ages 20-39
            mock_values.append(mock_value)

        mock_result = Mock()
        mock_result.scalars.return_value.all.return_value = mock_values
        mock_session.execute.return_value = mock_result

        # Mock baseline storage
        with patch.object(feature_monitor, "_store_baseline_profile") as mock_store:
            mock_store.return_value = None

            # Execute
            start_time = datetime.utcnow() - timedelta(days=7)
            end_time = datetime.utcnow()

            baseline = await feature_monitor.create_baseline(
                "user_age", start_time, end_time
            )

        # Verify
        assert baseline.count == 100
        assert baseline.mean is not None
        assert baseline.std is not None
        assert baseline.min_val is not None
        assert baseline.max_val is not None
        assert len(baseline.percentiles) > 0
        mock_store.assert_called_once()

    @pytest.mark.asyncio
    async def test_create_baseline_categorical(
        self, feature_monitor, mock_feature_store_service
    ):
        """Test creating baseline for categorical feature."""
        # Mock feature definition for categorical feature
        feature_def = Mock()
        feature_def.name = "user_category"
        feature_def.feature_type = FeatureType.STRING
        mock_feature_store_service.get_feature.return_value = feature_def

        # Mock session and feature values
        mock_session = AsyncMock()
        mock_feature_store_service.db_manager.get_session.return_value.__aenter__.return_value = mock_session

        # Mock categorical values
        mock_values = []
        categories = ["A", "B", "C", "A", "B", "A"]
        for cat in categories:
            mock_value = Mock()
            mock_value.value = f'"{cat}"'  # JSON string
            mock_values.append(mock_value)

        mock_result = Mock()
        mock_result.scalars.return_value.all.return_value = mock_values
        mock_session.execute.return_value = mock_result

        # Mock baseline storage
        with patch.object(feature_monitor, "_store_baseline_profile") as mock_store:
            mock_store.return_value = None

            # Execute
            start_time = datetime.utcnow() - timedelta(days=7)
            end_time = datetime.utcnow()

            baseline = await feature_monitor.create_baseline(
                "user_category", start_time, end_time
            )

        # Verify
        assert baseline.count == 6
        assert baseline.value_counts is not None
        assert baseline.value_counts["A"] == 3
        assert baseline.value_counts["B"] == 2
        assert baseline.value_counts["C"] == 1
        assert baseline.unique_values == 3

    @pytest.mark.asyncio
    async def test_create_baseline_no_data(
        self, feature_monitor, mock_feature_store_service, sample_feature_def
    ):
        """Test creating baseline with no data."""
        # Mock feature lookup
        mock_feature_store_service.get_feature.return_value = sample_feature_def

        # Mock session with no feature values
        mock_session = AsyncMock()
        mock_feature_store_service.db_manager.get_session.return_value.__aenter__.return_value = mock_session

        mock_result = Mock()
        mock_result.scalars.return_value.all.return_value = []
        mock_session.execute.return_value = mock_result

        # Execute and verify exception
        start_time = datetime.utcnow() - timedelta(days=7)
        end_time = datetime.utcnow()

        with pytest.raises(ValueError, match="No feature values found"):
            await feature_monitor.create_baseline("user_age", start_time, end_time)

    @pytest.mark.asyncio
    async def test_detect_drift_numerical(
        self, feature_monitor, mock_feature_store_service, sample_baseline_stats
    ):
        """Test drift detection for numerical features."""
        # Mock baseline profile
        mock_profile = Mock()
        mock_profile.baseline_stats = sample_baseline_stats.model_dump_json()
        mock_profile.monitoring_config = MonitoringConfig().model_dump_json()
        mock_profile.created_at = datetime.utcnow()
        mock_profile.last_updated = datetime.utcnow()

        with patch.object(feature_monitor, "_get_baseline_profile") as mock_get_profile:
            mock_get_profile.return_value = mock_profile

            # Mock current period baseline creation
            current_stats = BaselineStats(
                count=500,
                mean=30.0,  # Shifted mean
                std=6.0,  # Changed std
                min_val=20.0,
                max_val=70.0,
                percentiles={"p25": 25.0, "p50": 30.0, "p75": 35.0},
                null_percentage=3.0,
                unique_values=40,
            )

            with patch.object(
                feature_monitor, "create_baseline"
            ) as mock_create_baseline:
                mock_create_baseline.return_value = current_stats

                with patch.object(
                    feature_monitor, "_store_drift_detection"
                ) as mock_store_drift:
                    mock_store_drift.return_value = None

                    # Execute
                    start_time = datetime.utcnow() - timedelta(hours=24)
                    end_time = datetime.utcnow()

                    result = await feature_monitor.detect_drift(
                        "user_age", start_time, end_time
                    )

        # Verify
        assert result.feature_name == "user_age"
        assert result.drift_score > 0  # Should detect some drift
        assert result.drift_status in [
            DriftStatus.LOW_DRIFT,
            DriftStatus.MEDIUM_DRIFT,
            DriftStatus.HIGH_DRIFT,
        ]
        assert result.detection_method == "PSI_numerical"
        assert "baseline_mean" in result.details
        assert "current_mean" in result.details

    @pytest.mark.asyncio
    async def test_detect_drift_categorical(
        self, feature_monitor, mock_feature_store_service
    ):
        """Test drift detection for categorical features."""
        # Mock baseline profile with categorical stats
        baseline_stats = BaselineStats(
            count=1000,
            value_counts={"A": 500, "B": 300, "C": 200},
            null_percentage=0.0,
            unique_values=3,
        )

        mock_profile = Mock()
        mock_profile.baseline_stats = baseline_stats.model_dump_json()
        mock_profile.monitoring_config = MonitoringConfig().model_dump_json()
        mock_profile.created_at = datetime.utcnow()
        mock_profile.last_updated = datetime.utcnow()

        with patch.object(feature_monitor, "_get_baseline_profile") as mock_get_profile:
            mock_get_profile.return_value = mock_profile

            # Mock current period with shifted distribution
            current_stats = BaselineStats(
                count=500,
                value_counts={
                    "A": 200,
                    "B": 200,
                    "C": 100,
                },  # More balanced distribution
                null_percentage=0.0,
                unique_values=3,
            )

            with patch.object(
                feature_monitor, "create_baseline"
            ) as mock_create_baseline:
                mock_create_baseline.return_value = current_stats

                with patch.object(
                    feature_monitor, "_store_drift_detection"
                ) as mock_store_drift:
                    mock_store_drift.return_value = None

                    # Execute
                    start_time = datetime.utcnow() - timedelta(hours=24)
                    end_time = datetime.utcnow()

                    result = await feature_monitor.detect_drift(
                        "user_category", start_time, end_time
                    )

        # Verify
        assert result.feature_name == "user_category"
        assert result.drift_score > 0
        assert result.detection_method == "PSI_categorical"

    @pytest.mark.asyncio
    async def test_detect_drift_no_baseline(self, feature_monitor):
        """Test drift detection without baseline."""
        with patch.object(feature_monitor, "_get_baseline_profile") as mock_get_profile:
            mock_get_profile.return_value = None

            start_time = datetime.utcnow() - timedelta(hours=24)
            end_time = datetime.utcnow()

            # Execute and verify exception
            with pytest.raises(ValueError, match="No baseline profile found"):
                await feature_monitor.detect_drift("user_age", start_time, end_time)

    def test_calculate_psi_numerical(self, feature_monitor, sample_baseline_stats):
        """Test PSI calculation for numerical features."""
        # Current stats with shifted mean
        current_stats = BaselineStats(
            count=500,
            mean=30.0,  # Shifted from 25.5
            std=6.0,  # Changed from 5.2
            min_val=20.0,
            max_val=70.0,
            percentiles={},
            null_percentage=0.0,
            unique_values=40,
        )

        psi_score = feature_monitor._calculate_psi_numerical(
            sample_baseline_stats, current_stats
        )

        assert 0.0 <= psi_score <= 1.0
        assert psi_score > 0  # Should detect some drift due to mean/std changes

    def test_calculate_psi_categorical(self, feature_monitor):
        """Test PSI calculation for categorical features."""
        baseline_stats = BaselineStats(
            count=1000,
            value_counts={"A": 500, "B": 300, "C": 200},
            null_percentage=0.0,
            unique_values=3,
        )

        # Current stats with different distribution
        current_stats = BaselineStats(
            count=500,
            value_counts={"A": 200, "B": 200, "C": 100},
            null_percentage=0.0,
            unique_values=3,
        )

        psi_score = feature_monitor._calculate_psi_categorical(
            baseline_stats, current_stats
        )

        assert 0.0 <= psi_score <= 1.0
        assert psi_score > 0  # Should detect drift due to distribution changes

    def test_determine_drift_status(self, feature_monitor, sample_monitoring_config):
        """Test drift status determination."""
        # Test no drift
        status = feature_monitor._determine_drift_status(0.05, sample_monitoring_config)
        assert status == DriftStatus.NO_DRIFT

        # Test low drift
        status = feature_monitor._determine_drift_status(0.2, sample_monitoring_config)
        assert status == DriftStatus.LOW_DRIFT

        # Test medium drift
        status = feature_monitor._determine_drift_status(0.4, sample_monitoring_config)
        assert status == DriftStatus.MEDIUM_DRIFT

        # Test high drift
        status = feature_monitor._determine_drift_status(0.8, sample_monitoring_config)
        assert status == DriftStatus.HIGH_DRIFT

    @pytest.mark.asyncio
    async def test_create_alert(self, feature_monitor, mock_feature_store_service):
        """Test creating an alert."""
        # Mock session
        mock_session = AsyncMock()
        mock_feature_store_service.db_manager.get_session.return_value.__aenter__.return_value = mock_session

        # Mock alert record
        mock_alert = Mock()
        mock_alert.id = 1
        mock_session.refresh = AsyncMock()

        # Execute
        await feature_monitor.create_alert(
            feature_name="user_age",
            alert_type="drift_detection",
            alert_level=AlertLevel.WARNING,
            message="Drift detected",
            alert_data={"drift_score": 0.4},
        )

        # Verify
        mock_session.add.assert_called_once()
        mock_session.commit.assert_called_once()
        mock_session.refresh.assert_called_once()

    @pytest.mark.asyncio
    async def test_get_alerts(self, feature_monitor, mock_feature_store_service):
        """Test getting alerts."""
        # Mock session
        mock_session = AsyncMock()
        mock_feature_store_service.db_manager.get_session.return_value.__aenter__.return_value = mock_session

        # Mock alert records
        mock_alert = Mock()
        mock_alert.id = 1
        mock_alert.feature_name = "user_age"
        mock_alert.alert_type = "drift_detection"
        mock_alert.alert_level = AlertLevel.WARNING.value
        mock_alert.message = "Drift detected"
        mock_alert.alert_data = '{"drift_score": 0.4}'
        mock_alert.acknowledged = "false"
        mock_alert.acknowledged_by = None
        mock_alert.acknowledged_at = None
        mock_alert.created_at = datetime.utcnow()

        mock_result = Mock()
        mock_result.scalars.return_value.all.return_value = [mock_alert]
        mock_session.execute.return_value = mock_result

        # Execute
        alerts = await feature_monitor.get_alerts(
            feature_name="user_age", alert_level=AlertLevel.WARNING
        )

        # Verify
        assert len(alerts) == 1
        assert alerts[0].feature_name == "user_age"
        assert alerts[0].alert_level == AlertLevel.WARNING
        assert alerts[0].acknowledged is False

    @pytest.mark.asyncio
    async def test_acknowledge_alert(self, feature_monitor, mock_feature_store_service):
        """Test acknowledging an alert."""
        # Mock session
        mock_session = AsyncMock()
        mock_feature_store_service.db_manager.get_session.return_value.__aenter__.return_value = mock_session

        # Mock alert record
        mock_alert = Mock()
        mock_alert.acknowledged = "false"
        mock_alert.acknowledged_by = None
        mock_alert.acknowledged_at = None
        mock_session.get.return_value = mock_alert

        # Execute
        result = await feature_monitor.acknowledge_alert(1, "admin")

        # Verify
        assert result is True
        assert mock_alert.acknowledged == "true"
        assert mock_alert.acknowledged_by == "admin"
        assert mock_alert.acknowledged_at is not None
        mock_session.commit.assert_called_once()

    @pytest.mark.asyncio
    async def test_acknowledge_alert_not_found(
        self, feature_monitor, mock_feature_store_service
    ):
        """Test acknowledging non-existent alert."""
        # Mock session
        mock_session = AsyncMock()
        mock_feature_store_service.db_manager.get_session.return_value.__aenter__.return_value = mock_session
        mock_session.get.return_value = None

        # Execute
        result = await feature_monitor.acknowledge_alert(999, "admin")

        # Verify
        assert result is False
