"""Comprehensive test suite for feature store functionality."""

import json
import tempfile
from datetime import datetime, timedelta, timezone
from unittest.mock import patch

import pandas as pd
import pytest

from libs.ml_models import (
    FeatureDefinition,
    FeatureSet,
    FeatureStore,
    FeatureStoreConfig,
    FeatureTransformation,
    FeatureValue,
)


@pytest.fixture
def feature_store_config():
    """Create a test feature store configuration."""
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        db_path = f.name

    return FeatureStoreConfig(
        database_url=f"sqlite:///{db_path}",
        online_serving=True,
        offline_serving=True,
        enable_feature_caching=True,
        cache_backend="memory",
        enable_drift_detection=True,
        enable_lineage_tracking=True,
        enable_feature_discovery=True,
    )


@pytest.fixture
def feature_store(feature_store_config):
    """Create a test feature store instance."""
    return FeatureStore(feature_store_config)


@pytest.fixture
def sample_feature_definition():
    """Create a sample feature definition."""
    return FeatureDefinition(
        name="user_age",
        description="Age of the user in years",
        feature_type="numerical",
        data_type="int",
        source_table="users",
        transformation_logic="SELECT age FROM users WHERE user_id = :entity_id",
        owner="test_user",
        tags=["demographic", "user"],
        validation_rules={
            "min_value": 0,
            "max_value": 150,
        },
        expected_update_frequency="daily",
    )


@pytest.fixture
def sample_feature_values():
    """Create sample feature values."""
    return [
        FeatureValue(
            feature_name="user_age",
            entity_id="user_1",
            value=25,
            event_timestamp=datetime.now(timezone.utc),
            source="user_service",
        ),
        FeatureValue(
            feature_name="user_age",
            entity_id="user_2",
            value=30,
            event_timestamp=datetime.now(timezone.utc),
            source="user_service",
        ),
        FeatureValue(
            feature_name="user_income",
            entity_id="user_1",
            value=50000,
            event_timestamp=datetime.now(timezone.utc),
            source="user_service",
        ),
    ]


@pytest.fixture
def sample_transformation():
    """Create a sample feature transformation."""
    return FeatureTransformation(
        name="user_demographics",
        description="Calculate user demographic features",
        input_features=["raw_age", "raw_income"],
        output_features=["user_age", "user_income"],
        transformation_code="""
# Calculate age in years
result = df.copy()
result['user_age'] = result['raw_age']
result['user_income'] = result['raw_income']
result = result[['user_age', 'user_income']]
""",
        transformation_type="python",
        owner="test_user",
    )


class TestFeatureStore:
    """Test cases for FeatureStore class."""

    def test_feature_store_initialization(self, feature_store_config):
        """Test feature store initialization."""
        store = FeatureStore(feature_store_config)

        assert store.config == feature_store_config
        assert store.engine is not None
        assert store.SessionLocal is not None
        assert store.lineage_tracker is not None
        assert isinstance(store.transformations, dict)

    def test_create_feature_definition(self, feature_store, sample_feature_definition):
        """Test creating a feature definition."""
        feature_id = feature_store.create_feature(sample_feature_definition)

        assert feature_id is not None
        assert isinstance(feature_id, str)

        # Verify feature was stored
        retrieved_feature = feature_store.get_feature_definition(
            sample_feature_definition.name
        )
        assert retrieved_feature is not None
        assert retrieved_feature.name == sample_feature_definition.name
        assert retrieved_feature.description == sample_feature_definition.description

    def test_duplicate_feature_definition(
        self, feature_store, sample_feature_definition
    ):
        """Test that creating duplicate feature definitions raises an error."""
        feature_store.create_feature(sample_feature_definition)

        with pytest.raises(ValueError, match="already exists"):
            feature_store.create_feature(sample_feature_definition)

    def test_write_and_read_features(
        self, feature_store, sample_feature_definition, sample_feature_values
    ):
        """Test writing and reading feature values."""
        # First create the feature definition
        feature_store.create_feature(sample_feature_definition)

        # Write feature values
        written_count = feature_store.write_features(sample_feature_values)
        assert written_count == len(sample_feature_values)

        # Read features
        feature_names = ["user_age"]
        entity_ids = ["user_1", "user_2"]

        result_df = feature_store.read_features(feature_names, entity_ids)

        assert len(result_df) == 2
        assert "entity_id" in result_df.columns
        assert "user_age" in result_df.columns
        assert (
            result_df.loc[result_df["entity_id"] == "user_1", "user_age"].iloc[0] == 25
        )
        assert (
            result_df.loc[result_df["entity_id"] == "user_2", "user_age"].iloc[0] == 30
        )

    def test_online_feature_serving(
        self, feature_store, sample_feature_definition, sample_feature_values
    ):
        """Test online feature serving."""
        feature_store.create_feature(sample_feature_definition)
        feature_store.write_features(sample_feature_values)

        # Test online feature serving
        result_df = feature_store.get_online_features(
            feature_names=["user_age"],
            entity_ids=["user_1"],
            timeout_ms=500,
        )

        assert len(result_df) == 1
        assert result_df.iloc[0]["user_age"] == 25

    def test_historical_features(self, feature_store, sample_feature_definition):
        """Test historical feature retrieval with point-in-time correctness."""
        feature_store.create_feature(sample_feature_definition)

        # Create historical feature values
        base_time = datetime.now(timezone.utc)
        historical_values = [
            FeatureValue(
                feature_name="user_age",
                entity_id="user_1",
                value=24,
                event_timestamp=base_time - timedelta(days=2),
                source="historical",
            ),
            FeatureValue(
                feature_name="user_age",
                entity_id="user_1",
                value=25,
                event_timestamp=base_time - timedelta(days=1),
                source="historical",
            ),
        ]

        feature_store.write_features(historical_values)

        # Test historical feature retrieval
        entity_timestamps = [
            ("user_1", base_time - timedelta(hours=12)),  # Should get age 25
        ]

        result_df = feature_store.get_historical_features(
            feature_names=["user_age"],
            entity_timestamps=entity_timestamps,
            point_in_time_accuracy=True,
        )

        assert len(result_df) == 1
        assert result_df.iloc[0]["user_age"] == 25

    def test_feature_transformation(self, feature_store, sample_transformation):
        """Test feature transformation registration and execution."""
        # Register transformation
        transformation_id = feature_store.register_transformation(sample_transformation)
        assert transformation_id is not None

        # Test transformation execution
        input_data = pd.DataFrame(
            {
                "raw_age": [25, 30, 35],
                "raw_income": [50000, 60000, 70000],
                "entity_id": ["user_1", "user_2", "user_3"],
            }
        )

        result_df = feature_store.execute_transformation(
            transformation_name=sample_transformation.name,
            input_data=input_data,
        )

        assert len(result_df) == 3
        assert "user_age" in result_df.columns
        assert "user_income" in result_df.columns
        assert result_df["user_age"].tolist() == [25, 30, 35]

    def test_feature_set_creation(self, feature_store, sample_feature_definition):
        """Test feature set creation."""
        # Create a feature first
        feature_store.create_feature(sample_feature_definition)

        feature_set = FeatureSet(
            name="user_demographics",
            description="User demographic features",
            features=["user_age"],
            owner="test_user",
            tags=["demographic"],
        )

        feature_set_id = feature_store.create_feature_set(feature_set)
        assert feature_set_id is not None

    def test_feature_set_with_missing_features(self, feature_store):
        """Test that feature set creation fails with missing features."""
        feature_set = FeatureSet(
            name="invalid_set",
            description="Set with missing features",
            features=["nonexistent_feature"],
            owner="test_user",
        )

        with pytest.raises(ValueError, match="Missing feature definitions"):
            feature_store.create_feature_set(feature_set)

    def test_feature_stats(self, feature_store, sample_feature_definition):
        """Test feature store statistics."""
        feature_store.create_feature(sample_feature_definition)

        stats = feature_store.get_feature_stats()

        assert "total_features" in stats
        assert "features_by_type" in stats
        assert "recent_features_7d" in stats
        assert stats["total_features"] >= 1

    def test_feature_discovery(self, feature_store, sample_feature_definition):
        """Test feature discovery information."""
        feature_store.create_feature(sample_feature_definition)

        discovery_info = feature_store.get_feature_discovery_info()

        assert "total_features" in discovery_info
        assert "features_by_type" in discovery_info
        assert "feature_store_health" in discovery_info
        assert discovery_info["total_features"] >= 1

    def test_batch_computation(self, feature_store, sample_transformation):
        """Test batch feature computation."""
        feature_store.register_transformation(sample_transformation)

        entity_df = pd.DataFrame(
            {
                "entity_id": ["user_1", "user_2"],
                "raw_age": [25, 30],
                "raw_income": [50000, 60000],
            }
        )

        start_date = datetime.now(timezone.utc) - timedelta(days=1)
        end_date = datetime.now(timezone.utc)

        processed_count = feature_store.batch_compute_features(
            transformation_name=sample_transformation.name,
            entity_df=entity_df,
            start_date=start_date,
            end_date=end_date,
        )

        assert processed_count > 0

    @patch("libs.ml_models.feature_store.ModelMonitor")
    def test_drift_detection(self, mock_monitor, feature_store):
        """Test feature drift detection."""
        # Mock the drift detector
        mock_monitor_instance = mock_monitor.return_value
        mock_monitor_instance.detect_data_drift.return_value = [
            type(
                "DriftResult",
                (),
                {
                    "feature_name": "test_feature",
                    "drift_detected": True,
                    "drift_magnitude": 0.3,
                    "p_value": 0.01,
                    "statistic": 0.3,
                    "detection_method": "ks_test",
                    "severity": "medium",
                    "recommended_action": "Monitor closely",
                    "reference_samples": 100,
                    "current_samples": 100,
                },
            )()
        ]

        reference_data = pd.DataFrame({"test_feature": [1, 2, 3, 4, 5]})
        current_data = pd.DataFrame({"test_feature": [10, 20, 30, 40, 50]})

        drift_result = feature_store.detect_feature_drift(
            feature_name="test_feature",
            reference_data=reference_data,
            current_data=current_data,
        )

        assert drift_result["feature_name"] == "test_feature"
        assert drift_result["drift_detected"] is True
        assert "drift_magnitude" in drift_result

    def test_feature_lineage(self, feature_store, sample_feature_definition):
        """Test feature lineage tracking."""
        feature_store.create_feature(sample_feature_definition)

        # Test lineage retrieval (will be empty since we haven't set up transformations)
        lineage_info = feature_store.get_feature_lineage(sample_feature_definition.name)

        assert "feature_name" in lineage_info
        assert lineage_info["feature_name"] == sample_feature_definition.name

    def test_feature_validation(self, feature_store, sample_feature_definition):
        """Test feature value validation."""
        feature_store.create_feature(sample_feature_definition)

        # Test with invalid values (too many nulls)
        invalid_values = [
            FeatureValue(
                feature_name="user_age",
                entity_id=f"user_{i}",
                value=None,
                event_timestamp=datetime.now(timezone.utc),
            )
            for i in range(20)  # 20 null values
        ]

        # Add one valid value
        invalid_values.append(
            FeatureValue(
                feature_name="user_age",
                entity_id="user_valid",
                value=25,
                event_timestamp=datetime.now(timezone.utc),
            )
        )

        # This should raise an error due to too many null values
        with pytest.raises(ValueError, match="exceeding threshold"):
            feature_store.write_features(invalid_values)

    def test_cleanup_old_values(
        self, feature_store, sample_feature_definition, sample_feature_values
    ):
        """Test cleanup of old feature values."""
        feature_store.create_feature(sample_feature_definition)
        feature_store.write_features(sample_feature_values)

        # Test cleanup (should not delete anything since values are recent)
        deleted_count = feature_store.cleanup_old_values(retention_days=1)
        assert deleted_count == 0

    def test_list_features_with_filters(self, feature_store):
        """Test listing features with various filters."""
        # Create multiple features
        features = [
            FeatureDefinition(
                name="age",
                description="User age",
                feature_type="numerical",
                data_type="int",
                owner="team_a",
                tags=["demographic"],
            ),
            FeatureDefinition(
                name="income",
                description="User income",
                feature_type="numerical",
                data_type="float",
                owner="team_b",
                tags=["financial"],
            ),
        ]

        for feature in features:
            feature_store.create_feature(feature)

        # Test filtering by type
        numerical_features = feature_store.list_features(feature_type="numerical")
        assert len(numerical_features) == 2

        # Test filtering by owner
        team_a_features = feature_store.list_features(owner="team_a")
        assert len(team_a_features) == 1
        assert team_a_features[0].name == "age"

        # Test filtering by tags
        demographic_features = feature_store.list_features(tags=["demographic"])
        assert len(demographic_features) == 1
        assert demographic_features[0].name == "age"

    def test_cache_functionality(
        self, feature_store, sample_feature_definition, sample_feature_values
    ):
        """Test feature caching functionality."""
        feature_store.create_feature(sample_feature_definition)
        feature_store.write_features(sample_feature_values)

        # First read should populate cache
        result1 = feature_store.read_features(["user_age"], ["user_1"])

        # Second read should hit cache (tested indirectly through performance)
        result2 = feature_store.read_features(["user_age"], ["user_1"])

        assert len(result1) == len(result2)
        assert result1.iloc[0]["user_age"] == result2.iloc[0]["user_age"]

    def test_sql_transformation_execution(self, feature_store):
        """Test SQL-based transformation execution."""
        sql_transformation = FeatureTransformation(
            name="sql_demo",
            description="SQL transformation demo",
            input_features=["raw_value"],
            output_features=["processed_value"],
            transformation_code="SELECT raw_value * 2 as processed_value FROM temp_sql_demo_*",
            transformation_type="sql",
            owner="test_user",
        )

        feature_store.register_transformation(sql_transformation)

        input_data = pd.DataFrame(
            {
                "raw_value": [1, 2, 3, 4, 5],
            }
        )

        result_df = feature_store.execute_transformation(
            transformation_name=sql_transformation.name,
            input_data=input_data,
        )

        assert "processed_value" in result_df.columns
        # Values should be doubled
        assert result_df["processed_value"].tolist() == [2, 4, 6, 8, 10]


class TestFeatureStoreIntegration:
    """Integration tests for feature store."""

    def test_end_to_end_ml_pipeline(self, feature_store):
        """Test a complete ML pipeline using the feature store."""
        # 1. Define features
        age_feature = FeatureDefinition(
            name="user_age",
            description="Age of the user",
            feature_type="numerical",
            data_type="int",
            owner="ml_team",
        )

        income_feature = FeatureDefinition(
            name="user_income",
            description="Income of the user",
            feature_type="numerical",
            data_type="float",
            owner="ml_team",
        )

        feature_store.create_feature(age_feature)
        feature_store.create_feature(income_feature)

        # 2. Create transformation
        transformation = FeatureTransformation(
            name="ml_features",
            description="ML model features",
            input_features=["user_age", "user_income"],
            output_features=["age_normalized", "income_log"],
            transformation_code="""
import numpy as np
result = df.copy()
result['age_normalized'] = (result['user_age'] - result['user_age'].mean()) / result['user_age'].std()
result['income_log'] = np.log(result['user_income'] + 1)
result = result[['age_normalized', 'income_log']]
""",
            transformation_type="python",
            owner="ml_team",
        )

        feature_store.register_transformation(transformation)

        # 3. Ingest raw data
        raw_features = [
            FeatureValue(
                feature_name="user_age",
                entity_id="user_1",
                value=25,
                event_timestamp=datetime.now(timezone.utc),
            ),
            FeatureValue(
                feature_name="user_income",
                entity_id="user_1",
                value=50000.0,
                event_timestamp=datetime.now(timezone.utc),
            ),
            FeatureValue(
                feature_name="user_age",
                entity_id="user_2",
                value=35,
                event_timestamp=datetime.now(timezone.utc),
            ),
            FeatureValue(
                feature_name="user_income",
                entity_id="user_2",
                value=75000.0,
                event_timestamp=datetime.now(timezone.utc),
            ),
        ]

        feature_store.write_features(raw_features)

        # 4. Read features for training
        training_data = feature_store.read_features(
            feature_names=["user_age", "user_income"],
            entity_ids=["user_1", "user_2"],
        )

        assert len(training_data) == 2

        # 5. Apply transformation
        transformed_data = feature_store.execute_transformation(
            transformation_name="ml_features",
            input_data=training_data,
        )

        assert "age_normalized" in transformed_data.columns
        assert "income_log" in transformed_data.columns

        # 6. Test online serving for inference
        online_features = feature_store.get_online_features(
            feature_names=["user_age", "user_income"],
            entity_ids=["user_1"],
        )

        assert len(online_features) == 1

    def test_feature_versioning_workflow(self, feature_store):
        """Test feature versioning and evolution workflow."""
        # Create initial feature version
        feature_v1 = FeatureDefinition(
            name="user_score_v1",
            description="User score version 1",
            feature_type="numerical",
            data_type="float",
            owner="ml_team",
            tags=["v1"],
        )

        feature_store.create_feature(feature_v1)

        # Create improved feature version
        feature_v2 = FeatureDefinition(
            name="user_score_v2",
            description="User score version 2 (improved algorithm)",
            feature_type="numerical",
            data_type="float",
            owner="ml_team",
            tags=["v2", "improved"],
        )

        feature_store.create_feature(feature_v2)

        # Write values for both versions
        values = [
            FeatureValue(
                feature_name="user_score_v1",
                entity_id="user_1",
                value=0.7,
                event_timestamp=datetime.now(timezone.utc),
            ),
            FeatureValue(
                feature_name="user_score_v2",
                entity_id="user_1",
                value=0.8,
                event_timestamp=datetime.now(timezone.utc),
            ),
        ]

        feature_store.write_features(values)

        # Test that both versions coexist
        v1_features = feature_store.list_features(tags=["v1"])
        v2_features = feature_store.list_features(tags=["v2"])

        assert len(v1_features) == 1
        assert len(v2_features) == 1
        assert v1_features[0].name == "user_score_v1"
        assert v2_features[0].name == "user_score_v2"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])

