"""Comprehensive tests for ML models library."""

import os
import tempfile
from datetime import datetime
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest
from sklearn.datasets import make_classification, make_regression
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import train_test_split

from libs.ml_models import (
    ExperimentConfig,
    ExperimentMetadata,
    ExperimentTracker,
    FeatureDefinition,
    FeatureStore,
    FeatureStoreConfig,
    FeatureValue,
    InferenceRequest,
    InferenceResponse,
    ModelMetadata,
    ModelMonitor,
    ModelMonitoringConfig,
    ModelRegistry,
    ModelRegistryConfig,
    ModelServer,
    ModelServingConfig,
    RunMetadata,
)


@pytest.fixture
def temp_db_path():
    """Create a temporary database path."""
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as tmp:
        path = tmp.name
    yield path
    # Cleanup
    if os.path.exists(path):
        os.unlink(path)


@pytest.fixture
def temp_mlflow_path():
    """Create temporary MLflow tracking directory."""
    with tempfile.TemporaryDirectory() as tmp_dir:
        yield tmp_dir


@pytest.fixture
def sample_model_data():
    """Generate sample model training data."""
    features, target = make_classification(
        n_samples=100, n_features=10, random_state=42
    )
    features_train, features_test, target_train, target_test = train_test_split(
        features, target, test_size=0.2, random_state=42
    )

    # Convert to DataFrame for easier handling
    feature_names = [f"feature_{i}" for i in range(features.shape[1])]
    features_train_df = pd.DataFrame(features_train, columns=feature_names)
    features_test_df = pd.DataFrame(features_test, columns=feature_names)

    return features_train_df, features_test_df, target_train, target_test, feature_names


@pytest.fixture
def sample_regression_data():
    """Generate sample regression data."""
    features, target = make_regression(
        n_samples=100, n_features=5, noise=0.1, random_state=42
    )
    features_train, features_test, target_train, target_test = train_test_split(
        features, target, test_size=0.2, random_state=42
    )

    feature_names = [f"reg_feature_{i}" for i in range(features.shape[1])]
    features_train_df = pd.DataFrame(features_train, columns=feature_names)
    features_test_df = pd.DataFrame(features_test, columns=feature_names)

    return features_train_df, features_test_df, target_train, target_test, feature_names


class TestModuleStructure:
    """Test basic module structure and imports."""

    def test_module_import(self):
        """Test that ml_models module can be imported."""
        import libs.ml_models as ml

        assert ml.__doc__ == "Machine learning model utilities and shared components."

    def test_all_exports(self):
        """Test that all expected components are exported."""
        import libs.ml_models as ml

        # Check main components are available
        assert hasattr(ml, "ModelRegistry")
        assert hasattr(ml, "ExperimentTracker")
        assert hasattr(ml, "ModelServer")
        assert hasattr(ml, "ModelMonitor")
        assert hasattr(ml, "FeatureStore")

        # Check config classes
        assert hasattr(ml, "ModelRegistryConfig")
        assert hasattr(ml, "ExperimentConfig")
        assert hasattr(ml, "ModelServingConfig")
        assert hasattr(ml, "ModelMonitoringConfig")
        assert hasattr(ml, "FeatureStoreConfig")


class TestModelRegistry:
    """Test model registry functionality."""

    def test_registry_initialization(self, temp_mlflow_path):
        """Test model registry initialization."""
        config = ModelRegistryConfig(
            mlflow_tracking_uri=f"sqlite:///{temp_mlflow_path}/mlflow.db",
            mlflow_artifact_root=str(temp_mlflow_path),
        )
        registry = ModelRegistry(config)

        assert registry.config == config
        assert registry.client is not None

    def test_model_metadata_creation(self):
        """Test creating model metadata."""
        metadata = ModelMetadata(
            name="test_model",
            version="1.0.0",
            description="Test model for unit tests",
            model_type="classification",
            algorithm="random_forest",
            framework="sklearn",
            metrics={"accuracy": 0.95, "f1": 0.92},
        )

        assert metadata.name == "test_model"
        assert metadata.version == "1.0.0"
        assert metadata.metrics["accuracy"] == 0.95
        assert isinstance(metadata.created_at, datetime)

    @patch("mlflow.start_run")
    @patch("mlflow.sklearn.log_model")
    def test_model_registration(
        self, mock_log_model, mock_start_run, temp_mlflow_path, sample_model_data
    ):
        """Test model registration process."""
        features_train, features_test, target_train, target_test, feature_names = (
            sample_model_data
        )

        # Train a simple model
        model = RandomForestClassifier(n_estimators=10, random_state=42)
        model.fit(features_train, target_train)

        # Mock MLflow run
        mock_run = MagicMock()
        mock_run.info.run_id = "test_run_id"
        mock_start_run.return_value.__enter__.return_value = mock_run

        config = ModelRegistryConfig(
            mlflow_tracking_uri=f"sqlite:///{temp_mlflow_path}/mlflow.db",
            require_input_example=False  # Disable for test
        )
        registry = ModelRegistry(config)

        metadata = ModelMetadata(
            name="test_classifier",
            version="1.0.0",
            model_type="classification",
            algorithm="random_forest",
            framework="sklearn",
            metrics={"accuracy": 0.95},
        )

        with patch.object(registry, "_ensure_experiment_exists", return_value="exp_1"):
            with patch.object(registry, "_register_model_version") as mock_register:
                mock_register.return_value = MagicMock(version="1")

                version = registry.register_model(model, metadata)

                assert version == "1"
                mock_start_run.assert_called_once()


class TestExperimentTracker:
    """Test experiment tracking functionality."""

    def test_tracker_initialization(self, temp_mlflow_path):
        """Test experiment tracker initialization."""
        config = ExperimentConfig(
            mlflow_tracking_uri=f"sqlite:///{temp_mlflow_path}/mlflow.db"
        )
        tracker = ExperimentTracker(config)

        assert tracker.config == config

    def test_experiment_metadata(self):
        """Test experiment metadata creation."""
        metadata = ExperimentMetadata(
            name="test_experiment",
            description="Test experiment",
            project="ml_platform",
            team="data_team",
            tags={"env": "test", "model_type": "classification"},
        )

        assert metadata.name == "test_experiment"
        assert metadata.tags["env"] == "test"
        assert isinstance(metadata.created_at, datetime)

    def test_run_metadata(self):
        """Test run metadata creation."""
        metadata = RunMetadata(
            run_name="test_run",
            description="Test run",
            experiment_id="exp_1",
            run_type="training",
            parameters={"n_estimators": 100, "max_depth": 10},
            metrics={"accuracy": 0.95},
        )

        assert metadata.run_name == "test_run"
        assert metadata.parameters["n_estimators"] == 100
        assert metadata.metrics["accuracy"] == 0.95

    @patch("mlflow.create_experiment")
    @patch("mlflow.get_experiment_by_name")
    def test_create_experiment(self, mock_get_exp, mock_create_exp, temp_mlflow_path):
        """Test experiment creation."""
        mock_get_exp.return_value = None
        mock_create_exp.return_value = "exp_123"

        config = ExperimentConfig(
            mlflow_tracking_uri=f"sqlite:///{temp_mlflow_path}/mlflow.db"
        )
        tracker = ExperimentTracker(config)

        metadata = ExperimentMetadata(name="new_experiment")
        experiment_id = tracker.create_experiment(metadata)

        assert experiment_id == "exp_123"
        mock_create_exp.assert_called_once()


class TestModelServer:
    """Test model serving functionality."""

    def test_server_initialization(self):
        """Test model server initialization."""
        config = ModelServingConfig(model_cache_size=5, inference_timeout_seconds=10.0)
        server = ModelServer(config)

        assert server.config == config
        assert len(server.model_cache) == 0

    def test_inference_request(self):
        """Test inference request creation."""
        request = InferenceRequest(
            model_name="test_model",
            model_version="1.0.0",
            inputs={"feature_1": 1.0, "feature_2": 2.0},
            return_probabilities=True,
        )

        assert request.model_name == "test_model"
        assert request.model_version == "1.0.0"
        assert request.return_probabilities is True

    def test_inference_response(self):
        """Test inference response creation."""
        response = InferenceResponse(
            predictions=[1, 0, 1],
            probabilities=[[0.9, 0.1], [0.3, 0.7], [0.8, 0.2]],
            model_name="test_model",
            model_version="1.0.0",
            model_stage=None,
            inference_time_ms=50.5,
            preprocessing_time_ms=10.0,
            postprocessing_time_ms=5.0,
            request_id="test_request_123",
        )

        assert len(response.predictions) == 3
        assert response.inference_time_ms == 50.5
        assert response.status == "success"

    @pytest.mark.asyncio
    async def test_server_lifecycle(self):
        """Test server start and stop."""
        config = ModelServingConfig(
            health_check_interval_seconds=0
        )  # Disable health checks
        server = ModelServer(config)

        await server.start()
        assert server._health_check_task is None  # Since interval is 0

        await server.stop()
        assert len(server.model_cache) == 0


class TestModelMonitor:
    """Test model monitoring functionality."""

    def test_monitor_initialization(self):
        """Test model monitor initialization."""
        config = ModelMonitoringConfig(drift_threshold=0.05, performance_threshold=0.1)
        monitor = ModelMonitor(config)

        assert monitor.config == config
        assert len(monitor.reference_data) == 0

    def test_drift_detection_result(self):
        """Test drift detection result creation."""
        from libs.ml_models.monitoring import DriftDetectionResult

        result = DriftDetectionResult(
            model_name="test_model",
            feature_name="feature_1",
            detection_method="ks_test",
            statistic=0.15,
            p_value=0.02,
            threshold=0.05,
            drift_detected=True,
            reference_samples=1000,
            current_samples=500,
            drift_magnitude=0.2,
            severity="medium",
        )

        assert result.drift_detected is True
        assert result.severity == "medium"
        assert result.p_value < result.threshold

    def test_set_reference_data(self, sample_model_data):
        """Test setting reference data."""
        features_train, _, _, _, _ = sample_model_data

        monitor = ModelMonitor()
        monitor.set_reference_data("test_model", features_train)

        assert "test_model" in monitor.reference_data
        assert len(monitor.reference_data["test_model"]) == len(features_train)



class TestFeatureStore:
    """Test feature store functionality."""

    def test_feature_store_initialization(self, temp_db_path):
        """Test feature store initialization."""
        config = FeatureStoreConfig(database_url=f"sqlite:///{temp_db_path}")
        store = FeatureStore(config)

        assert store.config == config
        assert store.engine is not None

    def test_feature_definition(self):
        """Test feature definition creation."""
        feature_def = FeatureDefinition(
            name="user_age",
            description="User age in years",
            feature_type="numerical",
            data_type="int",
            source_table="users",
            owner="data_team",
            tags=["demographics", "core_feature"],
        )

        assert feature_def.name == "user_age"
        assert feature_def.data_type == "int"
        assert "demographics" in feature_def.tags

    def test_feature_value(self):
        """Test feature value creation."""
        feature_value = FeatureValue(
            feature_name="user_age",
            entity_id="user_123",
            value=25,
            source="user_profile_service",
        )

        assert feature_value.feature_name == "user_age"
        assert feature_value.value == 25
        assert isinstance(feature_value.timestamp, datetime)

    def test_create_feature(self, temp_db_path):
        """Test creating a feature definition."""
        config = FeatureStoreConfig(database_url=f"sqlite:///{temp_db_path}")
        store = FeatureStore(config)

        feature_def = FeatureDefinition(
            name="test_feature",
            description="Test feature",
            feature_type="numerical",
            data_type="float",
            owner="test_team",
        )

        feature_id = store.create_feature(feature_def)
        assert feature_id is not None

        # Test retrieving the feature
        retrieved = store.get_feature_definition("test_feature")
        assert retrieved is not None
        assert retrieved.name == "test_feature"

    def test_write_and_read_features(self, temp_db_path):
        """Test writing and reading feature values."""
        config = FeatureStoreConfig(
            database_url=f"sqlite:///{temp_db_path}",
            enable_feature_validation=False,  # Disable for this test
        )
        store = FeatureStore(config)

        # Create feature definition first
        feature_def = FeatureDefinition(
            name="test_metric",
            description="Test metric",
            feature_type="numerical",
            data_type="float",
        )
        store.create_feature(feature_def)

        # Write feature values
        feature_values = [
            FeatureValue(
                feature_name="test_metric", entity_id=f"entity_{i}", value=float(i * 10)
            )
            for i in range(5)
        ]

        written_count = store.write_features(feature_values)
        assert written_count == 5

        # Read feature values
        entity_ids = [f"entity_{i}" for i in range(3)]
        df = store.read_features(["test_metric"], entity_ids)

        assert len(df) == 3
        assert "test_metric" in df.columns
        assert df.loc[0, "entity_id"] == "entity_0"


class TestIntegration:
    """Integration tests for MLOps components."""

    @pytest.mark.asyncio
    async def test_end_to_end_model_lifecycle(
        self, temp_mlflow_path, sample_model_data
    ):
        """Test complete model lifecycle from training to serving."""
        features_train, features_test, target_train, target_test, feature_names = (
            sample_model_data
        )

        # 1. Train model
        model = RandomForestClassifier(n_estimators=10, random_state=42)
        model.fit(features_train, target_train)
        accuracy = model.score(features_test, target_test)

        # 2. Create experiment and track training
        exp_config = ExperimentConfig(
            mlflow_tracking_uri=f"sqlite:///{temp_mlflow_path}/mlflow.db"
        )

        with (
            patch("mlflow.start_run"),
            patch("mlflow.create_experiment", return_value="test_exp_123"),
            patch("mlflow.get_experiment_by_name", return_value=None),
        ):
            tracker = ExperimentTracker(exp_config)

            exp_metadata = ExperimentMetadata(
                name="test_integration_experiment",
                description="Integration test experiment",
            )
            exp_id = tracker.create_experiment(exp_metadata)

            _run_metadata = RunMetadata(
                experiment_id=exp_id,
                run_name="training_run",
                parameters={"n_estimators": 10, "random_state": 42},
                metrics={"accuracy": accuracy},
            )

        # 3. Register model
        reg_config = ModelRegistryConfig(
            mlflow_tracking_uri=f"sqlite:///{temp_mlflow_path}/mlflow.db",
            require_input_example=False  # Disable for test
        )

        with (
            patch("mlflow.start_run") as mock_start_run,
            patch("mlflow.sklearn.log_model"),
            patch("mlflow.log_param"),
            patch("mlflow.log_metric"),
            patch("mlflow.set_tag"),
            patch.object(ModelRegistry, "_ensure_experiment_exists", return_value="test_exp_123"),
            patch.object(ModelRegistry, "_register_model_version") as mock_register,
        ):
            # Properly mock MLflow run
            mock_run = MagicMock()
            mock_run.info.run_id = "test_integration_run_id"
            mock_start_run.return_value.__enter__.return_value = mock_run

            mock_version = MagicMock()
            mock_version.version = "1.0.0"
            mock_register.return_value = mock_version

            registry = ModelRegistry(reg_config)

            model_metadata = ModelMetadata(
                name="integration_test_model",
                version="1.0.0",
                model_type="classification",
                algorithm="random_forest",
                framework="sklearn",
                metrics={"accuracy": accuracy},
            )

            version = registry.register_model(model, model_metadata)
            assert version is not None

        # 4. Test monitoring setup
        monitor = ModelMonitor()
        monitor.set_reference_data("integration_test_model", features_train)
        monitor.set_baseline_performance(
            "integration_test_model", {"accuracy": accuracy}
        )

        # Check that data was set
        assert "integration_test_model" in monitor.reference_data
        assert "integration_test_model" in monitor.baseline_performance

    def test_feature_store_to_model_pipeline(self, temp_db_path, sample_model_data):
        """Test pipeline from feature store to model training."""
        features_train, features_test, target_train, target_test, feature_names = (
            sample_model_data
        )

        # 1. Setup feature store
        config = FeatureStoreConfig(
            database_url=f"sqlite:///{temp_db_path}", enable_feature_validation=False
        )
        store = FeatureStore(config)

        # 2. Create feature definitions
        for feature_name in feature_names:
            feature_def = FeatureDefinition(
                name=feature_name,
                description=f"Feature {feature_name}",
                feature_type="numerical",
                data_type="float",
            )
            store.create_feature(feature_def)

        # 3. Write training features
        feature_values = []
        for idx, row in features_train.iterrows():
            entity_id = f"entity_{idx}"
            for feature_name in feature_names:
                feature_values.append(
                    FeatureValue(
                        feature_name=feature_name,
                        entity_id=entity_id,
                        value=float(row[feature_name]),
                    )
                )

        written_count = store.write_features(feature_values)
        assert written_count == len(feature_values)

        # 4. Read features for training
        entity_ids = [f"entity_{i}" for i in features_train.index[:5]]  # Get first 5
        df = store.read_features(feature_names, entity_ids)

        assert len(df) == 5
        assert all(col in df.columns for col in feature_names)

        # 5. Train model with features from store
        training_features = df[feature_names].values
        training_labels = target_train[:5]  # Corresponding labels

        model = RandomForestClassifier(n_estimators=10, random_state=42)
        model.fit(training_features, training_labels)

        # Model should be trained successfully
        assert hasattr(model, "feature_importances_")
        assert len(model.feature_importances_) == len(feature_names)


class TestErrorHandling:
    """Test error handling in MLOps components."""

    def test_model_registry_invalid_framework(self, temp_mlflow_path):
        """Test error handling for invalid model framework."""
        config = ModelRegistryConfig(
            mlflow_tracking_uri=f"sqlite:///{temp_mlflow_path}/mlflow.db",
            require_input_example=False  # Disable for test
        )
        registry = ModelRegistry(config)

        # Create invalid metadata
        metadata = ModelMetadata(
            name="invalid_model",
            version="1.0.0",
            model_type="classification",
            algorithm="unknown",
            framework="invalid_framework",
        )

        model = MagicMock()

        with (
            patch("mlflow.start_run") as mock_start_run,
            patch("mlflow.pyfunc.log_model"),
            patch("mlflow.log_param"),
            patch("mlflow.log_metric"),
            patch("mlflow.set_tag"),
            patch.object(registry, "_ensure_experiment_exists", return_value="test_exp_123"),
            patch.object(registry, "_register_model_version") as mock_register,
        ):
            # Properly mock MLflow run
            mock_run = MagicMock()
            mock_run.info.run_id = "test_error_run_id"
            mock_start_run.return_value.__enter__.return_value = mock_run

            mock_version = MagicMock()
            mock_version.version = "1.0.0"
            mock_register.return_value = mock_version

            # Should handle invalid framework gracefully by falling back to pyfunc
            version = registry.register_model(model, metadata)
            assert version is not None

    def test_feature_store_missing_feature(self, temp_db_path):
        """Test error handling when reading non-existent features."""
        config = FeatureStoreConfig(database_url=f"sqlite:///{temp_db_path}")
        store = FeatureStore(config)

        # Try to read non-existent feature
        df = store.read_features(["non_existent_feature"], ["entity_1"])

        # Based on implementation, returns empty DataFrame when no data exists
        # but still has correct column structure
        assert "entity_id" in df.columns
        assert "non_existent_feature" in df.columns

        # For this test, let's check that it handles missing features gracefully
        # by returning empty result instead of raising an error
        assert len(df) == 0 or (len(df) == 1 and pd.isna(df.loc[0, "non_existent_feature"]))

    def test_monitor_insufficient_samples(self, sample_model_data):
        """Test drift detection with insufficient samples."""
        features_train, _, _, _, _ = sample_model_data

        monitor = ModelMonitor(ModelMonitoringConfig(min_samples_for_drift=1000))
        monitor.set_reference_data("test_model", features_train)  # Only 80 samples

        # Create small current dataset
        small_data = features_train.head(10)

        drift_results = monitor.detect_data_drift("test_model", small_data)

        # Should return empty results due to insufficient samples
        assert len(drift_results) == 0

    def test_data_drift_detection(self, sample_model_data):
        """Test data drift detection."""
        features_train, features_test, _, _, _ = sample_model_data

        # Use lower threshold for test environment
        monitor = ModelMonitor(ModelMonitoringConfig(min_samples_for_drift=10))
        monitor.set_reference_data("test_model", features_train)

        # Add some drift to test data
        features_test_drift = features_test.copy()
        features_test_drift.iloc[:, 0] = (
            features_test_drift.iloc[:, 0] + 2.0
        )  # Add drift to first feature

        drift_results = monitor.detect_data_drift("test_model", features_test_drift)

        assert len(drift_results) > 0
        # Should detect drift in first feature
        feature_0_drift = [r for r in drift_results if r.feature_name == "feature_0"]
        assert len(feature_0_drift) > 0


if __name__ == "__main__":
    pytest.main([__file__])
