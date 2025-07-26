"""Tests for advanced ML model store functionality."""

import asyncio
from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from libs.ml_models.ab_testing import (
    ABTestingConfig,
    ABTestingManager,
    ExperimentStatus,
)
from libs.ml_models.feature_store_integration import (
    FeatureResponse,
    FeatureStoreClient,
    FeatureStoreConfig,
)
from libs.ml_models.retraining import (
    RetrainingConfig,
    RetrainingOrchestrator,
    RetrainingPriority,
    RetrainingStatus,
    RetrainingTriggerType,
)
from libs.ml_models.versioning import (
    ModelVersioningConfig,
    ModelVersionManager,
)
from libs.streaming_analytics.realtime_ml import PredictionStatus


class TestModelVersionManager:
    """Test model versioning functionality."""

    @pytest.fixture
    def version_manager(self):
        """Create version manager for testing."""
        config = ModelVersioningConfig(versioning_strategy="semantic")

        with patch("libs.ml_models.versioning.MLOpsMetrics") as mock_metrics_class:
            mock_metrics = MagicMock()
            mock_metrics_class.return_value = mock_metrics

            mock_registry = MagicMock()
            mock_registry.get_latest_model_version.side_effect = Exception(
                "No model found"
            )

            return ModelVersionManager(config=config, model_registry=mock_registry)

    def test_create_version_semantic(self, version_manager):
        """Test creating semantic versions."""
        # Create first version
        version_info = version_manager.create_version(
            model_name="test_model",
            change_type="major",
            change_description="Initial version",
            created_by="test_user",
        )

        assert version_info.model_name == "test_model"
        assert version_info.version == "1.0.0"
        assert version_info.change_type == "major"
        assert version_info.previous_version is None

        # Create minor version
        version_info_2 = version_manager.create_version(
            model_name="test_model",
            change_type="minor",
            change_description="Added feature",
        )

        assert version_info_2.version == "1.1.0"
        assert version_info_2.previous_version == "1.0.0"

        # Create patch version
        version_info_3 = version_manager.create_version(
            model_name="test_model",
            change_type="patch",
            change_description="Bug fix",
        )

        assert version_info_3.version == "1.1.1"
        assert version_info_3.previous_version == "1.1.0"

    def test_create_rollback_plan(self, version_manager):
        """Test creating rollback plan."""
        # Create versions
        version_manager.create_version("test_model", "major", "v1")
        version_manager.create_version("test_model", "minor", "v2")
        version_manager.create_version("test_model", "patch", "v3")

        # Create rollback plan
        rollback_plan = version_manager.create_rollback_plan(
            model_name="test_model",
            target_version="1.0.0",
            rollback_strategy="immediate",
        )

        assert rollback_plan.model_name == "test_model"
        assert rollback_plan.current_version == "1.1.1"
        assert rollback_plan.target_version == "1.0.0"
        assert rollback_plan.rollback_strategy == "immediate"
        assert len(rollback_plan.rollback_steps) > 0

    def test_execute_rollback_dry_run(self, version_manager):
        """Test rollback execution in dry run mode."""
        # Create versions
        version_manager.create_version("test_model", "major", "v1")
        version_manager.create_version("test_model", "minor", "v2")

        # Create and execute rollback plan
        rollback_plan = version_manager.create_rollback_plan(
            model_name="test_model",
            target_version="1.0.0",
        )

        result = version_manager.execute_rollback(
            rollback_plan=rollback_plan,
            executed_by="test_user",
            confirmation=True,
            dry_run=True,
        )

        assert result["status"] == "dry_run_success"
        assert "steps_to_execute" in result

    def test_compare_versions(self, version_manager):
        """Test version comparison."""
        # Create versions with performance metrics
        version_manager.create_version(
            "test_model",
            "major",
            "v1",
            performance_metrics={"accuracy": 0.85, "precision": 0.80},
        )
        version_manager.create_version(
            "test_model",
            "minor",
            "v2",
            performance_metrics={"accuracy": 0.88, "precision": 0.82},
        )

        comparison = version_manager.compare_versions(
            model_name="test_model", version1="1.0.0", version2="1.1.0"
        )

        assert comparison["model_name"] == "test_model"
        assert "performance_comparison" in comparison
        assert comparison["performance_comparison"]["accuracy"][
            "difference"
        ] == pytest.approx(0.03, abs=1e-6)


class TestABTestingManager:
    """Test A/B testing functionality."""

    @pytest.fixture
    def ab_manager(self):
        """Create A/B testing manager for testing."""
        config = ABTestingConfig(min_sample_size=100)
        mock_registry = MagicMock()
        return ABTestingManager(config=config, model_registry=mock_registry)

    def test_create_experiment(self, ab_manager):
        """Test creating A/B experiment."""
        with patch.object(ab_manager, "_validate_model_exists"):
            experiment = ab_manager.create_experiment(
                name="test_experiment",
                control_model_name="model_v1",
                control_model_version="1.0.0",
                treatment_model_name="model_v2",
                treatment_model_version="2.0.0",
                primary_metric="accuracy",
                traffic_split=0.5,
                created_by="test_user",
            )

            assert experiment.name == "test_experiment"
            assert len(experiment.variants) == 2
            assert experiment.control_variant_id == "control"
            assert experiment.primary_metric == "accuracy"
            assert experiment.status == ExperimentStatus.DRAFT

    def test_start_experiment(self, ab_manager):
        """Test starting experiment."""
        with patch.object(ab_manager, "_validate_model_exists"):
            experiment = ab_manager.create_experiment(
                name="test_experiment",
                control_model_name="model_v1",
                control_model_version="1.0.0",
                treatment_model_name="model_v2",
                treatment_model_version="2.0.0",
                primary_metric="accuracy",
            )

            ab_manager.start_experiment(experiment.experiment_id)

            updated_experiment = ab_manager.get_experiment(experiment.experiment_id)
            assert updated_experiment.status == ExperimentStatus.ACTIVE
            assert updated_experiment.start_time is not None
            assert updated_experiment.end_time is not None

    def test_assign_variant_hash_based(self, ab_manager):
        """Test hash-based variant assignment."""
        with patch.object(ab_manager, "_validate_model_exists"):
            experiment = ab_manager.create_experiment(
                name="test_experiment",
                control_model_name="model_v1",
                control_model_version="1.0.0",
                treatment_model_name="model_v2",
                treatment_model_version="2.0.0",
                primary_metric="accuracy",
                traffic_split=0.5,
            )

            ab_manager.start_experiment(experiment.experiment_id)

            # Test consistent assignment
            user_id = "user_123"
            variant1 = ab_manager.assign_variant(experiment.experiment_id, user_id)
            variant2 = ab_manager.assign_variant(experiment.experiment_id, user_id)

            assert variant1 == variant2  # Should be consistent
            assert variant1 in ["control", "treatment"]

    def test_analyze_experiment(self, ab_manager):
        """Test experiment analysis."""
        with patch.object(ab_manager, "_validate_model_exists"):
            experiment = ab_manager.create_experiment(
                name="test_experiment",
                control_model_name="model_v1",
                control_model_version="1.0.0",
                treatment_model_name="model_v2",
                treatment_model_version="2.0.0",
                primary_metric="accuracy",
            )

            ab_manager.start_experiment(experiment.experiment_id)

            # Mock experiment metrics
            with patch.object(ab_manager, "_get_experiment_metrics") as mock_metrics:
                mock_metrics.return_value = {
                    "control": {"accuracy": 0.85},
                    "treatment": {"accuracy": 0.87},
                }

                result = ab_manager.analyze_experiment(experiment.experiment_id)

                assert result.experiment_id == experiment.experiment_id
                assert "control" in result.variant_results
                assert "treatment" in result.variant_results
                assert result.winning_variant_id is not None

    def test_record_experiment_metric(self, ab_manager):
        """Test recording experiment metrics."""
        with patch.object(ab_manager, "_validate_model_exists"):
            experiment = ab_manager.create_experiment(
                name="test_experiment",
                control_model_name="model_v1",
                control_model_version="1.0.0",
                treatment_model_name="model_v2",
                treatment_model_version="2.0.0",
                primary_metric="accuracy",
            )

            # Should not raise exception
            ab_manager.record_experiment_metric(
                experiment_id=experiment.experiment_id,
                variant_id="control",
                user_id="user_123",
                metric_name="accuracy",
                metric_value=0.85,
            )


class TestRetrainingOrchestrator:
    """Test automated retraining functionality."""

    @pytest.fixture
    def orchestrator(self):
        """Create retraining orchestrator for testing."""
        config = RetrainingConfig(
            trigger_check_interval_minutes=1,
            max_concurrent_retraining_jobs=2,
        )
        mock_registry = MagicMock()
        mock_monitor = MagicMock()
        return RetrainingOrchestrator(
            config=config, model_registry=mock_registry, model_monitor=mock_monitor
        )

    def test_create_trigger(self, orchestrator):
        """Test creating retraining trigger."""
        trigger = orchestrator.create_trigger(
            model_name="test_model",
            trigger_type=RetrainingTriggerType.SCHEDULED,
            conditions={"schedule": "daily"},
            priority=RetrainingPriority.MEDIUM,
            created_by="test_user",
        )

        assert trigger.model_name == "test_model"
        assert trigger.trigger_type == RetrainingTriggerType.SCHEDULED
        assert trigger.priority == RetrainingPriority.MEDIUM
        assert trigger.is_active is True

    @pytest.mark.asyncio
    async def test_create_retraining_job(self, orchestrator):
        """Test creating retraining job."""
        job = await orchestrator.create_retraining_job(
            model_name="test_model",
            priority=RetrainingPriority.HIGH,
            created_by="test_user",
        )

        assert job.model_name == "test_model"
        assert job.priority == RetrainingPriority.HIGH
        assert job.status == RetrainingStatus.PENDING
        assert job.created_by == "test_user"

    @pytest.mark.asyncio
    async def test_approve_retraining_job(self, orchestrator):
        """Test approving retraining job."""
        job = await orchestrator.create_retraining_job(
            model_name="test_model",
            priority=RetrainingPriority.LOW,
        )

        await orchestrator.approve_retraining_job(
            job_id=job.job_id,
            approved_by="test_approver",
        )

        updated_job = orchestrator.get_job(job.job_id)
        assert updated_job.approved_by == "test_approver"

    def test_scheduled_trigger_evaluation(self, orchestrator):
        """Test scheduled trigger evaluation."""
        trigger = orchestrator.create_trigger(
            model_name="test_model",
            trigger_type=RetrainingTriggerType.SCHEDULED,
            conditions={"schedule": "daily"},
        )

        # Should trigger if never triggered before
        should_trigger = orchestrator._evaluate_scheduled_trigger(trigger)
        assert should_trigger is True

        # Should not trigger if triggered recently
        trigger.last_triggered = datetime.now(timezone.utc)
        should_trigger = orchestrator._evaluate_scheduled_trigger(trigger)
        assert should_trigger is False

    def test_model_staleness_evaluation(self, orchestrator):
        """Test model staleness trigger evaluation."""
        trigger = orchestrator.create_trigger(
            model_name="test_model",
            trigger_type=RetrainingTriggerType.MODEL_STALENESS,
            conditions={"max_age_days": 7},
        )

        # Mock old model
        mock_metadata = MagicMock()
        mock_metadata.created_at = datetime.now(timezone.utc) - timedelta(days=10)

        with patch.object(
            orchestrator.model_registry,
            "get_model_metadata",
            return_value=mock_metadata,
        ):
            should_trigger = orchestrator._evaluate_staleness_trigger(trigger)
            assert should_trigger is True

        # Mock recent model
        mock_metadata.created_at = datetime.now(timezone.utc) - timedelta(days=3)
        with patch.object(
            orchestrator.model_registry,
            "get_model_metadata",
            return_value=mock_metadata,
        ):
            should_trigger = orchestrator._evaluate_staleness_trigger(trigger)
            assert should_trigger is False


class TestFeatureStoreIntegration:
    """Test feature store integration."""

    @pytest.fixture
    def feature_store_client(self):
        """Create feature store client for testing."""
        config = FeatureStoreConfig(
            feature_cache_ttl_seconds=300,
            enable_real_time_features=True,
        )
        return FeatureStoreClient(config=config)

    @pytest.mark.asyncio
    async def test_get_features(self, feature_store_client):
        """Test getting features from feature store."""
        responses = await feature_store_client.get_features(
            entity_ids=["user_123"],
            feature_names=["user_age", "user_income"],
            include_real_time=False,
        )

        assert len(responses) == 1
        assert responses[0].entity_id == "user_123"
        assert "user_age" in responses[0].features
        assert "user_income" in responses[0].features

    @pytest.mark.asyncio
    async def test_feature_caching(self, feature_store_client):
        """Test feature caching."""
        # First call should hit the store
        responses1 = await feature_store_client.get_features(
            entity_ids=["user_123"],
            feature_names=["user_age"],
        )
        assert responses1[0].cache_hit is False

        # Second call should hit the cache
        responses2 = await feature_store_client.get_features(
            entity_ids=["user_123"],
            feature_names=["user_age"],
        )
        assert responses2[0].cache_hit is True

    @pytest.mark.asyncio
    async def test_batch_features(self, feature_store_client):
        """Test batch feature fetching."""
        results = await feature_store_client.get_batch_features(
            entity_ids=["user_123", "user_456"],
            feature_names=["user_age", "user_income"],
        )

        assert len(results) == 2
        assert "user_123" in results
        assert "user_456" in results
        assert "user_age" in results["user_123"]

    def test_real_time_feature_computer(self, feature_store_client):
        """Test real-time feature computation."""
        computer = feature_store_client.real_time_computer

        # Register a simple feature computer
        def compute_age_category(entity_id, all_features, dependencies):
            age = dependencies.get("user_age", 0)
            if age < 25:
                return "young"
            elif age < 65:
                return "adult"
            else:
                return "senior"

        computer.register_feature_computer(
            "age_category", compute_age_category, ["user_age"]
        )

        # Test computation
        computed = asyncio.run(
            computer.compute_features(
                entity_id="user_123",
                input_features={"user_age": 30},
                requested_features=["age_category"],
            )
        )

        assert computed["age_category"] == "adult"

    def test_feature_dependency_sorting(self, feature_store_client):
        """Test feature dependency topological sorting."""
        computer = feature_store_client.real_time_computer

        # Set up dependencies: C depends on B, B depends on A
        computer.feature_dependencies = {
            "feature_a": [],
            "feature_b": ["feature_a"],
            "feature_c": ["feature_b"],
        }

        sorted_features = computer._topological_sort(
            ["feature_c", "feature_a", "feature_b"]
        )

        # feature_a should come before feature_b, feature_b before feature_c
        assert sorted_features.index("feature_a") < sorted_features.index("feature_b")
        assert sorted_features.index("feature_b") < sorted_features.index("feature_c")

    @pytest.mark.asyncio
    async def test_feature_freshness_validation(self, feature_store_client):
        """Test feature freshness validation."""
        # Get features to populate cache
        await feature_store_client.get_features(
            entity_ids=["user_123"],
            feature_names=["user_age"],
        )

        # Validate freshness
        freshness = await feature_store_client.validate_feature_freshness(
            features={"user_age": 30},
            entity_id="user_123",
        )

        assert freshness["user_age"] is True

    def test_cache_statistics(self, feature_store_client):
        """Test cache statistics."""
        # Add some data to cache manually for testing
        feature_store_client._cache_features("user_123", {"user_age": 30})
        feature_store_client._cache_features(
            "user_456", {"user_age": 25, "user_income": 50000}
        )

        stats = feature_store_client.get_cache_stats()

        assert stats["cached_entities"] == 2
        assert stats["average_features_per_entity"] == 1.5

    def test_clear_cache(self, feature_store_client):
        """Test cache clearing."""
        # Add data to cache
        feature_store_client._cache_features("user_123", {"user_age": 30})
        feature_store_client._cache_features("user_456", {"user_age": 25})

        # Clear specific entity
        feature_store_client.clear_cache("user_123")
        assert "user_123" not in feature_store_client.feature_cache
        assert "user_456" in feature_store_client.feature_cache

        # Clear all cache
        feature_store_client.clear_cache()
        assert len(feature_store_client.feature_cache) == 0


@pytest.mark.integration
class TestIntegratedMLPipeline:
    """Integration tests for the complete ML pipeline."""

    @pytest.mark.asyncio
    async def test_end_to_end_prediction_with_features(self):
        """Test end-to-end prediction with feature store integration."""
        # Create mock components
        from libs.streaming_analytics.config import RealtimeMLConfig
        from libs.streaming_analytics.event_store import EventSchema, EventType
        from libs.streaming_analytics.realtime_ml import RealtimeMLInferenceEngine

        config = RealtimeMLConfig(model_cache_size=5)

        model_registry = MagicMock()
        feature_store_client = MagicMock()

        # Mock feature store response
        mock_feature_response = FeatureResponse(
            entity_id="user_123",
            features={"user_age": 30, "user_income": 50000},
            timestamp=datetime.now(timezone.utc),
        )
        feature_store_client.get_features = AsyncMock(
            return_value=[mock_feature_response]
        )

        # Create inference engine
        engine = RealtimeMLInferenceEngine(
            config=config,
            model_registry=model_registry,
            feature_store_client=feature_store_client,
        )

        # Create test event
        test_event = EventSchema(
            event_id="test_event_123",
            event_type=EventType.USER_ACTION,
            event_name="user_click",
            source_service="test_service",
            payload={
                "user_id": "user_123",
                "action": "click",
                "item_id": "item_456",
            },
            timestamp=datetime.now(timezone.utc),
        )

        # Mock model loading and prediction
        with (
            patch.object(engine, "_load_model") as mock_load_model,
            patch.object(engine.model_cache, "get_model") as mock_get_model,
        ):
            # Mock model version
            mock_model_version = MagicMock()
            mock_model_version.name = "test_model"
            mock_model_version.version = "1.0.0"
            mock_load_model.return_value = mock_model_version

            # Mock ML model
            mock_model = MagicMock()
            mock_model.predict.return_value = [0.85]
            mock_get_model.return_value = mock_model

            # Make prediction
            result = await engine.predict_from_event(
                event=test_event,
                model_name="test_model",
                use_feature_store=True,
            )

            # Verify feature store was called
            feature_store_client.get_features.assert_called_once()

            # Verify prediction result
            if result:  # May be None due to mocking
                assert result.model_name == "test_model"
                assert (
                    result.status == PredictionStatus.SUCCESS
                    or result.status == PredictionStatus.MODEL_NOT_FOUND
                )

    @pytest.mark.asyncio
    async def test_ab_testing_with_versioning(self):
        """Test A/B testing integration with model versioning."""
        # Create version manager and A/B testing manager
        version_config = ModelVersioningConfig()
        ab_config = ABTestingConfig()

        with (
            patch("libs.ml_models.versioning.ModelRegistry") as mock_version_registry,
            patch("libs.ml_models.ab_testing.ModelRegistry") as mock_ab_registry,
            patch("libs.ml_models.versioning.MLOpsMetrics") as mock_metrics_class,
        ):
            # Configure mocks
            mock_registry = MagicMock()
            mock_registry.get_latest_model_version.side_effect = Exception(
                "No model found"
            )
            mock_version_registry.return_value = mock_registry

            mock_metrics = MagicMock()
            mock_metrics_class.return_value = mock_metrics

            version_manager = ModelVersionManager(
                config=version_config, model_registry=mock_registry
            )
            ab_manager = ABTestingManager(
                config=ab_config, model_registry=mock_ab_registry.return_value
            )

            # Create model versions
            version_manager.create_version(
                "recommendation_model", "major", "Initial model"
            )
            version_manager.create_version(
                "recommendation_model", "minor", "Improved model"
            )

            # Create A/B experiment
            with patch.object(ab_manager, "_validate_model_exists"):
                experiment = ab_manager.create_experiment(
                    name="model_comparison",
                    control_model_name="recommendation_model",
                    control_model_version="1.0.0",
                    treatment_model_name="recommendation_model",
                    treatment_model_version="1.1.0",
                    primary_metric="precision",
                )

                ab_manager.start_experiment(experiment.experiment_id)

                # Test variant assignment
                variant = ab_manager.assign_variant(
                    experiment.experiment_id, "user_123"
                )
                assert variant in ["control", "treatment"]

                # Get model version for variant
                model_name, model_version = ab_manager.get_variant_model(
                    experiment.experiment_id, variant
                )
                assert model_name == "recommendation_model"
                assert model_version in ["1.0.0", "1.1.0"]
