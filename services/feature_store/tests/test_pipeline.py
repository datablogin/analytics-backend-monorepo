"""Tests for feature pipeline orchestration."""

from datetime import datetime
from unittest.mock import AsyncMock, Mock, patch

import pandas as pd
import pytest
from services.feature_store.models import FeatureValueWrite
from services.feature_store.pipeline import (
    FeaturePipelineOrchestrator,
    PipelineConfig,
    PipelineStatus,
    PipelineType,
)


@pytest.fixture
def mock_feature_store_service():
    """Mock feature store service."""
    service = Mock()
    service.db_manager = Mock()
    service.db_manager.get_session = AsyncMock()
    service.write_feature_values_batch = AsyncMock()
    return service


@pytest.fixture
def pipeline_orchestrator(mock_feature_store_service):
    """Pipeline orchestrator with mocked dependencies."""
    return FeaturePipelineOrchestrator(mock_feature_store_service)


@pytest.fixture
def sample_pipeline_config():
    """Sample pipeline configuration."""
    return PipelineConfig(
        name="user_features_pipeline",
        pipeline_type=PipelineType.BATCH,
        source_config={"type": "csv", "file_path": "/tmp/users.csv"},
        feature_transformations=[
            {"feature_name": "user_age", "expression": "$age"},
            {"feature_name": "user_age_squared", "expression": "$age * $age"},
        ],
        batch_size=100,
    )


class TestFeaturePipelineOrchestrator:
    """Tests for FeaturePipelineOrchestrator."""

    @pytest.mark.asyncio
    async def test_register_pipeline(
        self, pipeline_orchestrator, sample_pipeline_config
    ):
        """Test registering a pipeline."""
        result = await pipeline_orchestrator.register_pipeline(sample_pipeline_config)

        assert result == sample_pipeline_config.name
        assert sample_pipeline_config.name in pipeline_orchestrator._pipeline_configs

    @pytest.mark.asyncio
    async def test_execute_batch_pipeline(
        self, pipeline_orchestrator, sample_pipeline_config, mock_feature_store_service
    ):
        """Test executing a batch pipeline."""
        # Register pipeline first
        await pipeline_orchestrator.register_pipeline(sample_pipeline_config)

        # Mock session
        mock_session = AsyncMock()
        mock_feature_store_service.db_manager.get_session.return_value.__aenter__.return_value = mock_session

        # Mock pipeline run model
        mock_run = Mock()
        mock_run.id = 1
        mock_session.refresh = AsyncMock()
        mock_session.refresh.return_value = None

        # Mock data loading
        sample_data = pd.DataFrame(
            {"entity_id": ["user_1", "user_2", "user_3"], "age": [25, 30, 35]}
        )

        with patch.object(
            pipeline_orchestrator, "_load_data_from_source"
        ) as mock_load_data:
            mock_load_data.return_value = sample_data

            with patch.object(
                pipeline_orchestrator, "_update_pipeline_run"
            ) as mock_update:
                mock_update.return_value = None

                # Execute
                result = await pipeline_orchestrator.execute_pipeline(
                    sample_pipeline_config.name
                )

        # Verify
        assert result.pipeline_name == sample_pipeline_config.name
        assert result.status == PipelineStatus.SUCCESS
        assert result.records_processed == 3
        assert result.features_processed == 6  # 3 records * 2 features

        # Verify feature values were written
        mock_feature_store_service.write_feature_values_batch.assert_called_once()

    @pytest.mark.asyncio
    async def test_execute_pipeline_not_found(self, pipeline_orchestrator):
        """Test executing a non-existent pipeline."""
        with pytest.raises(ValueError, match="Pipeline 'non_existent' not found"):
            await pipeline_orchestrator.execute_pipeline("non_existent")

    @pytest.mark.asyncio
    async def test_load_csv_data(self, pipeline_orchestrator):
        """Test loading data from CSV source."""
        source_config = {"type": "csv", "file_path": "/tmp/test.csv"}

        sample_data = pd.DataFrame({"col1": [1, 2], "col2": [3, 4]})

        with patch("pandas.read_csv") as mock_read_csv:
            mock_read_csv.return_value = sample_data

            result = await pipeline_orchestrator._load_data_from_source(source_config)

            assert len(result) == 2
            mock_read_csv.assert_called_once_with("/tmp/test.csv")

    @pytest.mark.asyncio
    async def test_load_unsupported_source(self, pipeline_orchestrator):
        """Test loading from unsupported source type."""
        source_config = {"type": "unsupported"}

        with pytest.raises(ValueError, match="Unsupported source type: unsupported"):
            await pipeline_orchestrator._load_data_from_source(source_config)

    @pytest.mark.asyncio
    async def test_apply_transformations(self, pipeline_orchestrator):
        """Test applying feature transformations."""
        data = pd.DataFrame(
            {
                "entity_id": ["user_1", "user_2"],
                "age": [25, 30],
                "income": [50000, 60000],
            }
        )

        transformations = [
            {"feature_name": "user_age", "expression": "$age"},
            {"feature_name": "income_k", "expression": "$income / 1000"},
        ]

        result = await pipeline_orchestrator._apply_transformations(
            data, transformations
        )

        assert len(result) == 4  # 2 records * 2 features
        assert all(isinstance(fv, FeatureValueWrite) for fv in result)

        # Check specific values
        age_features = [fv for fv in result if fv.feature_name == "user_age"]
        assert len(age_features) == 2
        assert age_features[0].value == 25
        assert age_features[1].value == 30

    def test_evaluate_expression_simple(self, pipeline_orchestrator):
        """Test simple expression evaluation."""
        row = pd.Series({"age": 25, "income": 50000})

        # Test simple column reference
        result = pipeline_orchestrator._evaluate_expression("$age", row)
        assert result == 25

        # Test mathematical expression
        result = pipeline_orchestrator._evaluate_expression("$age * 2", row)
        assert result == 50

    def test_evaluate_expression_fallback(self, pipeline_orchestrator):
        """Test expression evaluation fallback."""
        row = pd.Series({"age": 25, "name": "John"})

        # Test complex expression (should return as string)
        result = pipeline_orchestrator._evaluate_expression("$name.upper()", row)
        assert result == "$name.upper()"  # Should not evaluate complex expressions

        # Test invalid expression (should return first column value)
        result = pipeline_orchestrator._evaluate_expression("invalid_expression", row)
        assert result == 25  # First column value

    @pytest.mark.asyncio
    async def test_get_pipeline_runs(
        self, pipeline_orchestrator, mock_feature_store_service
    ):
        """Test getting pipeline run history."""
        # Mock session and query results
        mock_session = AsyncMock()
        mock_feature_store_service.db_manager.get_session.return_value.__aenter__.return_value = mock_session

        # Mock run records
        mock_run1 = Mock()
        mock_run1.id = 1
        mock_run1.pipeline_name = "test_pipeline"
        mock_run1.status = PipelineStatus.SUCCESS.value
        mock_run1.start_time = datetime.utcnow()
        mock_run1.end_time = datetime.utcnow()
        mock_run1.created_at = datetime.utcnow()
        mock_run1.features_processed = 10
        mock_run1.records_processed = 5
        mock_run1.error_message = None
        mock_run1.metrics = '{"test": true}'

        mock_result = Mock()
        mock_result.scalars.return_value.all.return_value = [mock_run1]
        mock_session.execute.return_value = mock_result

        # Execute
        runs = await pipeline_orchestrator.get_pipeline_runs("test_pipeline")

        # Verify
        assert len(runs) == 1
        assert runs[0].pipeline_name == "test_pipeline"
        assert runs[0].status == PipelineStatus.SUCCESS
        assert runs[0].features_processed == 10

    def test_get_registered_pipelines(
        self, pipeline_orchestrator, sample_pipeline_config
    ):
        """Test getting list of registered pipelines."""
        # Initially empty
        pipelines = pipeline_orchestrator.get_registered_pipelines()
        assert len(pipelines) == 0

        # Add a pipeline
        pipeline_orchestrator._pipeline_configs[sample_pipeline_config.name] = (
            sample_pipeline_config
        )

        pipelines = pipeline_orchestrator.get_registered_pipelines()
        assert len(pipelines) == 1
        assert sample_pipeline_config.name in pipelines

    def test_get_pipeline_config(self, pipeline_orchestrator, sample_pipeline_config):
        """Test getting pipeline configuration."""
        # Non-existent pipeline
        config = pipeline_orchestrator.get_pipeline_config("non_existent")
        assert config is None

        # Add and retrieve pipeline
        pipeline_orchestrator._pipeline_configs[sample_pipeline_config.name] = (
            sample_pipeline_config
        )
        config = pipeline_orchestrator.get_pipeline_config(sample_pipeline_config.name)
        assert config == sample_pipeline_config

    @pytest.mark.asyncio
    async def test_cancel_pipeline(self, pipeline_orchestrator):
        """Test cancelling a running pipeline."""
        # Mock running task
        mock_task = AsyncMock()
        pipeline_orchestrator._running_pipelines["test_pipeline"] = mock_task

        # Cancel
        result = pipeline_orchestrator.cancel_pipeline("test_pipeline")

        # Verify
        assert result is True
        mock_task.cancel.assert_called_once()
        assert "test_pipeline" not in pipeline_orchestrator._running_pipelines

        # Test cancelling non-existent pipeline
        result = pipeline_orchestrator.cancel_pipeline("non_existent")
        assert result is False
