"""Tests for ML experiment tracking functionality."""

import asyncio
import tempfile
from pathlib import Path

import pytest
from fastapi.testclient import TestClient
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.orm import sessionmaker

from libs.analytics_core.database import Base
from libs.analytics_core.models import User
from libs.ml_models import (
    ArtifactManager,
    ExperimentCreate,
    MyExperimentTracker,
    MetricCreate,
    ParamCreate,
    RunCreate,
)
from services.analytics_api.main import app


# Test fixtures
@pytest.fixture
async def async_db_session():
    """Create an async database session for testing."""
    engine = create_async_engine("sqlite+aiosqlite:///:memory:", echo=False)

    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    async_session = sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)

    async with async_session() as session:
        yield session

    await engine.dispose()


@pytest.fixture
async def test_user(async_db_session: AsyncSession):
    """Create a test user."""
    user = User(
        email="test@example.com",
        username="testuser",
        full_name="Test User",
        hashed_password="$2b$12$abcdefghijklmnopqrstuvwxyz",
        is_active=True,
        is_superuser=False,
    )

    async_db_session.add(user)
    await async_db_session.commit()
    await async_db_session.refresh(user)

    return user


@pytest.fixture
def temp_artifacts_dir():
    """Create a temporary directory for artifacts."""
    with tempfile.TemporaryDirectory() as tmpdir:
        yield tmpdir


@pytest.fixture
def artifact_manager(temp_artifacts_dir):
    """Create an artifact manager with temporary directory."""
    return ArtifactManager(temp_artifacts_dir)


class TestExperimentTracker:
    """Test cases for ExperimentTracker."""

    async def test_create_experiment(
        self, async_db_session: AsyncSession, test_user: User
    ):
        """Test creating a new experiment."""
        tracker = MyExperimentTracker(async_db_session)

        experiment_data = ExperimentCreate(
            name="test_experiment",
            description="A test experiment",
            tags={"project": "testing", "version": "1.0"},
        )

        experiment = await tracker.create_experiment(experiment_data, test_user.id)

        assert experiment.name == "test_experiment"
        assert experiment.description == "A test experiment"
        assert experiment.tags == {"project": "testing", "version": "1.0"}
        assert experiment.owner_id == test_user.id
        assert experiment.lifecycle_stage == "active"
        assert experiment.artifact_location is not None

    async def test_list_experiments(
        self, async_db_session: AsyncSession, test_user: User
    ):
        """Test listing experiments."""
        tracker = MyExperimentTracker(async_db_session)

        # Create multiple experiments
        for i in range(3):
            experiment_data = ExperimentCreate(name=f"experiment_{i}")
            await tracker.create_experiment(experiment_data, test_user.id)

        experiments = await tracker.list_experiments()

        assert len(experiments) == 3
        assert all(exp.name.startswith("experiment_") for exp in experiments)

    async def test_get_experiment(
        self, async_db_session: AsyncSession, test_user: User
    ):
        """Test getting a specific experiment."""
        tracker = MyExperimentTracker(async_db_session)

        experiment_data = ExperimentCreate(name="get_test_experiment")
        created_experiment = await tracker.create_experiment(
            experiment_data, test_user.id
        )

        retrieved_experiment = await tracker.get_experiment(created_experiment.id)

        assert retrieved_experiment is not None
        assert retrieved_experiment.name == "get_test_experiment"
        assert retrieved_experiment.id == created_experiment.id

    async def test_create_run(self, async_db_session: AsyncSession, test_user: User):
        """Test creating an experiment run."""
        tracker = MyExperimentTracker(async_db_session)

        experiment_data = ExperimentCreate(name="run_test_experiment")
        experiment = await tracker.create_experiment(experiment_data, test_user.id)

        run_data = RunCreate(
            name="test_run",
            tags={"environment": "test", "framework": "pytest"},
        )

        run = await tracker.create_run(experiment.id, run_data)

        assert run.experiment_id == experiment.id
        assert run.name == "test_run"
        assert run.status == "RUNNING"
        assert run.run_uuid is not None
        assert len(run.run_uuid) == 32  # UUID hex length
        assert run.artifact_uri is not None

    async def test_log_metric(self, async_db_session: AsyncSession, test_user: User):
        """Test logging metrics for a run."""
        tracker = MyExperimentTracker(async_db_session)

        experiment_data = ExperimentCreate(name="metric_test_experiment")
        experiment = await tracker.create_experiment(experiment_data, test_user.id)

        run_data = RunCreate(name="metric_test_run")
        run = await tracker.create_run(experiment.id, run_data)

        metric = MetricCreate(key="accuracy", value=0.85, step=1)
        await tracker.log_metric(run.run_uuid, metric)

        # Retrieve metrics
        metrics = await tracker.get_run_metrics(run.run_uuid)

        assert len(metrics) == 1
        assert metrics[0]["key"] == "accuracy"
        assert metrics[0]["value"] == 0.85
        assert metrics[0]["step"] == 1

    async def test_log_param(self, async_db_session: AsyncSession, test_user: User):
        """Test logging parameters for a run."""
        tracker = MyExperimentTracker(async_db_session)

        experiment_data = ExperimentCreate(name="param_test_experiment")
        experiment = await tracker.create_experiment(experiment_data, test_user.id)

        run_data = RunCreate(name="param_test_run")
        run = await tracker.create_run(experiment.id, run_data)

        param = ParamCreate(key="learning_rate", value="0.001")
        await tracker.log_param(run.run_uuid, param)

        # Retrieve parameters
        params = await tracker.get_run_params(run.run_uuid)

        assert len(params) == 1
        assert params["learning_rate"] == "0.001"

    async def test_finish_run(self, async_db_session: AsyncSession, test_user: User):
        """Test finishing a run."""
        tracker = MyExperimentTracker(async_db_session)

        experiment_data = ExperimentCreate(name="finish_test_experiment")
        experiment = await tracker.create_experiment(experiment_data, test_user.id)

        run_data = RunCreate(name="finish_test_run")
        run = await tracker.create_run(experiment.id, run_data)

        await tracker.finish_run(run.run_uuid, "FINISHED")

        # Verify run is finished
        runs = await tracker.list_runs(experiment.id)
        finished_run = next(r for r in runs if r.run_uuid == run.run_uuid)

        assert finished_run.status == "FINISHED"
        assert finished_run.end_time is not None

    async def test_compare_runs(self, async_db_session: AsyncSession, test_user: User):
        """Test comparing multiple runs."""
        tracker = MyExperimentTracker(async_db_session)

        experiment_data = ExperimentCreate(name="compare_test_experiment")
        experiment = await tracker.create_experiment(experiment_data, test_user.id)

        # Create two runs with different metrics
        run1_data = RunCreate(name="run_1")
        run1 = await tracker.create_run(experiment.id, run1_data)

        run2_data = RunCreate(name="run_2")
        run2 = await tracker.create_run(experiment.id, run2_data)

        # Log different metrics for each run
        await tracker.log_metric(
            run1.run_uuid, MetricCreate(key="accuracy", value=0.85)
        )
        await tracker.log_metric(
            run2.run_uuid, MetricCreate(key="accuracy", value=0.90)
        )

        await tracker.log_param(run1.run_uuid, ParamCreate(key="lr", value="0.001"))
        await tracker.log_param(run2.run_uuid, ParamCreate(key="lr", value="0.01"))

        # Compare runs
        comparison = await tracker.compare_runs([run1.run_uuid, run2.run_uuid])

        assert len(comparison["runs"]) == 2
        assert "accuracy" in comparison["metrics_comparison"]
        assert "lr" in comparison["params_comparison"]

        # Check metric comparison
        accuracy_comparison = comparison["metrics_comparison"]["accuracy"]
        assert accuracy_comparison[run1.run_uuid] == 0.85
        assert accuracy_comparison[run2.run_uuid] == 0.90


class TestArtifactManager:
    """Test cases for ArtifactManager."""

    async def test_store_artifact(self, artifact_manager: ArtifactManager):
        """Test storing an artifact."""
        run_uuid = "test_run_uuid"
        artifact_name = "model.pkl"
        content = b"fake model content"

        metadata = await artifact_manager.store_artifact(
            run_uuid=run_uuid,
            artifact_name=artifact_name,
            file_content=content,
            content_type="application/octet-stream",
        )

        assert metadata.name == artifact_name
        assert metadata.size == len(content)
        assert metadata.run_uuid == run_uuid
        assert metadata.content_type == "application/octet-stream"
        assert metadata.checksum is not None

    async def test_get_artifact(self, artifact_manager: ArtifactManager):
        """Test retrieving an artifact."""
        run_uuid = "test_run_uuid"
        artifact_name = "test_file.txt"
        original_content = b"test content"

        # Store artifact
        await artifact_manager.store_artifact(
            run_uuid=run_uuid,
            artifact_name=artifact_name,
            file_content=original_content,
        )

        # Retrieve artifact
        result = await artifact_manager.get_artifact(run_uuid, artifact_name)

        assert result is not None
        content, metadata = result
        assert content == original_content
        assert metadata.name == artifact_name

    async def test_list_artifacts(self, artifact_manager: ArtifactManager):
        """Test listing artifacts for a run."""
        run_uuid = "test_run_uuid"

        # Store multiple artifacts
        artifacts_data = [
            ("file1.txt", b"content1"),
            ("file2.json", b'{"key": "value"}'),
            ("model.pkl", b"model data"),
        ]

        for name, content in artifacts_data:
            await artifact_manager.store_artifact(
                run_uuid=run_uuid,
                artifact_name=name,
                file_content=content,
            )

        # List artifacts
        artifacts = await artifact_manager.list_artifacts(run_uuid)

        assert len(artifacts) == 3
        artifact_names = {a.name for a in artifacts}
        assert artifact_names == {"file1.txt", "file2.json", "model.pkl"}

    async def test_delete_artifact(self, artifact_manager: ArtifactManager):
        """Test deleting an artifact."""
        run_uuid = "test_run_uuid"
        artifact_name = "to_delete.txt"

        # Store artifact
        await artifact_manager.store_artifact(
            run_uuid=run_uuid,
            artifact_name=artifact_name,
            file_content=b"delete me",
        )

        # Verify it exists
        artifacts_before = await artifact_manager.list_artifacts(run_uuid)
        assert len(artifacts_before) == 1

        # Delete artifact
        success = await artifact_manager.delete_artifact(run_uuid, artifact_name)
        assert success

        # Verify it's gone
        artifacts_after = await artifact_manager.list_artifacts(run_uuid)
        assert len(artifacts_after) == 0

    async def test_store_artifact_from_file(
        self, artifact_manager: ArtifactManager, temp_artifacts_dir
    ):
        """Test storing an artifact from a file."""
        # Create a temporary file
        temp_file = Path(temp_artifacts_dir) / "source_file.txt"
        test_content = "file content"
        temp_file.write_text(test_content)

        run_uuid = "test_run_uuid"
        artifact_name = "copied_file.txt"

        metadata = await artifact_manager.store_artifact_from_file(
            run_uuid=run_uuid,
            artifact_name=artifact_name,
            file_path=str(temp_file),
            content_type="text/plain",
        )

        assert metadata.name == artifact_name
        assert metadata.size == len(test_content.encode())
        assert metadata.content_type == "text/plain"

        # Verify the content
        result = await artifact_manager.get_artifact(run_uuid, artifact_name)
        assert result is not None
        content, _ = result
        assert content.decode() == test_content


@pytest.mark.asyncio
class TestExperimentAPI:
    """Test cases for experiment tracking API endpoints."""

    def test_api_endpoints_available(self):
        """Test that all API endpoints are properly registered."""
        client = TestClient(app)

        # Test that the API returns proper responses (even if authentication fails)
        # We mainly want to ensure endpoints are registered

        response = client.get("/v1/experiments")
        assert response.status_code in [
            200,
            401,
            422,
        ]  # Auth required but endpoint exists

        response = client.post("/v1/experiments", json={"name": "test"})
        assert response.status_code in [
            200,
            401,
            422,
        ]  # Auth required but endpoint exists


# Helper function to run async tests
def test_experiment_tracker_integration():
    """Integration test for experiment tracking workflow."""

    async def run_test():
        # Create in-memory database
        engine = create_async_engine("sqlite+aiosqlite:///:memory:", echo=False)

        async with engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)

        async_session = sessionmaker(
            engine, class_=AsyncSession, expire_on_commit=False
        )

        async with async_session() as session:
            # Create test user
            user = User(
                email="integration@test.com",
                username="integration_user",
                hashed_password="test_password",
                is_active=True,
            )
            session.add(user)
            await session.commit()
            await session.refresh(user)

            # Test complete workflow
            tracker = MyExperimentTracker(session)

            # 1. Create experiment
            exp_data = ExperimentCreate(name="integration_test")
            experiment = await tracker.create_experiment(exp_data, user.id)

            # 2. Create run
            run_data = RunCreate(name="test_run")
            run = await tracker.create_run(experiment.id, run_data)

            # 3. Log metrics and parameters
            await tracker.log_metric(run.run_uuid, MetricCreate(key="loss", value=0.1))
            await tracker.log_param(run.run_uuid, ParamCreate(key="epochs", value="10"))

            # 4. Finish run
            await tracker.finish_run(run.run_uuid)

            # 5. Verify everything
            final_experiment = await tracker.get_experiment(experiment.id)
            runs = await tracker.list_runs(experiment.id)
            metrics = await tracker.get_run_metrics(run.run_uuid)
            params = await tracker.get_run_params(run.run_uuid)

            assert final_experiment.name == "integration_test"
            assert len(runs) == 1
            assert runs[0].status == "FINISHED"
            assert len(metrics) == 1
            assert metrics[0]["key"] == "loss"
            assert len(params) == 1
            assert params["epochs"] == "10"

        await engine.dispose()

    # Run the async test
    asyncio.run(run_test())
