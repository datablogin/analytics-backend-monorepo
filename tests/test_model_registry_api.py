"""Tests for ML Model Registry and Serving API endpoints."""

import json
from io import BytesIO
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from fastapi.testclient import TestClient
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.orm import sessionmaker

from libs.analytics_core.database import Base
from libs.analytics_core.models import User
from services.analytics_api.main import app


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
def client():
    """Create test client with overridden dependencies."""
    from services.analytics_api.routes.models import (
        get_current_user,
        get_model_registry,
    )

    # Create mock user
    mock_user = MagicMock()
    mock_user.username = "testuser"
    mock_user.id = 1
    mock_user.email = "test@example.com"

    # Create mock registry
    mock_registry = MagicMock()

    # Override dependencies
    app.dependency_overrides[get_current_user] = lambda: mock_user
    app.dependency_overrides[get_model_registry] = lambda: mock_registry

    # Mock the secure model loader to avoid validation issues
    with patch("services.analytics_api.routes.models.load_model_secure") as mock_loader:
        mock_model = MagicMock()
        mock_model.predict = MagicMock(return_value=[0, 1, 0])
        mock_loader.return_value = mock_model

        client = TestClient(app)
        yield client

    # Clean up overrides
    app.dependency_overrides.clear()


@pytest.fixture
def auth_headers():
    """Mock authentication headers."""
    return {"Authorization": "Bearer mock_token"}


class TestModelRegistryAPI:
    """Test cases for Model Registry API endpoints."""

    def test_register_model_sklearn(self, client):
        """Test registering a sklearn model."""
        # Get the mock registry from the client fixture and configure it
        from services.analytics_api.routes.models import get_model_registry

        mock_registry = app.dependency_overrides[get_model_registry]()
        mock_registry.register_model.return_value = "1"

        # Create simple test file content (we'll mock the secure loader)
        pickle_content = b"fake_model_content"

        # Prepare request data - serialize dict values as JSON strings for multipart form
        model_data = {
            "name": "test_model",
            "description": "Test sklearn model",
            "model_type": "classification",
            "algorithm": "random_forest",
            "framework": "sklearn",
            "metrics": json.dumps({"accuracy": 0.95, "f1_score": 0.92}),
            "tags": json.dumps({"project": "test", "version": "1.0"}),
        }

        files = {
            "model_file": (
                "model.pkl",
                BytesIO(pickle_content),
                "application/octet-stream",
            )
        }

        response = client.post(
            "/v1/models",
            data=model_data,
            files=files,
            headers={"Authorization": "Bearer mock_token"},
        )

        assert response.status_code == 200
        response_data = response.json()
        assert response_data["success"] is True
        assert response_data["data"]["model_name"] == "test_model"
        assert response_data["data"]["version"] == "1"

    def test_list_models(self, client):
        """Test listing models."""
        # Get the mock registry from the client fixture and configure it
        from services.analytics_api.routes.models import get_model_registry

        mock_registry = app.dependency_overrides[get_model_registry]()

        mock_models = [
            {
                "name": "model1",
                "description": "First model",
                "creation_time": "2023-01-01T00:00:00",
                "last_updated": "2023-01-01T00:00:00",
                "latest_versions": {"Production": "1", "Staging": "2"},
                "tags": {"project": "test"},
            },
            {
                "name": "model2",
                "description": "Second model",
                "creation_time": "2023-01-02T00:00:00",
                "last_updated": "2023-01-02T00:00:00",
                "latest_versions": {"Production": "1"},
                "tags": {},
            },
        ]
        mock_registry.list_models.return_value = mock_models

        response = client.get(
            "/v1/models",
            headers={"Authorization": "Bearer mock_token"},
        )

        assert response.status_code == 200
        response_data = response.json()
        assert response_data["success"] is True
        assert len(response_data["data"]) == 2
        assert response_data["data"][0]["name"] == "model1"
        assert response_data["data"][1]["name"] == "model2"

    @patch("services.analytics_api.routes.models.get_model_registry")
    @patch("services.analytics_api.routes.models.get_current_user")
    def test_get_model(self, mock_auth, mock_registry, client):
        """Test getting a specific model."""
        # Mock authentication
        mock_user = MagicMock()
        mock_auth.return_value = mock_user

        # Mock registry response
        mock_registry_instance = MagicMock()
        mock_metadata = MagicMock()
        mock_metadata.name = "test_model"
        mock_metadata.version = "1"
        mock_metadata.stage = "Production"
        mock_metadata.description = "Test model"
        mock_metadata.model_type = "classification"
        mock_metadata.algorithm = "random_forest"
        mock_metadata.framework = "sklearn"
        mock_metadata.metrics = {"accuracy": 0.95}
        mock_metadata.training_dataset_version = "v1.0"
        mock_metadata.training_run_id = "run123"
        mock_metadata.model_artifact_uri = "s3://bucket/path"
        mock_metadata.created_by = "testuser"
        mock_metadata.created_at = "2023-01-01T00:00:00"
        mock_metadata.tags = {"project": "test"}

        mock_registry_instance.get_model_metadata.return_value = mock_metadata
        mock_registry.return_value = mock_registry_instance

        response = client.get(
            "/v1/models/test_model",
            headers={"Authorization": "Bearer mock_token"},
        )

        assert response.status_code == 200
        response_data = response.json()
        assert response_data["success"] is True
        assert response_data["data"]["name"] == "test_model"
        assert response_data["data"]["version"] == "1"
        assert response_data["data"]["stage"] == "Production"

    @patch("services.analytics_api.routes.models.get_model_registry")
    @patch("services.analytics_api.routes.models.get_current_user")
    def test_deploy_model(self, mock_auth, mock_registry, client):
        """Test deploying a model to a stage."""
        # Mock authentication
        mock_user = MagicMock()
        mock_auth.return_value = mock_user

        # Mock registry response
        mock_registry_instance = MagicMock()
        mock_model_version = MagicMock()
        mock_model_version.current_stage = "Staging"
        mock_registry_instance.transition_model_stage.return_value = mock_model_version
        mock_registry.return_value = mock_registry_instance

        deploy_data = {
            "stage": "Production",
            "archive_existing": True,
        }

        response = client.post(
            "/v1/models/test_model/versions/1/deploy",
            json=deploy_data,
            headers={"Authorization": "Bearer mock_token"},
        )

        assert response.status_code == 200
        response_data = response.json()
        assert response_data["success"] is True
        assert response_data["data"]["model_name"] == "test_model"
        assert response_data["data"]["version"] == "1"
        assert response_data["data"]["new_stage"] == "Production"

    @patch("services.analytics_api.routes.models.get_model_server")
    @patch("services.analytics_api.routes.models.get_current_user")
    def test_predict(self, mock_auth, mock_server, client):
        """Test making predictions with a model."""
        # Mock authentication
        mock_user = MagicMock()
        mock_auth.return_value = mock_user

        # Mock server response
        mock_server_instance = AsyncMock()
        mock_prediction_result = MagicMock()
        mock_prediction_result.predictions = [0, 1, 0]
        mock_prediction_result.probabilities = [[0.8, 0.2], [0.3, 0.7], [0.9, 0.1]]
        mock_prediction_result.model_name = "test_model"
        mock_prediction_result.model_version = "1"
        mock_prediction_result.model_stage = "Production"
        mock_prediction_result.inference_time_ms = 25.5
        mock_prediction_result.preprocessing_time_ms = 5.0
        mock_prediction_result.postprocessing_time_ms = 2.5
        mock_prediction_result.request_id = "req_123"
        mock_prediction_result.status = "success"
        mock_prediction_result.error_message = None

        mock_server_instance.predict.return_value = mock_prediction_result
        mock_server.return_value = mock_server_instance

        prediction_data = {
            "inputs": [
                {"feature1": 1.0, "feature2": 2.0},
                {"feature1": 1.5, "feature2": 2.5},
                {"feature1": 0.5, "feature2": 1.5},
            ],
            "return_probabilities": True,
            "request_id": "test_request_123",
        }

        response = client.post(
            "/v1/models/test_model/predict",
            json=prediction_data,
            headers={"Authorization": "Bearer mock_token"},
        )

        assert response.status_code == 200
        response_data = response.json()
        assert response_data["success"] is True
        assert len(response_data["data"].predictions) == 3
        assert response_data["data"].model_name == "test_model"

    @patch("services.analytics_api.routes.models.get_model_server")
    @patch("services.analytics_api.routes.models.get_current_user")
    def test_get_model_status(self, mock_auth, mock_server, client):
        """Test getting model status."""
        # Mock authentication
        mock_user = MagicMock()
        mock_auth.return_value = mock_user

        # Mock server response
        mock_server_instance = MagicMock()
        mock_stats = {
            "name": "test_model",
            "version": "1",
            "stage": "Production",
            "healthy": True,
            "loaded": True,
            "usage_count": 100,
            "failure_count": 2,
            "consecutive_failures": 0,
            "age_seconds": 3600,
            "idle_seconds": 60,
            "last_used": "2023-01-01T12:00:00",
            "last_failure": None,
        }
        mock_server_instance.get_model_stats.return_value = mock_stats
        mock_server.return_value = mock_server_instance

        response = client.get(
            "/v1/models/test_model/status",
            headers={"Authorization": "Bearer mock_token"},
        )

        assert response.status_code == 200
        response_data = response.json()
        assert response_data["success"] is True
        assert response_data["data"]["name"] == "test_model"
        assert response_data["data"]["healthy"] is True
        assert response_data["data"]["loaded"] is True

    @patch("services.analytics_api.routes.models.get_model_registry")
    @patch("services.analytics_api.routes.models.get_model_server")
    @patch("services.analytics_api.routes.models.get_current_user")
    def test_get_registry_stats(self, mock_auth, mock_server, mock_registry, client):
        """Test getting registry statistics."""
        # Mock authentication
        mock_user = MagicMock()
        mock_auth.return_value = mock_user

        # Mock registry stats
        mock_registry_instance = MagicMock()
        mock_registry_stats = {
            "total_models": 10,
            "total_versions": 25,
            "models_by_stage": {"Production": 5, "Staging": 3, "None": 2},
            "recent_models_7d": 2,
            "registry_health": "healthy",
        }
        mock_registry_instance.get_registry_stats.return_value = mock_registry_stats
        mock_registry.return_value = mock_registry_instance

        # Mock server stats
        mock_server_instance = MagicMock()
        mock_server_stats = {
            "uptime_seconds": 3600,
            "request_count": 1000,
            "error_count": 5,
            "error_rate": 0.005,
            "loaded_models": 3,
            "cache_utilization": 0.3,
            "healthy_models": 3,
        }
        mock_server_instance.get_server_stats.return_value = mock_server_stats
        mock_server.return_value = mock_server_instance

        response = client.get(
            "/v1/models/registry/stats",
            headers={"Authorization": "Bearer mock_token"},
        )

        assert response.status_code == 200
        response_data = response.json()
        assert response_data["success"] is True
        assert "registry" in response_data["data"]
        assert "serving" in response_data["data"]
        assert response_data["data"]["registry"]["total_models"] == 10
        assert response_data["data"]["serving"]["loaded_models"] == 3

    @patch("services.analytics_api.routes.models.get_model_registry")
    @patch("services.analytics_api.routes.models.get_current_user")
    def test_delete_model(self, mock_auth, mock_registry, client):
        """Test deleting a model."""
        # Mock authentication
        mock_user = MagicMock()
        mock_auth.return_value = mock_user

        # Mock registry
        mock_registry_instance = MagicMock()
        mock_registry_instance.delete_model.return_value = None
        mock_registry.return_value = mock_registry_instance

        response = client.delete(
            "/v1/models/test_model",
            headers={"Authorization": "Bearer mock_token"},
        )

        assert response.status_code == 200
        response_data = response.json()
        assert response_data["success"] is True
        assert response_data["data"]["model_name"] == "test_model"
        assert response_data["data"]["deleted"] is True

    @patch("services.analytics_api.routes.models.get_model_registry")
    @patch("services.analytics_api.routes.models.get_current_user")
    def test_search_models(self, mock_auth, mock_registry, client):
        """Test searching models."""
        # Mock authentication
        mock_user = MagicMock()
        mock_auth.return_value = mock_user

        # Mock registry response
        mock_registry_instance = MagicMock()
        mock_model_version = MagicMock()
        mock_model_version.name = "test_model"
        mock_model_version.version = "1"
        mock_model_version.current_stage = "Production"
        mock_model_version.creation_timestamp = 1672531200000  # 2023-01-01
        mock_model_version.last_updated_timestamp = 1672531200000
        mock_model_version.description = "Test model"
        mock_model_version.tags = {"project": "test"}
        mock_model_version.run_id = "run123"
        mock_model_version.source = "s3://bucket/path"

        mock_registry_instance.search_model_versions.return_value = [mock_model_version]
        mock_registry.return_value = mock_registry_instance

        response = client.post(
            "/v1/models/search?filter_string=name='test_model'&max_results=10",
            headers={"Authorization": "Bearer mock_token"},
        )

        assert response.status_code == 200
        response_data = response.json()
        assert response_data["success"] is True
        assert len(response_data["data"]) == 1
        assert response_data["data"][0]["name"] == "test_model"
        assert response_data["data"][0]["version"] == "1"
        assert response_data["data"][0]["stage"] == "Production"

    def test_unauthorized_access(self, client):
        """Test that endpoints require authentication."""
        # Test without auth header
        response = client.get("/v1/models")
        assert response.status_code in [401, 403, 422]

        # Test with invalid auth header
        response = client.get(
            "/v1/models", headers={"Authorization": "Bearer invalid_token"}
        )
        assert response.status_code in [401, 403, 422]


class TestModelRegistryErrors:
    """Test error handling in Model Registry API."""

    @patch("services.analytics_api.routes.models.get_model_registry")
    @patch("services.analytics_api.routes.models.get_current_user")
    def test_register_model_unsupported_framework(
        self, mock_auth, mock_registry, client
    ):
        """Test registering model with unsupported framework."""
        # Mock authentication
        mock_user = MagicMock()
        mock_user.username = "testuser"
        mock_auth.return_value = mock_user

        # Mock registry
        mock_registry_instance = MagicMock()
        mock_registry.return_value = mock_registry_instance

        model_data = {
            "name": "test_model",
            "framework": "unsupported_framework",
            "model_type": "classification",
            "algorithm": "unknown",
        }

        files = {
            "model_file": (
                "model.bin",
                BytesIO(b"fake content"),
                "application/octet-stream",
            )
        }

        response = client.post(
            "/v1/models",
            data=model_data,
            files=files,
            headers={"Authorization": "Bearer mock_token"},
        )

        assert response.status_code == 400
        assert "not yet supported" in response.json()["detail"]

    @patch("services.analytics_api.routes.models.get_model_registry")
    @patch("services.analytics_api.routes.models.get_current_user")
    def test_get_nonexistent_model(self, mock_auth, mock_registry, client):
        """Test getting a model that doesn't exist."""
        # Mock authentication
        mock_user = MagicMock()
        mock_auth.return_value = mock_user

        # Mock registry to raise exception
        mock_registry_instance = MagicMock()
        mock_registry_instance.get_model_metadata.side_effect = Exception(
            "Model not found"
        )
        mock_registry.return_value = mock_registry_instance

        response = client.get(
            "/v1/models/nonexistent_model",
            headers={"Authorization": "Bearer mock_token"},
        )

        assert response.status_code == 404
        assert "Model not found" in response.json()["detail"]

    @patch("services.analytics_api.routes.models.get_model_server")
    @patch("services.analytics_api.routes.models.get_current_user")
    def test_predict_with_server_error(self, mock_auth, mock_server, client):
        """Test prediction when server raises an error."""
        # Mock authentication
        mock_user = MagicMock()
        mock_auth.return_value = mock_user

        # Mock server to raise exception
        mock_server_instance = AsyncMock()
        mock_server_instance.predict.side_effect = Exception("Model not loaded")
        mock_server.return_value = mock_server_instance

        prediction_data = {
            "inputs": [{"feature1": 1.0, "feature2": 2.0}],
        }

        response = client.post(
            "/v1/models/test_model/predict",
            json=prediction_data,
            headers={"Authorization": "Bearer mock_token"},
        )

        assert response.status_code == 500
        assert "Prediction failed" in response.json()["detail"]


class TestModelVersions:
    """Test model version management."""

    @patch("services.analytics_api.routes.models.get_model_registry")
    @patch("services.analytics_api.routes.models.get_current_user")
    def test_list_model_versions(self, mock_auth, mock_registry, client):
        """Test listing versions of a model."""
        # Mock authentication
        mock_user = MagicMock()
        mock_auth.return_value = mock_user

        # Mock registry response
        mock_registry_instance = MagicMock()

        # Mock model versions
        mock_version1 = MagicMock()
        mock_version1.version = "1"
        mock_version2 = MagicMock()
        mock_version2.version = "2"

        mock_registry_instance.search_model_versions.return_value = [
            mock_version1,
            mock_version2,
        ]

        # Mock metadata for each version
        mock_metadata1 = MagicMock()
        mock_metadata1.name = "test_model"
        mock_metadata1.version = "1"
        mock_metadata1.stage = "Production"
        mock_metadata1.description = "Version 1"
        mock_metadata1.model_type = "classification"
        mock_metadata1.algorithm = "random_forest"
        mock_metadata1.framework = "sklearn"
        mock_metadata1.metrics = {"accuracy": 0.95}
        mock_metadata1.training_dataset_version = None
        mock_metadata1.training_run_id = "run1"
        mock_metadata1.model_artifact_uri = "s3://bucket/v1"
        mock_metadata1.created_by = "user1"
        mock_metadata1.created_at = "2023-01-01T00:00:00"
        mock_metadata1.tags = {}

        mock_metadata2 = MagicMock()
        mock_metadata2.name = "test_model"
        mock_metadata2.version = "2"
        mock_metadata2.stage = "Staging"
        mock_metadata2.description = "Version 2"
        mock_metadata2.model_type = "classification"
        mock_metadata2.algorithm = "gradient_boosting"
        mock_metadata2.framework = "sklearn"
        mock_metadata2.metrics = {"accuracy": 0.97}
        mock_metadata2.training_dataset_version = None
        mock_metadata2.training_run_id = "run2"
        mock_metadata2.model_artifact_uri = "s3://bucket/v2"
        mock_metadata2.created_by = "user2"
        mock_metadata2.created_at = "2023-01-02T00:00:00"
        mock_metadata2.tags = {"improved": "true"}

        mock_registry_instance.get_model_metadata.side_effect = [
            mock_metadata1,
            mock_metadata2,
        ]
        mock_registry.return_value = mock_registry_instance

        response = client.get(
            "/v1/models/test_model/versions",
            headers={"Authorization": "Bearer mock_token"},
        )

        assert response.status_code == 200
        response_data = response.json()
        assert response_data["success"] is True
        assert len(response_data["data"]) == 2
        assert response_data["data"][0]["version"] == "1"
        assert response_data["data"][1]["version"] == "2"
        assert response_data["data"][0]["stage"] == "Production"
        assert response_data["data"][1]["stage"] == "Staging"
