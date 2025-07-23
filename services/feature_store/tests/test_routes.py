"""Tests for feature store API routes."""

from datetime import datetime
from unittest.mock import AsyncMock, Mock, patch

import pytest
from fastapi.testclient import TestClient

from ..main import app
from ..models import (
    FeatureDefinitionResponse,
    FeatureDiscoveryResponse,
    FeatureServingResponse,
    FeatureStatus,
    FeatureType,
    FeatureValueResponse,
)


@pytest.fixture
def client():
    """Test client."""
    return TestClient(app)


@pytest.fixture
def sample_feature_response():
    """Sample feature definition response."""
    return FeatureDefinitionResponse(
        id=1,
        name="user_age",
        feature_group="user_demographics",
        description="User age in years",
        feature_type=FeatureType.INTEGER,
        status=FeatureStatus.ACTIVE,
        default_value=25,
        validation_rules={"min_value": 0, "max_value": 120},
        tags=["demographic", "user"],
        owner="data_team",
        version=1,
        created_at=datetime.utcnow(),
        updated_at=datetime.utcnow(),
    )


class TestFeatureDefinitionRoutes:
    """Tests for feature definition routes."""

    @patch("services.feature_store.routes.get_feature_store_service")
    @patch("services.feature_store.routes.get_cache")
    def test_create_feature_success(
        self, mock_get_cache, mock_get_service, client, sample_feature_response
    ):
        """Test successful feature creation."""
        # Mock service
        mock_service = Mock()
        mock_service.get_feature = AsyncMock(return_value=None)  # Feature doesn't exist
        mock_service.create_feature = AsyncMock(return_value=sample_feature_response)
        mock_get_service.return_value = mock_service

        # Mock cache
        mock_cache = Mock()
        mock_cache.set_feature_definition = AsyncMock()
        mock_get_cache.return_value = mock_cache

        # Request data
        feature_data = {
            "name": "user_age",
            "feature_group": "user_demographics",
            "description": "User age in years",
            "feature_type": "integer",
            "default_value": 25,
            "validation_rules": {"min_value": 0, "max_value": 120},
            "tags": ["demographic", "user"],
            "owner": "data_team",
        }

        # Execute
        response = client.post("/api/v1/feature-store/features", json=feature_data)

        # Verify
        assert response.status_code == 201
        response_data = response.json()
        assert response_data["success"] is True
        assert response_data["message"] == "Feature created successfully"
        assert response_data["data"]["name"] == "user_age"

    @patch("services.feature_store.routes.get_feature_store_service")
    def test_create_feature_already_exists(
        self, mock_get_service, client, sample_feature_response
    ):
        """Test creating a feature that already exists."""
        # Mock service - feature already exists
        mock_service = Mock()
        mock_service.get_feature = AsyncMock(return_value=sample_feature_response)
        mock_get_service.return_value = mock_service

        # Request data
        feature_data = {
            "name": "user_age",
            "feature_group": "user_demographics",
            "feature_type": "integer",
        }

        # Execute
        response = client.post("/api/v1/feature-store/features", json=feature_data)

        # Verify
        assert response.status_code == 409
        response_data = response.json()
        assert "already exists" in response_data["detail"]

    @patch("services.feature_store.routes.get_feature_store_service")
    @patch("services.feature_store.routes.get_cache")
    def test_get_feature_from_cache(self, mock_get_cache, mock_get_service, client):
        """Test getting a feature from cache."""
        # Mock cache hit
        cached_data = {
            "id": 1,
            "name": "user_age",
            "feature_group": "user_demographics",
            "feature_type": "integer",
            "status": "active",
            "version": 1,
            "created_at": datetime.utcnow().isoformat(),
            "updated_at": datetime.utcnow().isoformat(),
        }
        mock_cache = Mock()
        mock_cache.get_feature_definition = AsyncMock(return_value=cached_data)
        mock_get_cache.return_value = mock_cache

        # Execute
        response = client.get("/api/v1/feature-store/features/user_age")

        # Verify
        assert response.status_code == 200
        response_data = response.json()
        assert response_data["success"] is True
        assert response_data["data"]["name"] == "user_age"

    @patch("services.feature_store.routes.get_feature_store_service")
    @patch("services.feature_store.routes.get_cache")
    def test_get_feature_not_found(self, mock_get_cache, mock_get_service, client):
        """Test getting a non-existent feature."""
        # Mock cache miss and service not found
        mock_cache = Mock()
        mock_cache.get_feature_definition = AsyncMock(return_value=None)
        mock_get_cache.return_value = mock_cache

        mock_service = Mock()
        mock_service.get_feature = AsyncMock(return_value=None)
        mock_get_service.return_value = mock_service

        # Execute
        response = client.get("/api/v1/feature-store/features/non_existent")

        # Verify
        assert response.status_code == 404
        response_data = response.json()
        assert "not found" in response_data["detail"]

    @patch("services.feature_store.routes.get_feature_store_service")
    @patch("services.feature_store.routes.get_cache")
    def test_update_feature_success(
        self, mock_get_cache, mock_get_service, client, sample_feature_response
    ):
        """Test successful feature update."""
        # Mock service
        updated_response = sample_feature_response.model_copy()
        updated_response.description = "Updated description"

        mock_service = Mock()
        mock_service.update_feature = AsyncMock(return_value=updated_response)
        mock_get_service.return_value = mock_service

        # Mock cache
        mock_cache = Mock()
        mock_cache.set_feature_definition = AsyncMock()
        mock_get_cache.return_value = mock_cache

        # Request data
        update_data = {
            "description": "Updated description",
            "status": "active",
        }

        # Execute
        response = client.put(
            "/api/v1/feature-store/features/user_age", json=update_data
        )

        # Verify
        assert response.status_code == 200
        response_data = response.json()
        assert response_data["success"] is True
        assert response_data["data"]["description"] == "Updated description"

    @patch("services.feature_store.routes.get_feature_store_service")
    def test_list_features(self, mock_get_service, client, sample_feature_response):
        """Test listing features."""
        # Mock service
        mock_service = Mock()
        mock_service.list_features = AsyncMock(return_value=[sample_feature_response])
        mock_get_service.return_value = mock_service

        # Execute
        response = client.get("/api/v1/feature-store/features?limit=10&offset=0")

        # Verify
        assert response.status_code == 200
        response_data = response.json()
        assert response_data["success"] is True
        assert len(response_data["data"]) == 1
        assert response_data["data"][0]["name"] == "user_age"

    @patch("services.feature_store.routes.get_feature_store_service")
    @patch("services.feature_store.routes.get_cache")
    def test_delete_feature_success(self, mock_get_cache, mock_get_service, client):
        """Test successful feature deletion."""
        # Mock service
        mock_service = Mock()
        mock_service.delete_feature = AsyncMock(return_value=True)
        mock_get_service.return_value = mock_service

        # Mock cache
        mock_cache = Mock()
        mock_cache.invalidate_feature_definition = AsyncMock()
        mock_get_cache.return_value = mock_cache

        # Execute
        response = client.delete("/api/v1/feature-store/features/user_age")

        # Verify
        assert response.status_code == 200
        response_data = response.json()
        assert response_data["success"] is True
        assert "deleted successfully" in response_data["message"]


class TestFeatureValueRoutes:
    """Tests for feature value routes."""

    @patch("services.feature_store.routes.get_feature_store_service")
    @patch("services.feature_store.routes.get_cache")
    def test_write_feature_value_success(
        self, mock_get_cache, mock_get_service, client
    ):
        """Test successful feature value write."""
        # Mock response
        value_response = FeatureValueResponse(
            id=1,
            feature_name="user_age",
            entity_id="user_123",
            value=30,
            timestamp=datetime.utcnow(),
            version=1,
            created_at=datetime.utcnow(),
        )

        # Mock service
        mock_service = Mock()
        mock_service.write_feature_value = AsyncMock(return_value=value_response)
        mock_get_service.return_value = mock_service

        # Mock cache
        mock_cache = Mock()
        mock_cache.invalidate_feature_values = AsyncMock()
        mock_get_cache.return_value = mock_cache

        # Request data
        value_data = {
            "feature_name": "user_age",
            "entity_id": "user_123",
            "value": 30,
        }

        # Execute
        response = client.post("/api/v1/feature-store/features/values", json=value_data)

        # Verify
        assert response.status_code == 201
        response_data = response.json()
        assert response_data["success"] is True
        assert response_data["data"]["value"] == 30

    @patch("services.feature_store.routes.get_feature_store_service")
    def test_write_feature_value_validation_error(self, mock_get_service, client):
        """Test feature value write with validation error."""
        # Mock service to raise validation error
        mock_service = Mock()
        mock_service.write_feature_value = AsyncMock(
            side_effect=ValueError("Invalid value")
        )
        mock_get_service.return_value = mock_service

        # Request data
        value_data = {
            "feature_name": "user_age",
            "entity_id": "user_123",
            "value": "invalid",
        }

        # Execute
        response = client.post("/api/v1/feature-store/features/values", json=value_data)

        # Verify
        assert response.status_code == 400
        response_data = response.json()
        assert "Invalid value" in response_data["detail"]

    @patch("services.feature_store.routes.get_feature_store_service")
    @patch("services.feature_store.routes.get_cache")
    def test_write_feature_values_batch(self, mock_get_cache, mock_get_service, client):
        """Test batch feature value write."""
        # Mock response
        value_responses = [
            FeatureValueResponse(
                id=1,
                feature_name="user_age",
                entity_id="user_123",
                value=30,
                timestamp=datetime.utcnow(),
                version=1,
                created_at=datetime.utcnow(),
            ),
            FeatureValueResponse(
                id=2,
                feature_name="user_name",
                entity_id="user_123",
                value="John",
                timestamp=datetime.utcnow(),
                version=1,
                created_at=datetime.utcnow(),
            ),
        ]

        # Mock service
        mock_service = Mock()
        mock_service.write_feature_values_batch = AsyncMock(
            return_value=value_responses
        )
        mock_get_service.return_value = mock_service

        # Mock cache
        mock_cache = Mock()
        mock_cache.invalidate_feature_values = AsyncMock()
        mock_get_cache.return_value = mock_cache

        # Request data
        batch_data = {
            "values": [
                {
                    "feature_name": "user_age",
                    "entity_id": "user_123",
                    "value": 30,
                },
                {
                    "feature_name": "user_name",
                    "entity_id": "user_123",
                    "value": "John",
                },
            ]
        }

        # Execute
        response = client.post(
            "/api/v1/feature-store/features/values/batch", json=batch_data
        )

        # Verify
        assert response.status_code == 201
        response_data = response.json()
        assert response_data["success"] is True
        assert len(response_data["data"]) == 2

    @patch("services.feature_store.routes.get_feature_store_service")
    def test_read_feature_values(self, mock_get_service, client):
        """Test reading feature values."""
        # Mock response
        value_responses = [
            FeatureValueResponse(
                id=1,
                feature_name="user_age",
                entity_id="user_123",
                value=30,
                timestamp=datetime.utcnow(),
                version=1,
                created_at=datetime.utcnow(),
            ),
        ]

        # Mock service
        mock_service = Mock()
        mock_service.read_feature_values = AsyncMock(return_value=value_responses)
        mock_get_service.return_value = mock_service

        # Request data
        read_data = {
            "feature_names": ["user_age"],
            "entity_ids": ["user_123"],
        }

        # Execute
        response = client.post(
            "/api/v1/feature-store/features/values/read", json=read_data
        )

        # Verify
        assert response.status_code == 200
        response_data = response.json()
        assert response_data["success"] is True
        assert len(response_data["data"]) == 1


class TestFeatureServingRoutes:
    """Tests for feature serving routes."""

    @patch("services.feature_store.routes.get_feature_store_service")
    @patch("services.feature_store.routes.get_cache")
    def test_serve_features_cache_hit(self, mock_get_cache, mock_get_service, client):
        """Test serving features from cache."""
        # Mock cache hit
        cached_values = {
            "values": {"user_age": 30, "user_name": "John"},
            "timestamp": datetime.utcnow().isoformat(),
        }
        mock_cache = Mock()
        mock_cache.get_feature_values = AsyncMock(return_value=cached_values)
        mock_get_cache.return_value = mock_cache

        # Execute
        response = client.get(
            "/api/v1/feature-store/features/serve/user_123?feature_names=user_age&feature_names=user_name"
        )

        # Verify
        assert response.status_code == 200
        response_data = response.json()
        assert response_data["success"] is True
        assert response_data["data"]["entity_id"] == "user_123"
        assert response_data["data"]["features"]["user_age"] == 30

    @patch("services.feature_store.routes.get_feature_store_service")
    @patch("services.feature_store.routes.get_cache")
    def test_serve_features_cache_miss(self, mock_get_cache, mock_get_service, client):
        """Test serving features with cache miss."""
        # Mock cache miss
        mock_cache = Mock()
        mock_cache.get_feature_values = AsyncMock(return_value={})
        mock_cache.set_feature_values = AsyncMock()
        mock_get_cache.return_value = mock_cache

        # Mock service response
        serving_response = FeatureServingResponse(
            entity_id="user_123",
            features={"user_age": 30, "user_name": "John"},
            timestamp=datetime.utcnow(),
        )
        mock_service = Mock()
        mock_service.serve_features = AsyncMock(return_value=serving_response)
        mock_get_service.return_value = mock_service

        # Execute
        response = client.get(
            "/api/v1/feature-store/features/serve/user_123?feature_names=user_age&feature_names=user_name"
        )

        # Verify
        assert response.status_code == 200
        response_data = response.json()
        assert response_data["success"] is True
        assert response_data["data"]["entity_id"] == "user_123"


class TestFeatureDiscoveryRoutes:
    """Tests for feature discovery routes."""

    @patch("services.feature_store.routes.get_feature_store_service")
    def test_discover_features(self, mock_get_service, client):
        """Test feature discovery."""
        # Mock response
        discovery_response = FeatureDiscoveryResponse(
            feature_groups=["user_demographics", "user_behavior"],
            total_features=10,
            features_by_type={
                FeatureType.INTEGER: 5,
                FeatureType.STRING: 3,
                FeatureType.BOOLEAN: 2,
            },
            features_by_status={
                FeatureStatus.ACTIVE: 8,
                FeatureStatus.INACTIVE: 1,
                FeatureStatus.DEPRECATED: 1,
            },
        )

        # Mock service
        mock_service = Mock()
        mock_service.discover_features = AsyncMock(return_value=discovery_response)
        mock_get_service.return_value = mock_service

        # Execute
        response = client.get("/api/v1/feature-store/features/discover")

        # Verify
        assert response.status_code == 200
        response_data = response.json()
        assert response_data["success"] is True
        assert response_data["data"]["total_features"] == 10
        assert len(response_data["data"]["feature_groups"]) == 2


class TestHealthRoute:
    """Tests for health check route."""

    @patch("services.feature_store.routes.get_cache")
    def test_health_check_success(self, mock_get_cache, client):
        """Test successful health check."""
        # Mock cache health
        cache_status = {
            "redis": {"available": True, "latency_ms": 1.5},
            "memory": {"available": True, "size": 100},
        }
        mock_cache = Mock()
        mock_cache.health_check = AsyncMock(return_value=cache_status)
        mock_get_cache.return_value = mock_cache

        # Execute
        response = client.get("/api/v1/feature-store/health")

        # Verify
        assert response.status_code == 200
        response_data = response.json()
        assert response_data["success"] is True
        assert response_data["data"]["status"] == "healthy"
        assert response_data["data"]["cache"] == cache_status

    @patch("services.feature_store.routes.get_cache")
    def test_health_check_failure(self, mock_get_cache, client):
        """Test health check with failure."""
        # Mock cache health check failure
        mock_cache = Mock()
        mock_cache.health_check = AsyncMock(
            side_effect=Exception("Redis connection failed")
        )
        mock_get_cache.return_value = mock_cache

        # Execute
        response = client.get("/api/v1/feature-store/health")

        # Verify
        assert response.status_code == 200  # Still returns 200 but with failure status
        response_data = response.json()
        assert response_data["success"] is False
        assert response_data["data"]["status"] == "unhealthy"
