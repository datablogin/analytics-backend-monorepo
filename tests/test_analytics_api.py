"""Tests for analytics API service."""

import pytest
from fastapi.testclient import TestClient

from services.analytics_api.main import app


@pytest.fixture
def client():
    """Create test client for analytics API."""
    return TestClient(app)


def test_health_check(client):
    """Test health check endpoint."""
    response = client.get("/health")
    assert response.status_code == 200

    data = response.json()
    assert data["success"] is True
    assert data["message"] == "Service is healthy"
    assert "data" in data
    assert data["data"]["status"] == "healthy"
    assert "metadata" in data


def test_app_metadata():
    """Test FastAPI app metadata."""
    assert app.title == "Analytics API"
    assert (
        app.description
        == "Comprehensive Analytics backend REST API with automatic OpenAPI documentation, standardized responses, JWT authentication, RBAC, and database migrations"
    )
    assert app.version == "1.0.0"
