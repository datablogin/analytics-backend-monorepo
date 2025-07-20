"""Pytest configuration and shared fixtures."""

import os

import pytest


@pytest.fixture(scope="session")
def test_database_url():
    """Database URL for testing."""
    return os.getenv(
        "DATABASE_URL", "postgresql://postgres:password@localhost:5432/analytics_test"
    )


@pytest.fixture(scope="session")
def test_redis_url():
    """Redis URL for testing."""
    return os.getenv("REDIS_URL", "redis://localhost:6379")


@pytest.fixture
def mock_env_vars(monkeypatch):
    """Mock environment variables for testing."""
    monkeypatch.setenv("DATABASE_URL", "sqlite:///:memory:")
    monkeypatch.setenv("REDIS_URL", "redis://localhost:6379")
    monkeypatch.setenv("ENV", "test")
