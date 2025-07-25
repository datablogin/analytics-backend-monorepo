"""Test configuration for reporting engine."""

from unittest.mock import Mock

import pytest


@pytest.fixture
def mock_database():
    """Mock database session for testing."""
    mock_db = Mock()
    mock_db.commit = Mock()
    mock_db.rollback = Mock()
    mock_db.refresh = Mock()
    mock_db.add = Mock()
    mock_db.delete = Mock()
    return mock_db

@pytest.fixture
def sample_report_data():
    """Sample report data for testing."""
    return {
        "title": "Test Report",
        "summary": {
            "total_records": 1000,
            "quality_score": 0.95,
        },
        "results": [
            {"dataset": "users", "score": 0.98, "records": 1000},
            {"dataset": "orders", "score": 0.92, "records": 500},
        ],
        "generated_at": "2023-01-01T00:00:00Z",
    }
