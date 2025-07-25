"""Tests for report management functionality."""

from unittest.mock import Mock, patch

import pytest
from fastapi.testclient import TestClient

from services.reporting_engine.main import app
from services.reporting_engine.models import ReportStatus, ReportType


@pytest.fixture
def client():
    """Test client fixture."""
    return TestClient(app)


@pytest.fixture
def mock_current_user():
    """Mock current user for testing."""
    return {"user_id": "test_user", "email": "test@example.com"}


@pytest.fixture
def mock_db_session():
    """Mock database session."""
    mock_session = Mock()
    mock_session.commit = Mock()
    mock_session.rollback = Mock()
    mock_session.refresh = Mock()
    mock_session.add = Mock()
    mock_session.delete = Mock()
    mock_session.execute = Mock()
    return mock_session


@patch("services.reporting_engine.main.get_current_user")
@patch("services.reporting_engine.main.get_db_session")
def test_create_report(mock_db, mock_user, client, mock_current_user, mock_db_session):
    """Test creating a new report."""
    mock_user.return_value = mock_current_user
    mock_db.return_value = mock_db_session

    # Mock the report creation
    with patch("services.reporting_engine.routes.reports.Report") as mock_report_class:
        mock_report = Mock()
        mock_report.id = "test-report-id"
        mock_report.name = "Test Report"
        mock_report.report_type = ReportType.DATA_QUALITY
        mock_report.status = ReportStatus.PENDING
        mock_report.created_by = "test_user"
        mock_report_class.return_value = mock_report

        response = client.post(
            "/api/v1/reports/",
            json={
                "name": "Test Report",
                "description": "A test report",
                "report_type": "data_quality",
                "config": {
                    "datasets": ["users", "orders"],
                    "quality_checks": ["null_check", "format_check"]
                },
                "filters": {},
                "frequency": "once"
            }
        )

        assert response.status_code == 200
        data = response.json()
        assert data["success"] is True
        assert "data" in data


@patch("services.reporting_engine.main.get_current_user")
@patch("services.reporting_engine.main.get_db_session")
def test_list_reports(mock_db, mock_user, client, mock_current_user, mock_db_session):
    """Test listing reports."""
    mock_user.return_value = mock_current_user
    mock_db.return_value = mock_db_session

    # Mock database query result
    mock_result = Mock()
    mock_result.scalars.return_value.all.return_value = []
    mock_db_session.execute.return_value = mock_result

    response = client.get("/api/v1/reports/")

    assert response.status_code == 200
    data = response.json()
    assert data["success"] is True
    assert "data" in data
    assert isinstance(data["data"], list)


@patch("services.reporting_engine.main.get_current_user")
@patch("services.reporting_engine.main.get_db_session")
def test_get_report(mock_db, mock_user, client, mock_current_user, mock_db_session):
    """Test getting a specific report."""
    mock_user.return_value = mock_current_user
    mock_db.return_value = mock_db_session

    # Mock report
    mock_report = Mock()
    mock_report.id = "test-report-id"
    mock_report.name = "Test Report"
    mock_report.report_type = ReportType.DATA_QUALITY
    mock_report.status = ReportStatus.PENDING
    mock_report.created_by = "test_user"

    mock_result = Mock()
    mock_result.scalar_one_or_none.return_value = mock_report
    mock_db_session.execute.return_value = mock_result

    response = client.get("/api/v1/reports/test-report-id")

    assert response.status_code == 200
    data = response.json()
    assert data["success"] is True
    assert "data" in data


@patch("services.reporting_engine.main.get_current_user")
@patch("services.reporting_engine.main.get_db_session")
def test_execute_report_async(mock_db, mock_user, client, mock_current_user, mock_db_session):
    """Test executing a report asynchronously."""
    mock_user.return_value = mock_current_user
    mock_db.return_value = mock_db_session

    # Mock report
    mock_report = Mock()
    mock_report.id = "test-report-id"
    mock_report.report_type = ReportType.DATA_QUALITY
    mock_report.config = {"datasets": ["users"]}
    mock_report.filters = {}

    mock_result = Mock()
    mock_result.scalar_one_or_none.return_value = mock_report
    mock_db_session.execute.return_value = mock_result

    # Mock execution creation
    with patch("services.reporting_engine.routes.reports.ReportExecution") as mock_execution_class:
        mock_execution = Mock()
        mock_execution.id = "test-execution-id"
        mock_execution.status = ReportStatus.PENDING
        mock_execution_class.return_value = mock_execution

        # Mock Celery task
        with patch("services.reporting_engine.routes.reports.generate_report") as mock_task:
            mock_task.delay.return_value.id = "test-task-id"

            response = client.post(
                "/api/v1/reports/test-report-id/execute",
                json={
                    "format": "pdf",
                    "async_execution": True
                }
            )

            assert response.status_code == 200
            data = response.json()
            assert data["success"] is True
            assert "data" in data


def test_health_check(client):
    """Test health check endpoint."""
    with patch("services.reporting_engine.main.get_db_session") as mock_db:
        mock_session = Mock()
        mock_session.execute = Mock()
        mock_db.return_value = mock_session

        with patch("services.reporting_engine.main.celery_app") as mock_celery:
            mock_inspect = Mock()
            mock_inspect.active_queues.return_value = {"worker1": []}
            mock_celery.control.inspect.return_value = mock_inspect

            response = client.get("/health")

            assert response.status_code == 200
            data = response.json()
            assert data["success"] is True
            assert "data" in data
            assert "status" in data["data"]


def test_root_endpoint(client):
    """Test root endpoint."""
    response = client.get("/")

    assert response.status_code == 200
    data = response.json()
    assert "service" in data
    assert data["service"] == "Reporting Engine API"
