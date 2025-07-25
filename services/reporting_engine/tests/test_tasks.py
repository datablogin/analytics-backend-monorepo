"""Tests for Celery tasks."""

import os
import tempfile
from unittest.mock import Mock, patch

import pytest

from services.reporting_engine.tasks import (
    _export_to_csv,
    _export_to_json,
    _export_to_pdf,
    _generate_analytics_dashboard_report,
    _generate_data_quality_report,
    export_report,
    generate_report,
)


def test_generate_data_quality_report():
    """Test data quality report generation."""
    config = {
        "title": "Test Data Quality Report",
        "datasets": ["users", "orders"],
        "quality_checks": ["null_check", "format_check"],
    }
    filters = {}

    result = _generate_data_quality_report(config, filters)

    assert result["title"] == "Test Data Quality Report"
    assert "summary" in result
    assert "results" in result
    assert len(result["results"]) == 2  # Two datasets
    assert "generated_at" in result


def test_generate_analytics_dashboard_report():
    """Test analytics dashboard report generation."""
    config = {
        "title": "Test Analytics Dashboard",
        "metrics": ["users", "sessions"],
        "visualizations": ["line", "bar"],
    }
    filters = {}

    result = _generate_analytics_dashboard_report(config, filters)

    assert result["title"] == "Test Analytics Dashboard"
    assert "metrics" in result
    assert "charts" in result
    assert len(result["charts"]) == 2  # Two visualizations
    assert "generated_at" in result


def test_export_to_pdf():
    """Test PDF export functionality."""
    report_data = {
        "title": "Test Report",
        "summary": {
            "total_records": 1000,
            "quality_score": 0.95,
        },
        "results": [
            {"dataset": "users", "score": 0.98},
            {"dataset": "orders", "score": 0.92},
        ],
        "generated_at": "2023-01-01T00:00:00Z",
    }

    with tempfile.TemporaryDirectory() as temp_dir:
        file_path = os.path.join(temp_dir, "test_report")

        result_path = _export_to_pdf(report_data, file_path)

        assert result_path.endswith('.pdf')
        assert os.path.exists(result_path)
        assert os.path.getsize(result_path) > 0


def test_export_to_csv():
    """Test CSV export functionality."""
    report_data = {
        "title": "Test Report",
        "results": [
            {"dataset": "users", "score": 0.98, "records": 1000},
            {"dataset": "orders", "score": 0.92, "records": 500},
        ],
    }

    with tempfile.TemporaryDirectory() as temp_dir:
        file_path = os.path.join(temp_dir, "test_report")

        result_path = _export_to_csv(report_data, file_path)

        assert result_path.endswith('.csv')
        assert os.path.exists(result_path)
        assert os.path.getsize(result_path) > 0

        # Check CSV content
        with open(result_path) as f:
            content = f.read()
            assert "dataset" in content
            assert "users" in content
            assert "orders" in content


def test_export_to_json():
    """Test JSON export functionality."""
    report_data = {
        "title": "Test Report",
        "summary": {"total": 100},
        "results": [{"id": 1, "value": "test"}],
    }

    with tempfile.TemporaryDirectory() as temp_dir:
        file_path = os.path.join(temp_dir, "test_report")

        result_path = _export_to_json(report_data, file_path)

        assert result_path.endswith('.json')
        assert os.path.exists(result_path)
        assert os.path.getsize(result_path) > 0

        # Check JSON content
        import json
        with open(result_path) as f:
            data = json.load(f)
            assert data["title"] == "Test Report"
            assert "summary" in data
            assert "results" in data


@patch('services.reporting_engine.tasks.current_task')
def test_generate_report_task(mock_current_task):
    """Test the generate_report Celery task."""
    mock_current_task.update_state = Mock()

    report_config = {
        "report_type": "data_quality",
        "config": {
            "title": "Test Report",
            "datasets": ["users"],
            "quality_checks": ["null_check"],
        },
        "filters": {},
    }

    # Mock the task execution
    with patch('services.reporting_engine.tasks._generate_data_quality_report') as mock_generate:
        mock_generate.return_value = {
            "title": "Test Report",
            "summary": {"total": 100},
            "results": [],
        }

        result = generate_report("test-report-id", report_config)

        assert result["status"] == "success"
        assert result["report_id"] == "test-report-id"
        assert "data" in result
        assert "processing_time_ms" in result

        # Verify task state updates were called
        assert mock_current_task.update_state.called


@patch('services.reporting_engine.tasks.current_task')
def test_export_report_task(mock_current_task):
    """Test the export_report Celery task."""
    mock_current_task.update_state = Mock()

    report_data = {
        "title": "Test Report",
        "results": [{"id": 1, "value": "test"}],
    }

    with tempfile.TemporaryDirectory() as temp_dir:
        # Change to temp directory for the test
        original_cwd = os.getcwd()
        os.chdir(temp_dir)

        try:
            result = export_report(report_data, "json", "test_export")

            assert result["status"] == "success"
            assert "file_path" in result
            assert "file_size_bytes" in result
            assert "processing_time_ms" in result

            # Verify file was created
            assert os.path.exists(result["file_path"])

        finally:
            os.chdir(original_cwd)


def test_generate_report_invalid_type():
    """Test generate_report with invalid report type."""
    report_config = {
        "report_type": "invalid_type",
        "config": {},
        "filters": {},
    }

    with pytest.raises(ValueError):
        generate_report("test-report-id", report_config)


def test_export_report_invalid_format():
    """Test export_report with invalid format."""
    report_data = {"title": "Test"}

    with pytest.raises(ValueError):
        export_report(report_data, "invalid_format", "test_file")
