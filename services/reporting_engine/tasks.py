"""Celery tasks for async report generation."""

import os
import time
from datetime import datetime
from typing import Any

import pandas as pd
import structlog
from celery import current_task
from reportlab.lib import colors
from reportlab.lib.pagesizes import letter
from reportlab.lib.styles import ParagraphStyle, getSampleStyleSheet
from reportlab.platypus import Paragraph, SimpleDocTemplate, Spacer, Table, TableStyle

from .celery_app import celery_app
from .models import ReportFormat, ReportType

logger = structlog.get_logger(__name__)


@celery_app.task(bind=True)
def generate_report(self, report_id: str, report_config: dict[str, Any]) -> dict[str, Any]:
    """Generate a report asynchronously."""
    try:
        logger.info("Starting report generation", report_id=report_id)
        start_time = time.time()

        # Update task status
        current_task.update_state(
            state="IN_PROGRESS",
            meta={"report_id": report_id, "progress": 0}
        )

        # Get report configuration
        report_type = ReportType(report_config["report_type"])
        config = report_config["config"]
        filters = report_config.get("filters", {})

        # Generate report data based on type
        if report_type == ReportType.DATA_QUALITY:
            report_data = _generate_data_quality_report(config, filters)
        elif report_type == ReportType.ANALYTICS_DASHBOARD:
            report_data = _generate_analytics_dashboard_report(config, filters)
        elif report_type == ReportType.AB_TEST_RESULTS:
            report_data = _generate_ab_test_report(config, filters)
        elif report_type == ReportType.ML_MODEL_PERFORMANCE:
            report_data = _generate_ml_performance_report(config, filters)
        elif report_type == ReportType.STREAMING_METRICS:
            report_data = _generate_streaming_metrics_report(config, filters)
        else:
            report_data = _generate_custom_report(config, filters)

        current_task.update_state(
            state="IN_PROGRESS",
            meta={"report_id": report_id, "progress": 50}
        )

        processing_time = round((time.time() - start_time) * 1000, 2)

        logger.info(
            "Report generation completed",
            report_id=report_id,
            processing_time_ms=processing_time
        )

        return {
            "status": "success",
            "report_id": report_id,
            "data": report_data,
            "processing_time_ms": processing_time,
            "generated_at": datetime.utcnow().isoformat(),
        }

    except Exception as e:
        logger.error("Report generation failed", report_id=report_id, error=str(e))
        current_task.update_state(
            state="FAILURE",
            meta={"report_id": report_id, "error": str(e)}
        )
        raise


@celery_app.task(bind=True)
def export_report(
    self,
    report_data: dict[str, Any],
    format: str,
    filename: str
) -> dict[str, Any]:
    """Export report to specified format."""
    try:
        logger.info("Starting report export", format=format, filename=filename)
        start_time = time.time()

        current_task.update_state(
            state="IN_PROGRESS",
            meta={"format": format, "progress": 0}
        )

        # Create exports directory if it doesn't exist
        exports_dir = "exports"
        os.makedirs(exports_dir, exist_ok=True)

        file_path = os.path.join(exports_dir, filename)

        if format == ReportFormat.PDF:
            file_path = _export_to_pdf(report_data, file_path)
        elif format == ReportFormat.EXCEL:
            file_path = _export_to_excel(report_data, file_path)
        elif format == ReportFormat.CSV:
            file_path = _export_to_csv(report_data, file_path)
        elif format == ReportFormat.JSON:
            file_path = _export_to_json(report_data, file_path)
        elif format == ReportFormat.HTML:
            file_path = _export_to_html(report_data, file_path)
        else:
            raise ValueError(f"Unsupported format: {format}")

        file_size = os.path.getsize(file_path)
        processing_time = round((time.time() - start_time) * 1000, 2)

        logger.info(
            "Report export completed",
            file_path=file_path,
            file_size_bytes=file_size,
            processing_time_ms=processing_time
        )

        return {
            "status": "success",
            "file_path": file_path,
            "file_size_bytes": file_size,
            "processing_time_ms": processing_time,
            "exported_at": datetime.utcnow().isoformat(),
        }

    except Exception as e:
        logger.error("Report export failed", format=format, error=str(e))
        current_task.update_state(
            state="FAILURE",
            meta={"format": format, "error": str(e)}
        )
        raise


def _generate_data_quality_report(config: dict, filters: dict) -> dict[str, Any]:
    """Generate data quality report."""
    logger.info("Generating data quality report", config=config)

    datasets = config.get("datasets", [])
    quality_checks = config.get("quality_checks", [])

    results = []

    for dataset in datasets:
        try:
            # Run quality checks for each dataset
            # This would typically connect to actual data sources
            quality_result = {
                "dataset": dataset,
                "checks_performed": quality_checks,
                "overall_score": 0.85,  # Mock score
                "violations": [],
                "recommendations": [],
                "timestamp": datetime.utcnow().isoformat(),
            }
            results.append(quality_result)
        except Exception as e:
            logger.error(f"Failed to check quality for dataset {dataset}", error=str(e))

    return {
        "title": config.get("title", "Data Quality Report"),
        "summary": {
            "total_datasets": len(datasets),
            "avg_quality_score": 0.85,
            "total_violations": 0,
        },
        "results": results,
        "generated_at": datetime.utcnow().isoformat(),
    }


def _generate_analytics_dashboard_report(config: dict, filters: dict) -> dict[str, Any]:
    """Generate analytics dashboard report."""
    logger.info("Generating analytics dashboard report", config=config)

    visualizations = config.get("visualizations", [])

    # Mock analytics data
    dashboard_data = {
        "title": config.get("title", "Analytics Dashboard"),
        "metrics": {
            "total_users": 15420,
            "active_sessions": 1250,
            "conversion_rate": 3.2,
            "revenue": 45680.50,
        },
        "charts": [],
        "time_period": config.get("time_granularity", "daily"),
        "generated_at": datetime.utcnow().isoformat(),
    }

    # Generate chart data for each visualization
    for viz in visualizations:
        chart_data = {
            "type": viz,
            "title": f"{viz.title()} Chart",
            "data": _generate_mock_chart_data(viz),
        }
        dashboard_data["charts"].append(chart_data)

    return dashboard_data


def _generate_ab_test_report(config: dict, filters: dict) -> dict[str, Any]:
    """Generate A/B test results report."""
    logger.info("Generating A/B test report", config=config)

    experiment_ids = config.get("experiment_ids", [])
    confidence_level = config.get("confidence_level", 0.95)

    results = []

    for exp_id in experiment_ids:
        # Mock A/B test results
        test_result = {
            "experiment_id": exp_id,
            "name": f"Experiment {exp_id}",
            "status": "completed",
            "variants": {
                "control": {
                    "users": 5000,
                    "conversions": 150,
                    "conversion_rate": 0.03,
                },
                "treatment": {
                    "users": 5000,
                    "conversions": 175,
                    "conversion_rate": 0.035,
                },
            },
            "statistical_significance": True,
            "confidence_level": confidence_level,
            "p_value": 0.023,
            "recommendation": "Launch treatment variant",
        }
        results.append(test_result)

    return {
        "title": config.get("title", "A/B Test Results"),
        "summary": {
            "total_experiments": len(experiment_ids),
            "significant_results": len([r for r in results if r["statistical_significance"]]),
        },
        "results": results,
        "generated_at": datetime.utcnow().isoformat(),
    }


def _generate_ml_performance_report(config: dict, filters: dict) -> dict[str, Any]:
    """Generate ML model performance report."""
    logger.info("Generating ML performance report", config=config)

    model_names = config.get("model_names", [])

    results = []

    for model_name in model_names:
        # Mock ML performance data
        performance = {
            "model_name": model_name,
            "version": "1.2.0",
            "metrics": {
                "accuracy": 0.892,
                "precision": 0.875,
                "recall": 0.901,
                "f1_score": 0.888,
                "auc_roc": 0.934,
            },
            "drift_detection": {
                "feature_drift": 0.02,
                "prediction_drift": 0.015,
                "status": "stable",
            },
            "performance_trend": "stable",
            "last_updated": datetime.utcnow().isoformat(),
        }
        results.append(performance)

    return {
        "title": config.get("title", "ML Model Performance Report"),
        "summary": {
            "total_models": len(model_names),
            "avg_accuracy": 0.892,
            "models_with_drift": 0,
        },
        "results": results,
        "generated_at": datetime.utcnow().isoformat(),
    }


def _generate_streaming_metrics_report(config: dict, filters: dict) -> dict[str, Any]:
    """Generate streaming metrics report."""
    logger.info("Generating streaming metrics report", config=config)

    services = config.get("services", [])

    # Mock streaming metrics data
    results = {
        "title": config.get("title", "Streaming Metrics Report"),
        "services": {},
        "overall_health": "healthy",
        "generated_at": datetime.utcnow().isoformat(),
    }

    for service in services:
        service_metrics = {
            "throughput": 15420,  # events/sec
            "latency_p95": 45,   # milliseconds
            "error_rate": 0.001,  # 0.1%
            "uptime": 99.95,     # percentage
            "queue_depth": 125,
            "consumer_lag": 0.5,  # seconds
        }
        results["services"][service] = service_metrics

    return results


def _generate_custom_report(config: dict, filters: dict) -> dict[str, Any]:
    """Generate custom report."""
    logger.info("Generating custom report", config=config)

    return {
        "title": config.get("title", "Custom Report"),
        "data": config.get("data", {}),
        "filters": filters,
        "generated_at": datetime.utcnow().isoformat(),
    }


def _generate_mock_chart_data(chart_type: str) -> dict[str, Any]:
    """Generate mock chart data."""
    if chart_type == "line":
        return {
            "labels": ["Jan", "Feb", "Mar", "Apr", "May", "Jun"],
            "datasets": [{
                "label": "Users",
                "data": [1200, 1350, 1180, 1420, 1650, 1480],
            }]
        }
    elif chart_type == "bar":
        return {
            "labels": ["Desktop", "Mobile", "Tablet"],
            "datasets": [{
                "label": "Sessions",
                "data": [3200, 2800, 450],
            }]
        }
    else:
        return {"message": f"Mock data for {chart_type}"}


def _export_to_pdf(report_data: dict, file_path: str) -> str:
    """Export report to PDF format."""
    if not file_path.endswith('.pdf'):
        file_path += '.pdf'

    doc = SimpleDocTemplate(file_path, pagesize=letter)
    styles = getSampleStyleSheet()
    story = []

    # Title
    title_style = ParagraphStyle(
        'CustomTitle',
        parent=styles['Heading1'],
        fontSize=24,
        spaceAfter=30,
        alignment=1,  # Center alignment
    )
    story.append(Paragraph(report_data.get("title", "Report"), title_style))
    story.append(Spacer(1, 12))

    # Summary section
    if "summary" in report_data:
        story.append(Paragraph("Summary", styles['Heading2']))
        for key, value in report_data["summary"].items():
            story.append(Paragraph(f"<b>{key.replace('_', ' ').title()}:</b> {value}", styles['Normal']))
        story.append(Spacer(1, 12))

    # Results section
    if "results" in report_data:
        story.append(Paragraph("Results", styles['Heading2']))

        # Create table for results if it's a list
        if isinstance(report_data["results"], list) and report_data["results"]:
            first_result = report_data["results"][0]
            if isinstance(first_result, dict):
                headers = list(first_result.keys())
                data = [headers]

                for result in report_data["results"][:10]:  # Limit to first 10 results
                    row = [str(result.get(header, "")) for header in headers]
                    data.append(row)

                table = Table(data)
                table.setStyle(TableStyle([
                    ('BACKGROUND', (0, 0), (-1, 0), colors.grey),
                    ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
                    ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
                    ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
                    ('FONTSIZE', (0, 0), (-1, 0), 14),
                    ('BOTTOMPADDING', (0, 0), (-1, 0), 12),
                    ('BACKGROUND', (0, 1), (-1, -1), colors.beige),
                    ('GRID', (0, 0), (-1, -1), 1, colors.black)
                ]))
                story.append(table)

    # Metadata
    story.append(Spacer(1, 20))
    story.append(Paragraph("Report Information", styles['Heading3']))
    story.append(Paragraph(f"Generated at: {report_data.get('generated_at', 'Unknown')}", styles['Normal']))

    doc.build(story)
    return file_path


def _export_to_excel(report_data: dict, file_path: str) -> str:
    """Export report to Excel format."""
    if not file_path.endswith(('.xlsx', '.xls')):
        file_path += '.xlsx'

    with pd.ExcelWriter(file_path, engine='openpyxl') as writer:
        # Summary sheet
        if "summary" in report_data:
            summary_df = pd.DataFrame(
                list(report_data["summary"].items()),
                columns=['Metric', 'Value']
            )
            summary_df.to_excel(writer, sheet_name='Summary', index=False)

        # Results sheet
        if "results" in report_data:
            results = report_data["results"]
            if isinstance(results, list) and results:
                results_df = pd.DataFrame(results)
                results_df.to_excel(writer, sheet_name='Results', index=False)
            elif isinstance(results, dict):
                results_df = pd.DataFrame([results])
                results_df.to_excel(writer, sheet_name='Results', index=False)

    return file_path


def _export_to_csv(report_data: dict, file_path: str) -> str:
    """Export report to CSV format."""
    if not file_path.endswith('.csv'):
        file_path += '.csv'

    # Export results if available
    if "results" in report_data:
        results = report_data["results"]
        if isinstance(results, list) and results:
            df = pd.DataFrame(results)
            df.to_csv(file_path, index=False)
        elif isinstance(results, dict):
            df = pd.DataFrame([results])
            df.to_csv(file_path, index=False)
    else:
        # Export summary or entire data
        df = pd.DataFrame([report_data])
        df.to_csv(file_path, index=False)

    return file_path


def _export_to_json(report_data: dict, file_path: str) -> str:
    """Export report to JSON format."""
    import json

    if not file_path.endswith('.json'):
        file_path += '.json'

    with open(file_path, 'w') as f:
        json.dump(report_data, f, indent=2, default=str)

    return file_path


def _export_to_html(report_data: dict, file_path: str) -> str:
    """Export report to HTML format."""
    if not file_path.endswith('.html'):
        file_path += '.html'

    html_content = f"""
    <!DOCTYPE html>
    <html>
    <head>
        <title>{report_data.get('title', 'Report')}</title>
        <style>
            body {{ font-family: Arial, sans-serif; margin: 40px; }}
            h1 {{ color: #333; }}
            h2 {{ color: #666; }}
            table {{ border-collapse: collapse; width: 100%; margin: 20px 0; }}
            th, td {{ border: 1px solid #ddd; padding: 8px; text-align: left; }}
            th {{ background-color: #f2f2f2; }}
            .summary {{ background-color: #f9f9f9; padding: 20px; margin: 20px 0; }}
        </style>
    </head>
    <body>
        <h1>{report_data.get('title', 'Report')}</h1>

        <div class="summary">
            <h2>Summary</h2>
            <ul>
    """

    if "summary" in report_data:
        for key, value in report_data["summary"].items():
            html_content += f"<li><strong>{key.replace('_', ' ').title()}:</strong> {value}</li>"

    html_content += """
            </ul>
        </div>

        <h2>Results</h2>
    """

    if "results" in report_data:
        results = report_data["results"]
        if isinstance(results, list) and results:
            # Create table
            html_content += "<table><tr>"

            # Headers
            if isinstance(results[0], dict):
                for header in results[0].keys():
                    html_content += f"<th>{header.replace('_', ' ').title()}</th>"
                html_content += "</tr>"

                # Rows
                for result in results:
                    html_content += "<tr>"
                    for value in result.values():
                        html_content += f"<td>{value}</td>"
                    html_content += "</tr>"

            html_content += "</table>"

    html_content += f"""
        <p><small>Generated at: {report_data.get('generated_at', 'Unknown')}</small></p>
    </body>
    </html>
    """

    with open(file_path, 'w') as f:
        f.write(html_content)

    return file_path
