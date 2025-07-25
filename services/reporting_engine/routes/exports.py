"""Export and file management routes."""

import os
import time
from datetime import datetime
from typing import Any

from fastapi import APIRouter, Depends, HTTPException
from fastapi.responses import FileResponse
from sqlalchemy.ext.asyncio import AsyncSession

from libs.analytics_core.auth import get_current_user
from libs.analytics_core.database import get_db_session
from libs.api_common.response_models import APIMetadata, StandardResponse
from libs.observability import trace_function

from ..models import ExportRequest
from ..tasks import export_report

router = APIRouter()


@router.post(
    "/",
    response_model=StandardResponse[dict[str, Any]],
    summary="Export Data",
    description="Export data to specified format",
)
@trace_function("export_data")
async def export_data(
    request: ExportRequest,
    db: AsyncSession = Depends(get_db_session),
    current_user: dict = Depends(get_current_user),
) -> StandardResponse[dict[str, Any]]:
    """Export data to specified format."""

    try:
        # Generate filename if not provided
        if not request.filename:
            timestamp = int(time.time())
            user_id = current_user.get("user_id", "unknown")
            request.filename = f"export_{user_id}_{timestamp}"

        # Mock data based on data source
        if request.data_source == "analytics_db":
            export_data = _get_analytics_export_data(request.query, request.filters)
        elif request.data_source == "streaming_analytics":
            export_data = _get_streaming_export_data(request.query, request.filters)
        elif request.data_source == "data_quality":
            export_data = _get_data_quality_export_data(request.query, request.filters)
        else:
            raise HTTPException(
                status_code=400, detail=f"Unknown data source: {request.data_source}"
            )

        # Export to requested format
        result = export_report(export_data, request.format.value, request.filename)

        return StandardResponse(
            success=True,
            data={
                "file_path": result["file_path"],
                "file_size_bytes": result["file_size_bytes"],
                "format": request.format.value,
                "download_url": f"/api/v1/exports/download/{os.path.basename(result['file_path'])}",
            },
            message="Export completed successfully",
            metadata=APIMetadata(
                version="v1",
                timestamp=datetime.utcnow(),
            ),
        )

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Export failed: {str(e)}")


@router.get(
    "/download/{filename}",
    summary="Download File",
    description="Download an exported file",
)
@trace_function("download_file")
async def download_file(
    filename: str,
    current_user: dict = Depends(get_current_user),
):
    """Download an exported file."""
    try:
        file_path = os.path.join("exports", filename)

        if not os.path.exists(file_path):
            raise HTTPException(status_code=404, detail="File not found")

        # Determine media type based on file extension
        media_type_map = {
            ".pdf": "application/pdf",
            ".xlsx": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            ".xls": "application/vnd.ms-excel",
            ".csv": "text/csv",
            ".json": "application/json",
            ".html": "text/html",
        }

        file_ext = os.path.splitext(filename)[1].lower()
        media_type = media_type_map.get(file_ext, "application/octet-stream")

        return FileResponse(
            path=file_path,
            filename=filename,
            media_type=media_type,
        )

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Download failed: {str(e)}")


@router.get(
    "/formats",
    response_model=StandardResponse[dict[str, Any]],
    summary="Get Export Formats",
    description="Get available export formats and their capabilities",
)
@trace_function("get_export_formats")
async def get_export_formats(
    current_user: dict = Depends(get_current_user),
) -> StandardResponse[dict[str, Any]]:
    """Get available export formats."""

    try:
        formats = {
            "pdf": {
                "name": "PDF",
                "description": "Portable Document Format",
                "mime_type": "application/pdf",
                "supports_charts": True,
                "supports_tables": True,
                "supports_images": True,
                "file_extension": ".pdf",
            },
            "excel": {
                "name": "Excel",
                "description": "Microsoft Excel Spreadsheet",
                "mime_type": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                "supports_charts": False,
                "supports_tables": True,
                "supports_images": False,
                "file_extension": ".xlsx",
            },
            "csv": {
                "name": "CSV",
                "description": "Comma Separated Values",
                "mime_type": "text/csv",
                "supports_charts": False,
                "supports_tables": True,
                "supports_images": False,
                "file_extension": ".csv",
            },
            "json": {
                "name": "JSON",
                "description": "JavaScript Object Notation",
                "mime_type": "application/json",
                "supports_charts": False,
                "supports_tables": False,
                "supports_images": False,
                "file_extension": ".json",
            },
            "html": {
                "name": "HTML",
                "description": "HyperText Markup Language",
                "mime_type": "text/html",
                "supports_charts": True,
                "supports_tables": True,
                "supports_images": True,
                "file_extension": ".html",
            },
        }

        return StandardResponse(
            success=True,
            data=formats,
            metadata=APIMetadata(
                version="v1",
                timestamp=datetime.utcnow(),
            ),
        )

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to get formats: {str(e)}")


@router.get(
    "/templates",
    response_model=StandardResponse[dict[str, Any]],
    summary="Get Export Templates",
    description="Get predefined export templates",
)
@trace_function("get_export_templates")
async def get_export_templates(
    current_user: dict = Depends(get_current_user),
) -> StandardResponse[dict[str, Any]]:
    """Get predefined export templates."""

    try:
        templates = {
            "user_analytics": {
                "name": "User Analytics Report",
                "description": "Comprehensive user behavior and analytics report",
                "data_source": "analytics_db",
                "query": {
                    "tables": ["users", "sessions", "events"],
                    "metrics": [
                        "total_users",
                        "active_users",
                        "session_duration",
                        "bounce_rate",
                    ],
                    "date_range": "30d",
                },
                "recommended_formats": ["pdf", "excel"],
                "default_format": "pdf",
            },
            "data_quality_summary": {
                "name": "Data Quality Summary",
                "description": "Data quality metrics and violations summary",
                "data_source": "data_quality",
                "query": {
                    "metrics": [
                        "overall_score",
                        "violations_count",
                        "datasets_monitored",
                    ],
                    "include_details": True,
                },
                "recommended_formats": ["pdf", "excel", "csv"],
                "default_format": "excel",
            },
            "streaming_performance": {
                "name": "Streaming Performance Report",
                "description": "Real-time streaming analytics performance metrics",
                "data_source": "streaming_analytics",
                "query": {
                    "metrics": ["throughput", "latency", "error_rate", "uptime"],
                    "time_period": "24h",
                    "granularity": "hourly",
                },
                "recommended_formats": ["pdf", "csv"],
                "default_format": "pdf",
            },
            "ab_test_results": {
                "name": "A/B Test Results",
                "description": "Statistical analysis of A/B test experiments",
                "data_source": "analytics_db",
                "query": {
                    "tables": ["experiments", "variants", "conversions"],
                    "include_statistical_analysis": True,
                    "confidence_level": 0.95,
                },
                "recommended_formats": ["pdf", "excel"],
                "default_format": "pdf",
            },
        }

        return StandardResponse(
            success=True,
            data=templates,
            metadata=APIMetadata(
                version="v1",
                timestamp=datetime.utcnow(),
            ),
        )

    except Exception as e:
        raise HTTPException(
            status_code=500, detail=f"Failed to get templates: {str(e)}"
        )


def _get_analytics_export_data(query: dict, filters: dict) -> dict[str, Any]:
    """Get mock analytics data for export."""
    import random
    from datetime import datetime, timedelta

    # Generate mock data
    users_data = []
    for i in range(100):
        users_data.append(
            {
                "user_id": f"user_{i:04d}",
                "email": f"user{i}@example.com",
                "signup_date": (
                    datetime.now() - timedelta(days=random.randint(1, 365))
                ).strftime("%Y-%m-%d"),
                "total_sessions": random.randint(1, 50),
                "total_events": random.randint(10, 500),
                "last_active": (
                    datetime.now() - timedelta(days=random.randint(0, 30))
                ).strftime("%Y-%m-%d"),
            }
        )

    return {
        "title": "User Analytics Export",
        "summary": {
            "total_users": len(users_data),
            "date_range": "Last 30 days",
            "export_date": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        },
        "results": users_data,
    }


def _get_streaming_export_data(query: dict, filters: dict) -> dict[str, Any]:
    """Get mock streaming data for export."""
    import random
    from datetime import datetime, timedelta

    # Generate mock streaming metrics
    metrics_data = []
    for i in range(24):  # 24 hours of data
        timestamp = datetime.now() - timedelta(hours=i)
        metrics_data.append(
            {
                "timestamp": timestamp.strftime("%Y-%m-%d %H:%M:%S"),
                "events_per_second": random.randint(1000, 5000),
                "avg_latency_ms": round(random.uniform(10, 100), 2),
                "error_rate": round(random.uniform(0.001, 0.01), 4),
                "queue_depth": random.randint(0, 1000),
                "cpu_usage": round(random.uniform(0.3, 0.9), 2),
                "memory_usage": round(random.uniform(0.4, 0.8), 2),
            }
        )

    return {
        "title": "Streaming Performance Export",
        "summary": {
            "time_period": "Last 24 hours",
            "avg_throughput": sum(m["events_per_second"] for m in metrics_data)
            / len(metrics_data),
            "avg_latency": sum(m["avg_latency_ms"] for m in metrics_data)
            / len(metrics_data),
            "export_date": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        },
        "results": metrics_data,
    }


def _get_data_quality_export_data(query: dict, filters: dict) -> dict[str, Any]:
    """Get mock data quality data for export."""
    import random
    from datetime import datetime

    # Generate mock data quality results
    datasets = ["users", "orders", "products", "sessions", "events"]
    quality_data = []

    for dataset in datasets:
        quality_data.append(
            {
                "dataset": dataset,
                "quality_score": round(random.uniform(0.8, 0.99), 3),
                "total_records": random.randint(1000, 100000),
                "null_violations": random.randint(0, 100),
                "format_violations": random.randint(0, 50),
                "range_violations": random.randint(0, 25),
                "last_checked": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "status": random.choice(["passed", "warning", "failed"]),
            }
        )

    return {
        "title": "Data Quality Export",
        "summary": {
            "total_datasets": len(datasets),
            "avg_quality_score": sum(d["quality_score"] for d in quality_data)
            / len(quality_data),
            "total_violations": sum(
                d["null_violations"] + d["format_violations"] + d["range_violations"]
                for d in quality_data
            ),
            "export_date": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        },
        "results": quality_data,
    }
