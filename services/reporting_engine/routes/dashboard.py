"""Dashboard and visualization routes."""

import time
from datetime import datetime
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, WebSocket, WebSocketDisconnect
from sqlalchemy.ext.asyncio import AsyncSession

from libs.analytics_core.auth import get_current_user
from libs.analytics_core.database import get_db_session
from libs.api_common.response_models import APIMetadata, StandardResponse
from libs.observability import trace_function

from ..models import (
    DashboardConfig,
    DashboardCreateRequest,
)

router = APIRouter()


# WebSocket connection manager for real-time updates
class ConnectionManager:
    """Manage WebSocket connections for real-time dashboard updates."""

    def __init__(self):
        self.active_connections: list[WebSocket] = []
        self.dashboard_subscribers: dict[str, list[WebSocket]] = {}

    async def connect(self, websocket: WebSocket, dashboard_id: str = None):
        """Accept a WebSocket connection."""
        await websocket.accept()
        self.active_connections.append(websocket)

        if dashboard_id:
            if dashboard_id not in self.dashboard_subscribers:
                self.dashboard_subscribers[dashboard_id] = []
            self.dashboard_subscribers[dashboard_id].append(websocket)

    def disconnect(self, websocket: WebSocket, dashboard_id: str = None):
        """Remove a WebSocket connection."""
        if websocket in self.active_connections:
            self.active_connections.remove(websocket)

        if dashboard_id and dashboard_id in self.dashboard_subscribers:
            if websocket in self.dashboard_subscribers[dashboard_id]:
                self.dashboard_subscribers[dashboard_id].remove(websocket)

    async def send_personal_message(self, message: str, websocket: WebSocket):
        """Send a message to a specific WebSocket."""
        await websocket.send_text(message)

    async def broadcast_to_dashboard(self, message: str, dashboard_id: str):
        """Broadcast a message to all subscribers of a dashboard."""
        if dashboard_id in self.dashboard_subscribers:
            for connection in self.dashboard_subscribers[dashboard_id]:
                try:
                    await connection.send_text(message)
                except Exception:
                    # Remove broken connections
                    self.disconnect(connection, dashboard_id)

    async def broadcast(self, message: str):
        """Broadcast a message to all active connections."""
        for connection in self.active_connections:
            try:
                await connection.send_text(message)
            except Exception:
                # Remove broken connections
                if connection in self.active_connections:
                    self.active_connections.remove(connection)


connection_manager = ConnectionManager()


@router.post(
    "/",
    response_model=StandardResponse[DashboardConfig],
    summary="Create Dashboard",
    description="Create a new dashboard configuration",
)
@trace_function("create_dashboard")
async def create_dashboard(
    request: DashboardCreateRequest,
    db: AsyncSession = Depends(get_db_session),
    current_user: dict = Depends(get_current_user),
) -> StandardResponse[DashboardConfig]:
    """Create a new dashboard."""

    try:
        # For now, we'll return the dashboard config as is
        # In a real implementation, you would save this to a database
        dashboard = DashboardConfig(
            name=request.name,
            description=request.description,
            layout=request.layout,
            widgets=request.widgets,
            refresh_interval=request.refresh_interval,
            filters=request.filters,
        )

        return StandardResponse(
            success=True,
            data=dashboard,
            message="Dashboard created successfully",
            metadata=APIMetadata(
                version="v1",
                timestamp=datetime.utcnow(),
            ),
        )

    except Exception as e:
        raise HTTPException(
            status_code=500, detail=f"Failed to create dashboard: {str(e)}"
        )


@router.get(
    "/templates",
    response_model=StandardResponse[list[DashboardConfig]],
    summary="Get Dashboard Templates",
    description="Get predefined dashboard templates",
)
@trace_function("get_dashboard_templates")
async def get_dashboard_templates(
    current_user: dict = Depends(get_current_user),
) -> StandardResponse[list[DashboardConfig]]:
    """Get predefined dashboard templates."""

    try:
        templates = [
            DashboardConfig(
                name="Analytics Overview",
                description="High-level analytics dashboard with key metrics",
                layout={
                    "type": "grid",
                    "columns": 12,
                    "rows": 8,
                },
                widgets=[
                    {
                        "id": "total_users",
                        "type": "metric",
                        "title": "Total Users",
                        "data_source": "analytics_db",
                        "query": {"table": "users", "metric": "count"},
                        "visualization": {"format": "number", "color": "blue"},
                        "size": {"width": 3, "height": 2},
                        "position": {"x": 0, "y": 0},
                    },
                    {
                        "id": "active_sessions",
                        "type": "metric",
                        "title": "Active Sessions",
                        "data_source": "streaming_analytics",
                        "query": {"metric": "active_sessions"},
                        "visualization": {"format": "number", "color": "green"},
                        "size": {"width": 3, "height": 2},
                        "position": {"x": 3, "y": 0},
                    },
                    {
                        "id": "conversion_rate",
                        "type": "metric",
                        "title": "Conversion Rate",
                        "data_source": "analytics_db",
                        "query": {"table": "conversions", "metric": "rate"},
                        "visualization": {"format": "percentage", "color": "orange"},
                        "size": {"width": 3, "height": 2},
                        "position": {"x": 6, "y": 0},
                    },
                    {
                        "id": "revenue",
                        "type": "metric",
                        "title": "Revenue",
                        "data_source": "analytics_db",
                        "query": {
                            "table": "transactions",
                            "metric": "sum",
                            "field": "amount",
                        },
                        "visualization": {"format": "currency", "color": "purple"},
                        "size": {"width": 3, "height": 2},
                        "position": {"x": 9, "y": 0},
                    },
                    {
                        "id": "user_growth",
                        "type": "chart",
                        "title": "User Growth",
                        "data_source": "analytics_db",
                        "query": {
                            "table": "users",
                            "metric": "count",
                            "group_by": "date",
                        },
                        "visualization": {
                            "type": "line",
                            "x_axis": "date",
                            "y_axis": "count",
                        },
                        "size": {"width": 6, "height": 4},
                        "position": {"x": 0, "y": 2},
                    },
                    {
                        "id": "traffic_sources",
                        "type": "chart",
                        "title": "Traffic Sources",
                        "data_source": "analytics_db",
                        "query": {
                            "table": "sessions",
                            "metric": "count",
                            "group_by": "source",
                        },
                        "visualization": {
                            "type": "pie",
                            "value": "count",
                            "label": "source",
                        },
                        "size": {"width": 6, "height": 4},
                        "position": {"x": 6, "y": 2},
                    },
                ],
                refresh_interval=300,
                filters={},
            ),
            DashboardConfig(
                name="Data Quality Dashboard",
                description="Monitor data quality metrics and alerts",
                layout={
                    "type": "grid",
                    "columns": 12,
                    "rows": 6,
                },
                widgets=[
                    {
                        "id": "quality_score",
                        "type": "metric",
                        "title": "Overall Quality Score",
                        "data_source": "data_quality",
                        "query": {"metric": "overall_score"},
                        "visualization": {"format": "percentage", "color": "green"},
                        "size": {"width": 4, "height": 2},
                        "position": {"x": 0, "y": 0},
                    },
                    {
                        "id": "data_violations",
                        "type": "metric",
                        "title": "Data Violations",
                        "data_source": "data_quality",
                        "query": {"metric": "violations_count"},
                        "visualization": {"format": "number", "color": "red"},
                        "size": {"width": 4, "height": 2},
                        "position": {"x": 4, "y": 0},
                    },
                    {
                        "id": "datasets_monitored",
                        "type": "metric",
                        "title": "Datasets Monitored",
                        "data_source": "data_quality",
                        "query": {"metric": "datasets_count"},
                        "visualization": {"format": "number", "color": "blue"},
                        "size": {"width": 4, "height": 2},
                        "position": {"x": 8, "y": 0},
                    },
                    {
                        "id": "quality_trends",
                        "type": "chart",
                        "title": "Quality Score Trends",
                        "data_source": "data_quality",
                        "query": {"metric": "quality_score", "group_by": "date"},
                        "visualization": {
                            "type": "line",
                            "x_axis": "date",
                            "y_axis": "score",
                        },
                        "size": {"width": 12, "height": 4},
                        "position": {"x": 0, "y": 2},
                    },
                ],
                refresh_interval=600,
                filters={},
            ),
            DashboardConfig(
                name="Streaming Analytics Dashboard",
                description="Real-time streaming metrics and performance",
                layout={
                    "type": "grid",
                    "columns": 12,
                    "rows": 8,
                },
                widgets=[
                    {
                        "id": "throughput",
                        "type": "metric",
                        "title": "Events/sec",
                        "data_source": "streaming_analytics",
                        "query": {"metric": "events_per_second"},
                        "visualization": {"format": "number", "color": "green"},
                        "size": {"width": 3, "height": 2},
                        "position": {"x": 0, "y": 0},
                    },
                    {
                        "id": "latency",
                        "type": "metric",
                        "title": "Avg Latency (ms)",
                        "data_source": "streaming_analytics",
                        "query": {"metric": "avg_latency_ms"},
                        "visualization": {"format": "number", "color": "blue"},
                        "size": {"width": 3, "height": 2},
                        "position": {"x": 3, "y": 0},
                    },
                    {
                        "id": "error_rate",
                        "type": "metric",
                        "title": "Error Rate (%)",
                        "data_source": "streaming_analytics",
                        "query": {"metric": "error_rate"},
                        "visualization": {"format": "percentage", "color": "red"},
                        "size": {"width": 3, "height": 2},
                        "position": {"x": 6, "y": 0},
                    },
                    {
                        "id": "queue_depth",
                        "type": "metric",
                        "title": "Queue Depth",
                        "data_source": "streaming_analytics",
                        "query": {"metric": "queue_depth"},
                        "visualization": {"format": "number", "color": "orange"},
                        "size": {"width": 3, "height": 2},
                        "position": {"x": 9, "y": 0},
                    },
                ],
                refresh_interval=60,  # 1 minute for real-time data
                filters={},
            ),
        ]

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


@router.get(
    "/data/{data_source}",
    response_model=StandardResponse[dict[str, Any]],
    summary="Get Dashboard Data",
    description="Get data for dashboard widgets",
)
@trace_function("get_dashboard_data")
async def get_dashboard_data(
    data_source: str,
    metric: str = None,
    filters: str = None,  # JSON string of filters
    db: AsyncSession = Depends(get_db_session),
    current_user: dict = Depends(get_current_user),
) -> StandardResponse[dict[str, Any]]:
    """Get data for dashboard widgets."""

    try:
        # Parse filters if provided
        import json

        parsed_filters = json.loads(filters) if filters else {}

        # Mock data based on data source
        if data_source == "analytics_db":
            data = _get_analytics_data(metric, parsed_filters)
        elif data_source == "streaming_analytics":
            data = _get_streaming_data(metric, parsed_filters)
        elif data_source == "data_quality":
            data = _get_data_quality_data(metric, parsed_filters)
        else:
            data = {"message": f"Unknown data source: {data_source}"}

        return StandardResponse(
            success=True,
            data=data,
            metadata=APIMetadata(
                version="v1",
                timestamp=datetime.utcnow(),
            ),
        )

    except Exception as e:
        raise HTTPException(
            status_code=500, detail=f"Failed to get dashboard data: {str(e)}"
        )


@router.websocket("/ws/{dashboard_id}")
async def websocket_endpoint(websocket: WebSocket, dashboard_id: str):
    """WebSocket endpoint for real-time dashboard updates."""
    await connection_manager.connect(websocket, dashboard_id)

    try:
        while True:
            # Wait for messages from client
            data = await websocket.receive_text()

            # Process the message (e.g., subscribe to specific metrics)
            await connection_manager.send_personal_message(
                f"Message received: {data}", websocket
            )

    except WebSocketDisconnect:
        connection_manager.disconnect(websocket, dashboard_id)


@router.post(
    "/broadcast/{dashboard_id}",
    response_model=StandardResponse[None],
    summary="Broadcast Update",
    description="Broadcast data update to dashboard subscribers",
)
@trace_function("broadcast_dashboard_update")
async def broadcast_dashboard_update(
    dashboard_id: str,
    data: dict[str, Any],
    current_user: dict = Depends(get_current_user),
) -> StandardResponse[None]:
    """Broadcast data update to dashboard subscribers."""

    try:
        import json

        message = json.dumps(
            {
                "type": "data_update",
                "dashboard_id": dashboard_id,
                "data": data,
                "timestamp": time.time(),
            }
        )

        await connection_manager.broadcast_to_dashboard(message, dashboard_id)

        return StandardResponse(
            success=True,
            data=None,
            message="Update broadcasted successfully",
            metadata=APIMetadata(
                version="v1",
                timestamp=datetime.utcnow(),
            ),
        )

    except Exception as e:
        raise HTTPException(
            status_code=500, detail=f"Failed to broadcast update: {str(e)}"
        )


def _get_analytics_data(metric: str, filters: dict) -> dict[str, Any]:
    """Get mock analytics data."""
    import random
    from datetime import datetime, timedelta

    if metric == "count":
        return {"value": random.randint(1000, 10000)}
    elif metric == "rate":
        return {"value": round(random.uniform(0.01, 0.1), 3)}
    elif metric == "sum":
        return {"value": round(random.uniform(10000, 100000), 2)}
    else:
        # Time series data
        dates = [
            (datetime.now() - timedelta(days=i)).strftime("%Y-%m-%d")
            for i in range(30, 0, -1)
        ]
        values = [random.randint(100, 1000) for _ in dates]
        return {
            "labels": dates,
            "data": values,
        }


def _get_streaming_data(metric: str, filters: dict) -> dict[str, Any]:
    """Get mock streaming analytics data."""
    import random

    metrics_data = {
        "events_per_second": random.randint(1000, 5000),
        "avg_latency_ms": round(random.uniform(10, 100), 2),
        "error_rate": round(random.uniform(0.001, 0.01), 3),
        "queue_depth": random.randint(0, 1000),
        "active_sessions": random.randint(500, 2000),
    }

    if metric in metrics_data:
        return {"value": metrics_data[metric]}
    else:
        return metrics_data


def _get_data_quality_data(metric: str, filters: dict) -> dict[str, Any]:
    """Get mock data quality data."""
    import random

    quality_data = {
        "overall_score": round(random.uniform(0.8, 0.99), 3),
        "violations_count": random.randint(0, 50),
        "datasets_count": random.randint(10, 100),
    }

    if metric in quality_data:
        return {"value": quality_data[metric]}
    else:
        return quality_data
