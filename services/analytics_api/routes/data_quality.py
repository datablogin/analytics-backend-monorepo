"""Data quality monitoring and management API endpoints."""

from datetime import datetime, timedelta

from fastapi import APIRouter, Depends, HTTPException, Query, Request
from pydantic import BaseModel

from libs.analytics_core.auth import get_current_user
from libs.api_common.response_models import APIMetadata, StandardResponse
from libs.data_processing import (
    DataLineageTracker,
    DataProfiler,
    DataQualityConfig,
    DataQualityFramework,
)

router = APIRouter(prefix="/data-quality", tags=["Data Quality"])


# Initialize data quality components
dq_framework = DataQualityFramework(DataQualityConfig())
profiler = DataProfiler()
lineage_tracker = DataLineageTracker()


class ValidationRequest(BaseModel):
    """Request for data validation."""

    dataset_name: str
    suite_name: str
    data_location: str


class ValidationSummary(BaseModel):
    """Summary of validation results."""

    dataset_name: str
    suite_name: str
    validation_time: datetime
    success: bool
    success_rate: float
    expectations_count: int
    failure_count: int


class QualityMetrics(BaseModel):
    """Data quality metrics."""

    total_datasets: int
    validated_datasets: int
    average_success_rate: float
    recent_failures: int
    quality_score: float


class ProfileSummary(BaseModel):
    """Summary of data profile."""

    dataset_name: str
    profiling_time: datetime
    row_count: int
    column_count: int
    missing_percentage: float
    duplicate_percentage: float
    warnings_count: int


class LineageNode(BaseModel):
    """Lineage graph node."""

    id: str
    name: str
    type: str
    location: str | None = None


class LineageEdgeInfo(BaseModel):
    """Lineage graph edge."""

    source_id: str
    target_id: str
    transformation_name: str | None = None


class LineageGraph(BaseModel):
    """Complete lineage graph response."""

    nodes: list[LineageNode]
    edges: list[LineageEdgeInfo]


@router.get(
    "/metrics",
    response_model=StandardResponse[QualityMetrics],
    summary="Get data quality metrics overview",
)
async def get_quality_metrics(
    request: Request, current_user=Depends(get_current_user)
) -> StandardResponse[QualityMetrics]:
    """Get overall data quality metrics and KPIs."""

    # TODO: Replace with actual validation history query from database/storage
    # In a real implementation, this would query actual validation history
    # For now, returning mock data that demonstrates the structure
    metrics = QualityMetrics(
        total_datasets=25,  # TODO: Query from validation store
        validated_datasets=23,  # TODO: Query from validation store
        average_success_rate=95.8,  # TODO: Calculate from actual validation results
        recent_failures=2,  # TODO: Query recent failure count
        quality_score=92.5,  # TODO: Implement quality scoring algorithm
    )

    return StandardResponse(
        success=True,
        data=metrics,
        message="Data quality metrics retrieved successfully",
        metadata=APIMetadata(
            request_id=getattr(request.state, "request_id", "unknown"),
            version="v1",
            environment="development",
        ),
    )


@router.get(
    "/validations",
    response_model=StandardResponse[list[ValidationSummary]],
    summary="Get recent validation results",
)
async def get_recent_validations(
    request: Request,
    limit: int = Query(10, ge=1, le=100),
    suite_name: str | None = Query(None),
    current_user=Depends(get_current_user),
) -> StandardResponse[list[ValidationSummary]]:
    """Get recent data validation results."""

    try:
        # Get validation history from Great Expectations
        validations = []

        # TODO: Replace with actual validation store query (Great Expectations backend)
        # In a real implementation, this would query the validation store
        # For demonstration, returning structured mock data
        sample_validations = [
            ValidationSummary(
                dataset_name="user_events",
                suite_name="user_events_suite",
                validation_time=datetime.utcnow() - timedelta(hours=1),
                success=True,
                success_rate=98.5,
                expectations_count=15,
                failure_count=0,
            ),
            ValidationSummary(
                dataset_name="product_catalog",
                suite_name="product_catalog_suite",
                validation_time=datetime.utcnow() - timedelta(hours=2),
                success=False,
                success_rate=87.2,
                expectations_count=20,
                failure_count=3,
            ),
        ]

        # Filter by suite name if provided
        if suite_name:
            sample_validations = [
                v for v in sample_validations if v.suite_name == suite_name
            ]

        validations = sample_validations[:limit]

        return StandardResponse(
            success=True,
            data=validations,
            message=f"Retrieved {len(validations)} validation results",
            metadata=APIMetadata(
                request_id=getattr(request.state, "request_id", "unknown"),
                version="v1",
                environment="development",
            ),
        )

    except Exception as e:
        raise HTTPException(
            status_code=500, detail=f"Failed to retrieve validation results: {str(e)}"
        )


@router.post(
    "/validate",
    response_model=StandardResponse[dict],
    summary="Trigger data validation",
)
async def validate_dataset(
    request: Request,
    validation_request: ValidationRequest,
    current_user=Depends(get_current_user),
) -> StandardResponse[dict]:
    """Trigger validation for a specific dataset."""

    try:
        # TODO: Implement actual dataset validation
        # In a real implementation, this would:
        # 1. Load the dataset from the specified location
        # 2. Run validation using the specified suite
        # 3. Return detailed results

        # For demonstration, returning structured mock response
        result = {
            "validation_id": f"val_{int(datetime.utcnow().timestamp())}",
            "dataset_name": validation_request.dataset_name,
            "suite_name": validation_request.suite_name,
            "status": "completed",
            "success": True,
            "success_rate": 96.7,
            "expectations_checked": 18,
            "failures": [],
        }

        return StandardResponse(
            success=True,
            data=result,
            message="Dataset validation completed successfully",
            metadata=APIMetadata(
                request_id=getattr(request.state, "request_id", "unknown"),
                version="v1",
                environment="development",
            ),
        )

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Validation failed: {str(e)}")


@router.get(
    "/profiles",
    response_model=StandardResponse[list[ProfileSummary]],
    summary="Get data profiling results",
)
async def get_data_profiles(
    request: Request,
    limit: int = Query(10, ge=1, le=100),
    current_user=Depends(get_current_user),
) -> StandardResponse[list[ProfileSummary]]:
    """Get recent data profiling results."""

    try:
        # TODO: Replace with actual profile store query
        # In a real implementation, this would query stored profiles
        profiles = [
            ProfileSummary(
                dataset_name="user_events",
                profiling_time=datetime.utcnow() - timedelta(hours=1),
                row_count=1250000,
                column_count=15,
                missing_percentage=2.3,
                duplicate_percentage=0.1,
                warnings_count=1,
            ),
            ProfileSummary(
                dataset_name="product_catalog",
                profiling_time=datetime.utcnow() - timedelta(hours=3),
                row_count=45000,
                column_count=22,
                missing_percentage=5.7,
                duplicate_percentage=0.8,
                warnings_count=3,
            ),
        ]

        profiles = profiles[:limit]

        return StandardResponse(
            success=True,
            data=profiles,
            message=f"Retrieved {len(profiles)} data profiles",
            metadata=APIMetadata(
                request_id=getattr(request.state, "request_id", "unknown"),
                version="v1",
                environment="development",
            ),
        )

    except Exception as e:
        raise HTTPException(
            status_code=500, detail=f"Failed to retrieve profiles: {str(e)}"
        )


@router.get(
    "/lineage/{asset_name}",
    response_model=StandardResponse[dict],
    summary="Get data lineage for asset",
)
async def get_asset_lineage(
    request: Request,
    asset_name: str,
    direction: str = Query("both", pattern="^(upstream|downstream|both)$"),
    max_depth: int = Query(5, ge=1, le=10),
    current_user=Depends(get_current_user),
) -> StandardResponse[dict]:
    """Get data lineage information for a specific asset."""

    try:
        # Find asset by name
        asset = lineage_tracker.get_asset_by_name(asset_name)
        if not asset:
            raise HTTPException(
                status_code=404, detail=f"Asset '{asset_name}' not found"
            )

        result = {"asset": asset.model_dump()}

        if direction in ["upstream", "both"]:
            upstream_ids = lineage_tracker.get_upstream_assets(asset.id, max_depth)
            result["upstream_assets"] = [
                lineage_tracker.graph.assets[asset_id].model_dump()
                for asset_id in upstream_ids
                if asset_id in lineage_tracker.graph.assets
            ]

        if direction in ["downstream", "both"]:
            downstream_ids = lineage_tracker.get_downstream_assets(asset.id, max_depth)
            result["downstream_assets"] = [
                lineage_tracker.graph.assets[asset_id].model_dump()
                for asset_id in downstream_ids
                if asset_id in lineage_tracker.graph.assets
            ]

        return StandardResponse(
            success=True,
            data=result,
            message=f"Lineage information for '{asset_name}' retrieved successfully",
            metadata=APIMetadata(
                request_id=getattr(request.state, "request_id", "unknown"),
                version="v1",
                environment="development",
            ),
        )

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=500, detail=f"Failed to retrieve lineage: {str(e)}"
        )


@router.get(
    "/lineage/{asset_name}/impact",
    response_model=StandardResponse[dict],
    summary="Analyze impact of changes to asset",
)
async def analyze_impact(
    request: Request, asset_name: str, current_user=Depends(get_current_user)
) -> StandardResponse[dict]:
    """Analyze potential impact of changes to a data asset."""

    try:
        # Find asset by name
        asset = lineage_tracker.get_asset_by_name(asset_name)
        if not asset:
            raise HTTPException(
                status_code=404, detail=f"Asset '{asset_name}' not found"
            )

        impact_analysis = lineage_tracker.analyze_impact(asset.id)

        return StandardResponse(
            success=True,
            data=impact_analysis,
            message=f"Impact analysis for '{asset_name}' completed",
            metadata=APIMetadata(
                request_id=getattr(request.state, "request_id", "unknown"),
                version="v1",
                environment="development",
            ),
        )

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Impact analysis failed: {str(e)}")


@router.get(
    "/lineage/graph",
    response_model=StandardResponse[LineageGraph],
    summary="Get complete lineage graph",
)
async def get_lineage_graph(
    request: Request, current_user=Depends(get_current_user)
) -> StandardResponse[LineageGraph]:
    """Get the complete data lineage graph."""

    try:
        # Convert internal graph to API response format
        nodes = [
            LineageNode(
                id=asset.id, name=asset.name, type=asset.type, location=asset.location
            )
            for asset in lineage_tracker.graph.assets.values()
        ]

        edges = []
        for edge in lineage_tracker.graph.edges:
            transformation_name = None
            if (
                edge.transformation_id
                and edge.transformation_id in lineage_tracker.graph.transformations
            ):
                transformation_name = lineage_tracker.graph.transformations[
                    edge.transformation_id
                ].name

            edges.append(
                LineageEdgeInfo(
                    source_id=edge.source_asset_id,
                    target_id=edge.target_asset_id,
                    transformation_name=transformation_name,
                )
            )

        graph = LineageGraph(nodes=nodes, edges=edges)

        return StandardResponse(
            success=True,
            data=graph,
            message="Complete lineage graph retrieved successfully",
            metadata=APIMetadata(
                request_id=getattr(request.state, "request_id", "unknown"),
                version="v1",
                environment="development",
            ),
        )

    except Exception as e:
        raise HTTPException(
            status_code=500, detail=f"Failed to retrieve lineage graph: {str(e)}"
        )


@router.get(
    "/suites",
    response_model=StandardResponse[list[dict]],
    summary="Get available expectation suites",
)
async def get_expectation_suites(
    request: Request, current_user=Depends(get_current_user)
) -> StandardResponse[list[dict]]:
    """Get list of available Great Expectations suites."""

    try:
        # Get suites from simplified framework
        suite_names = list(dq_framework.expectation_suites.keys())

        suites = []
        for suite_name in suite_names:
            expectations = dq_framework.expectation_suites.get(suite_name, [])
            suites.append(
                {
                    "name": suite_name,
                    "expectations_count": len(expectations),
                    "created_at": "unknown",
                    "description": f"Expectation suite for {suite_name}",
                }
            )

        return StandardResponse(
            success=True,
            data=suites,
            message=f"Retrieved {len(suites)} expectation suites",
            metadata=APIMetadata(
                request_id=getattr(request.state, "request_id", "unknown"),
                version="v1",
                environment="development",
            ),
        )

    except Exception as e:
        raise HTTPException(
            status_code=500, detail=f"Failed to retrieve expectation suites: {str(e)}"
        )


@router.get(
    "/alerts",
    response_model=StandardResponse[list[dict]],
    summary="Get data quality alerts",
)
async def get_quality_alerts(
    request: Request,
    severity: str | None = Query(None, pattern="^(low|medium|high|critical)$"),
    limit: int = Query(20, ge=1, le=100),
    current_user=Depends(get_current_user),
) -> StandardResponse[list[dict]]:
    """Get recent data quality alerts and violations."""

    try:
        # TODO: Replace with actual alerting system query
        # In a real implementation, this would query an alerting system
        alerts = [
            {
                "id": "alert_001",
                "timestamp": datetime.utcnow() - timedelta(minutes=30),
                "severity": "high",
                "dataset": "user_events",
                "message": "Null values detected in required field 'user_id'",
                "expectation": "expect_column_values_to_not_be_null",
                "failure_count": 45,
                "status": "active",
            },
            {
                "id": "alert_002",
                "timestamp": datetime.utcnow() - timedelta(hours=2),
                "severity": "medium",
                "dataset": "product_catalog",
                "message": "Data freshness SLA violated",
                "expectation": "expect_table_row_count_to_be_between",
                "failure_count": 1,
                "status": "resolved",
            },
        ]

        # Filter by severity if provided
        if severity:
            alerts = [alert for alert in alerts if alert["severity"] == severity]

        alerts = alerts[:limit]

        return StandardResponse(
            success=True,
            data=alerts,
            message=f"Retrieved {len(alerts)} quality alerts",
            metadata=APIMetadata(
                request_id=getattr(request.state, "request_id", "unknown"),
                version="v1",
                environment="development",
            ),
        )

    except Exception as e:
        raise HTTPException(
            status_code=500, detail=f"Failed to retrieve alerts: {str(e)}"
        )
