"""Feature store API endpoints."""

from datetime import datetime
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Query, Request
from pydantic import BaseModel

from libs.analytics_core.auth import get_current_user
from libs.api_common.response_models import APIMetadata, StandardResponse
from libs.ml_models import (
    FeatureDefinition,
    FeatureSet,
    FeatureStore,
    FeatureStoreConfig,
    FeatureTransformation,
    FeatureValue,
)

router = APIRouter(prefix="/feature-store", tags=["Feature Store"])

# Initialize feature store
feature_store = FeatureStore(FeatureStoreConfig())


class FeatureRequest(BaseModel):
    """Request for getting features."""

    feature_names: list[str]
    entity_ids: list[str]
    as_of_time: datetime | None = None


class OnlineFeatureRequest(BaseModel):
    """Request for online feature serving."""

    feature_names: list[str]
    entity_ids: list[str]
    timeout_ms: int | None = None


class HistoricalFeatureRequest(BaseModel):
    """Request for historical features."""

    feature_names: list[str]
    entity_timestamps: list[tuple[str, datetime]]
    point_in_time_accuracy: bool = True


class BatchComputeRequest(BaseModel):
    """Request for batch feature computation."""

    transformation_name: str
    entity_data: dict[str, Any]  # Would be entity DataFrame in JSON format
    start_date: datetime
    end_date: datetime
    chunk_size: int | None = None


class DriftDetectionRequest(BaseModel):
    """Request for feature drift detection."""

    feature_name: str
    reference_data: dict[str, Any]  # DataFrame in JSON format
    current_data: dict[str, Any]  # DataFrame in JSON format


@router.post(
    "/features",
    response_model=StandardResponse[FeatureDefinition],
    summary="Create feature definition",
)
async def create_feature(
    request: Request,
    feature_definition: FeatureDefinition,
    current_user=Depends(get_current_user),
) -> StandardResponse[FeatureDefinition]:
    """Create a new feature definition."""
    try:
        feature_id = feature_store.create_feature(feature_definition)

        return StandardResponse(
            success=True,
            data=feature_definition,
            message=f"Feature '{feature_definition.name}' created with ID {feature_id}",
            metadata=APIMetadata(
                request_id=getattr(request.state, "request_id", "unknown"),
                version="v1",
                environment="development",
            ),
        )

    except Exception as e:
        raise HTTPException(
            status_code=400, detail=f"Failed to create feature: {str(e)}"
        )


@router.get(
    "/features/{feature_name}",
    response_model=StandardResponse[FeatureDefinition],
    summary="Get feature definition",
)
async def get_feature(
    request: Request,
    feature_name: str,
    current_user=Depends(get_current_user),
) -> StandardResponse[FeatureDefinition]:
    """Get feature definition by name."""
    try:
        feature_def = feature_store.get_feature_definition(feature_name)

        if not feature_def:
            raise HTTPException(
                status_code=404, detail=f"Feature '{feature_name}' not found"
            )

        return StandardResponse(
            success=True,
            data=feature_def,
            message=f"Feature '{feature_name}' retrieved successfully",
            metadata=APIMetadata(
                request_id=getattr(request.state, "request_id", "unknown"),
                version="v1",
                environment="development",
            ),
        )

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to get feature: {str(e)}")


@router.get(
    "/features",
    response_model=StandardResponse[list[FeatureDefinition]],
    summary="List features",
)
async def list_features(
    request: Request,
    feature_type: str | None = Query(None),
    owner: str | None = Query(None),
    tags: list[str] | None = Query(None),
    current_user=Depends(get_current_user),
) -> StandardResponse[list[FeatureDefinition]]:
    """List feature definitions with optional filtering."""
    try:
        features = feature_store.list_features(
            feature_type=feature_type, owner=owner, tags=tags
        )

        return StandardResponse(
            success=True,
            data=features,
            message=f"Retrieved {len(features)} features",
            metadata=APIMetadata(
                request_id=getattr(request.state, "request_id", "unknown"),
                version="v1",
                environment="development",
            ),
        )

    except Exception as e:
        raise HTTPException(
            status_code=500, detail=f"Failed to list features: {str(e)}"
        )


@router.post(
    "/features/values",
    response_model=StandardResponse[dict],
    summary="Write feature values",
)
async def write_features(
    request: Request,
    feature_values: list[FeatureValue],
    current_user=Depends(get_current_user),
) -> StandardResponse[dict]:
    """Write feature values to the store."""
    try:
        written_count = feature_store.write_features(feature_values)

        return StandardResponse(
            success=True,
            data={"written_count": written_count},
            message=f"Successfully wrote {written_count} feature values",
            metadata=APIMetadata(
                request_id=getattr(request.state, "request_id", "unknown"),
                version="v1",
                environment="development",
            ),
        )

    except Exception as e:
        raise HTTPException(
            status_code=400, detail=f"Failed to write features: {str(e)}"
        )


@router.post(
    "/features/online",
    response_model=StandardResponse[dict],
    summary="Get features for online serving",
)
async def get_online_features(
    request: Request,
    feature_request: OnlineFeatureRequest,
    current_user=Depends(get_current_user),
) -> StandardResponse[dict]:
    """Get features for real-time inference with low latency."""
    try:
        result_df = feature_store.get_online_features(
            feature_names=feature_request.feature_names,
            entity_ids=feature_request.entity_ids,
            timeout_ms=feature_request.timeout_ms,
        )

        # Convert DataFrame to JSON
        result_data = {
            "features": result_df.to_dict("records"),
            "feature_names": feature_request.feature_names,
            "entity_count": len(feature_request.entity_ids),
            "result_count": len(result_df),
        }

        return StandardResponse(
            success=True,
            data=result_data,
            message=f"Retrieved {len(result_df)} online features",
            metadata=APIMetadata(
                request_id=getattr(request.state, "request_id", "unknown"),
                version="v1",
                environment="development",
            ),
        )

    except Exception as e:
        raise HTTPException(
            status_code=500, detail=f"Failed to get online features: {str(e)}"
        )


@router.post(
    "/features/historical",
    response_model=StandardResponse[dict],
    summary="Get historical features for training",
)
async def get_historical_features(
    request: Request,
    feature_request: HistoricalFeatureRequest,
    current_user=Depends(get_current_user),
) -> StandardResponse[dict]:
    """Get historical features with point-in-time correctness."""
    try:
        result_df = feature_store.get_historical_features(
            feature_names=feature_request.feature_names,
            entity_timestamps=feature_request.entity_timestamps,
            point_in_time_accuracy=feature_request.point_in_time_accuracy,
        )

        # Convert DataFrame to JSON
        result_data = {
            "features": result_df.to_dict("records"),
            "feature_names": feature_request.feature_names,
            "entity_timestamp_count": len(feature_request.entity_timestamps),
            "result_count": len(result_df),
            "point_in_time_accuracy": feature_request.point_in_time_accuracy,
        }

        return StandardResponse(
            success=True,
            data=result_data,
            message=f"Retrieved {len(result_df)} historical features",
            metadata=APIMetadata(
                request_id=getattr(request.state, "request_id", "unknown"),
                version="v1",
                environment="development",
            ),
        )

    except Exception as e:
        raise HTTPException(
            status_code=500, detail=f"Failed to get historical features: {str(e)}"
        )


@router.post(
    "/transformations",
    response_model=StandardResponse[dict],
    summary="Register feature transformation",
)
async def register_transformation(
    request: Request,
    transformation: FeatureTransformation,
    current_user=Depends(get_current_user),
) -> StandardResponse[dict]:
    """Register a feature transformation."""
    try:
        transformation_id = feature_store.register_transformation(transformation)

        return StandardResponse(
            success=True,
            data={"transformation_id": transformation_id},
            message=f"Transformation '{transformation.name}' registered with ID {transformation_id}",
            metadata=APIMetadata(
                request_id=getattr(request.state, "request_id", "unknown"),
                version="v1",
                environment="development",
            ),
        )

    except Exception as e:
        raise HTTPException(
            status_code=400, detail=f"Failed to register transformation: {str(e)}"
        )


@router.post(
    "/transformations/{transformation_name}/execute",
    response_model=StandardResponse[dict],
    summary="Execute feature transformation",
)
async def execute_transformation(
    request: Request,
    transformation_name: str,
    input_data: dict[str, Any],
    parameters: dict[str, Any] | None = None,
    current_user=Depends(get_current_user),
) -> StandardResponse[dict]:
    """Execute a feature transformation."""
    try:
        import pandas as pd

        # Convert input data to DataFrame
        input_df = pd.DataFrame(input_data)

        result_df = feature_store.execute_transformation(
            transformation_name=transformation_name,
            input_data=input_df,
            parameters=parameters,
        )

        # Convert result DataFrame to JSON
        result_data = {
            "output_data": result_df.to_dict("records"),
            "input_rows": len(input_df),
            "output_rows": len(result_df),
            "transformation_name": transformation_name,
        }

        return StandardResponse(
            success=True,
            data=result_data,
            message=f"Transformation '{transformation_name}' executed successfully",
            metadata=APIMetadata(
                request_id=getattr(request.state, "request_id", "unknown"),
                version="v1",
                environment="development",
            ),
        )

    except Exception as e:
        raise HTTPException(
            status_code=500, detail=f"Failed to execute transformation: {str(e)}"
        )


@router.post(
    "/feature-sets",
    response_model=StandardResponse[dict],
    summary="Create feature set",
)
async def create_feature_set(
    request: Request,
    feature_set: FeatureSet,
    current_user=Depends(get_current_user),
) -> StandardResponse[dict]:
    """Create a feature set."""
    try:
        feature_set_id = feature_store.create_feature_set(feature_set)

        return StandardResponse(
            success=True,
            data={"feature_set_id": feature_set_id},
            message=f"Feature set '{feature_set.name}' created with ID {feature_set_id}",
            metadata=APIMetadata(
                request_id=getattr(request.state, "request_id", "unknown"),
                version="v1",
                environment="development",
            ),
        )

    except Exception as e:
        raise HTTPException(
            status_code=400, detail=f"Failed to create feature set: {str(e)}"
        )


@router.get(
    "/stats",
    response_model=StandardResponse[dict],
    summary="Get feature store statistics",
)
async def get_feature_store_stats(
    request: Request, current_user=Depends(get_current_user)
) -> StandardResponse[dict]:
    """Get feature store statistics."""
    try:
        stats = feature_store.get_feature_stats()

        return StandardResponse(
            success=True,
            data=stats,
            message="Feature store statistics retrieved successfully",
            metadata=APIMetadata(
                request_id=getattr(request.state, "request_id", "unknown"),
                version="v1",
                environment="development",
            ),
        )

    except Exception as e:
        raise HTTPException(
            status_code=500, detail=f"Failed to get feature store stats: {str(e)}"
        )


@router.get(
    "/discovery",
    response_model=StandardResponse[dict],
    summary="Get feature discovery information",
)
async def get_feature_discovery(
    request: Request, current_user=Depends(get_current_user)
) -> StandardResponse[dict]:
    """Get feature discovery portal information."""
    try:
        discovery_info = feature_store.get_feature_discovery_info()

        return StandardResponse(
            success=True,
            data=discovery_info,
            message="Feature discovery information retrieved successfully",
            metadata=APIMetadata(
                request_id=getattr(request.state, "request_id", "unknown"),
                version="v1",
                environment="development",
            ),
        )

    except Exception as e:
        raise HTTPException(
            status_code=500, detail=f"Failed to get feature discovery info: {str(e)}"
        )


@router.get(
    "/features/{feature_name}/lineage",
    response_model=StandardResponse[dict],
    summary="Get feature lineage",
)
async def get_feature_lineage(
    request: Request,
    feature_name: str,
    current_user=Depends(get_current_user),
) -> StandardResponse[dict]:
    """Get lineage information for a feature."""
    try:
        lineage_info = feature_store.get_feature_lineage(feature_name)

        return StandardResponse(
            success=True,
            data=lineage_info,
            message=f"Lineage information for '{feature_name}' retrieved successfully",
            metadata=APIMetadata(
                request_id=getattr(request.state, "request_id", "unknown"),
                version="v1",
                environment="development",
            ),
        )

    except Exception as e:
        raise HTTPException(
            status_code=500, detail=f"Failed to get feature lineage: {str(e)}"
        )


@router.post(
    "/features/{feature_name}/drift",
    response_model=StandardResponse[dict],
    summary="Detect feature drift",
)
async def detect_feature_drift(
    request: Request,
    feature_name: str,
    drift_request: DriftDetectionRequest,
    current_user=Depends(get_current_user),
) -> StandardResponse[dict]:
    """Detect drift in feature values."""
    try:
        import pandas as pd

        # Convert data to DataFrames
        reference_df = pd.DataFrame(drift_request.reference_data)
        current_df = pd.DataFrame(drift_request.current_data)

        drift_result = feature_store.detect_feature_drift(
            feature_name=feature_name,
            reference_data=reference_df,
            current_data=current_df,
        )

        return StandardResponse(
            success=True,
            data=drift_result,
            message=f"Drift detection for '{feature_name}' completed",
            metadata=APIMetadata(
                request_id=getattr(request.state, "request_id", "unknown"),
                version="v1",
                environment="development",
            ),
        )

    except Exception as e:
        raise HTTPException(
            status_code=500, detail=f"Failed to detect feature drift: {str(e)}"
        )


@router.post(
    "/batch-compute",
    response_model=StandardResponse[dict],
    summary="Batch compute features",
)
async def batch_compute_features(
    request: Request,
    batch_request: BatchComputeRequest,
    current_user=Depends(get_current_user),
) -> StandardResponse[dict]:
    """Batch compute features using a transformation."""
    try:
        import pandas as pd

        # Convert entity data to DataFrame
        entity_df = pd.DataFrame(batch_request.entity_data)

        total_processed = feature_store.batch_compute_features(
            transformation_name=batch_request.transformation_name,
            entity_df=entity_df,
            start_date=batch_request.start_date,
            end_date=batch_request.end_date,
            chunk_size=batch_request.chunk_size,
        )

        return StandardResponse(
            success=True,
            data={
                "total_features_processed": total_processed,
                "transformation_name": batch_request.transformation_name,
                "entity_count": len(entity_df),
                "date_range": {
                    "start": batch_request.start_date.isoformat(),
                    "end": batch_request.end_date.isoformat(),
                },
            },
            message=f"Batch computation completed: {total_processed} features processed",
            metadata=APIMetadata(
                request_id=getattr(request.state, "request_id", "unknown"),
                version="v1",
                environment="development",
            ),
        )

    except Exception as e:
        raise HTTPException(
            status_code=500, detail=f"Failed to batch compute features: {str(e)}"
        )


@router.delete(
    "/features/cleanup",
    response_model=StandardResponse[dict],
    summary="Cleanup old feature values",
)
async def cleanup_old_features(
    request: Request,
    retention_days: int = Query(90, ge=1, le=365),
    current_user=Depends(get_current_user),
) -> StandardResponse[dict]:
    """Clean up old feature values."""
    try:
        deleted_count = feature_store.cleanup_old_values(retention_days)

        return StandardResponse(
            success=True,
            data={
                "deleted_count": deleted_count,
                "retention_days": retention_days,
            },
            message=f"Cleaned up {deleted_count} old feature values",
            metadata=APIMetadata(
                request_id=getattr(request.state, "request_id", "unknown"),
                version="v1",
                environment="development",
            ),
        )

    except Exception as e:
        raise HTTPException(
            status_code=500, detail=f"Failed to cleanup old features: {str(e)}"
        )

