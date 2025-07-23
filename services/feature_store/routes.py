"""Feature store API routes."""

from datetime import datetime

from fastapi import APIRouter, Depends, HTTPException, Query, status
from libs.analytics_core.database import DatabaseManager, get_database_manager
from libs.api_common.response_models import (
    StandardResponse,
)

from .cache import get_cache
from .core import FeatureStoreService
from .models import (
    FeatureDefinitionCreate,
    FeatureDefinitionResponse,
    FeatureDefinitionUpdate,
    FeatureDiscoveryResponse,
    FeatureServingResponse,
    FeatureStatus,
    FeatureValueBatchWrite,
    FeatureValueRead,
    FeatureValueResponse,
    FeatureValueWrite,
)

router = APIRouter(prefix="/feature-store", tags=["feature-store"])


def get_feature_store_service(
    db_manager: DatabaseManager = Depends(get_database_manager),
) -> FeatureStoreService:
    """Dependency to get feature store service."""
    return FeatureStoreService(db_manager)


@router.post(
    "/features",
    response_model=StandardResponse[FeatureDefinitionResponse],
    status_code=status.HTTP_201_CREATED,
    summary="Create feature definition",
    description="Create a new feature definition in the feature store.",
)
async def create_feature(
    feature_data: FeatureDefinitionCreate,
    service: FeatureStoreService = Depends(get_feature_store_service),
) -> StandardResponse[FeatureDefinitionResponse]:
    """Create a new feature definition."""
    try:
        # Check if feature already exists
        existing_feature = await service.get_feature(feature_data.name)
        if existing_feature:
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail=f"Feature '{feature_data.name}' already exists",
            )

        feature = await service.create_feature(feature_data)

        # Cache the feature definition
        cache = get_cache()
        await cache.set_feature_definition(feature.name, feature.model_dump())

        return StandardResponse[FeatureDefinitionResponse](
            success=True,
            data=feature,
            message="Feature created successfully",
        )
    except ValueError as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e),
        )
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to create feature: {str(e)}",
        )


@router.get(
    "/features",
    response_model=StandardResponse[list[FeatureDefinitionResponse]],
    summary="List features",
    description="List all feature definitions with optional filtering.",
)
async def list_features(
    feature_group: str | None = Query(None, description="Filter by feature group"),
    status_filter: FeatureStatus | None = Query(
        None, alias="status", description="Filter by status"
    ),
    limit: int = Query(
        100, ge=1, le=1000, description="Maximum number of features to return"
    ),
    offset: int = Query(0, ge=0, description="Number of features to skip"),
    service: FeatureStoreService = Depends(get_feature_store_service),
) -> StandardResponse[list[FeatureDefinitionResponse]]:
    """List features with optional filtering."""
    try:
        features = await service.list_features(
            feature_group=feature_group,
            status=status_filter,
            limit=limit,
            offset=offset,
        )

        return StandardResponse[list[FeatureDefinitionResponse]](
            success=True,
            data=features,
            message=f"Found {len(features)} features",
        )
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to list features: {str(e)}",
        )


@router.get(
    "/features/{feature_name}",
    response_model=StandardResponse[FeatureDefinitionResponse],
    summary="Get feature definition",
    description="Get a specific feature definition by name.",
)
async def get_feature(
    feature_name: str,
    service: FeatureStoreService = Depends(get_feature_store_service),
) -> StandardResponse[FeatureDefinitionResponse]:
    """Get a feature definition by name."""
    try:
        # Try cache first
        cache = get_cache()
        cached_feature = await cache.get_feature_definition(feature_name)

        if cached_feature:
            feature = FeatureDefinitionResponse(**cached_feature)
        else:
            feature = await service.get_feature(feature_name)
            if feature is None:
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail=f"Feature '{feature_name}' not found",
                )
            # Cache the result
            await cache.set_feature_definition(feature_name, feature.model_dump())

        return StandardResponse[FeatureDefinitionResponse](
            success=True,
            data=feature,
            message="Feature retrieved successfully",
        )
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to get feature: {str(e)}",
        )


@router.put(
    "/features/{feature_name}",
    response_model=StandardResponse[FeatureDefinitionResponse],
    summary="Update feature definition",
    description="Update an existing feature definition.",
)
async def update_feature(
    feature_name: str,
    update_data: FeatureDefinitionUpdate,
    service: FeatureStoreService = Depends(get_feature_store_service),
) -> StandardResponse[FeatureDefinitionResponse]:
    """Update a feature definition."""
    try:
        feature = await service.update_feature(feature_name, update_data)
        if feature is None:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Feature '{feature_name}' not found",
            )

        # Update cache
        cache = get_cache()
        await cache.set_feature_definition(feature_name, feature.model_dump())

        return StandardResponse[FeatureDefinitionResponse](
            success=True,
            data=feature,
            message="Feature updated successfully",
        )
    except HTTPException:
        raise
    except ValueError as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e),
        )
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to update feature: {str(e)}",
        )


@router.delete(
    "/features/{feature_name}",
    response_model=StandardResponse[None],
    summary="Delete feature definition",
    description="Delete a feature definition and all its values.",
)
async def delete_feature(
    feature_name: str,
    service: FeatureStoreService = Depends(get_feature_store_service),
) -> StandardResponse[None]:
    """Delete a feature definition."""
    try:
        deleted = await service.delete_feature(feature_name)
        if not deleted:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Feature '{feature_name}' not found",
            )

        # Invalidate cache
        cache = get_cache()
        await cache.invalidate_feature_definition(feature_name)

        return StandardResponse[None](
            success=True,
            message="Feature deleted successfully",
        )
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to delete feature: {str(e)}",
        )


@router.post(
    "/features/values",
    response_model=StandardResponse[FeatureValueResponse],
    status_code=status.HTTP_201_CREATED,
    summary="Write feature value",
    description="Write a single feature value to the store.",
)
async def write_feature_value(
    value_data: FeatureValueWrite,
    service: FeatureStoreService = Depends(get_feature_store_service),
) -> StandardResponse[FeatureValueResponse]:
    """Write a single feature value."""
    try:
        feature_value = await service.write_feature_value(value_data)

        # Invalidate cache for this entity
        cache = get_cache()
        await cache.invalidate_feature_values(value_data.entity_id)

        return StandardResponse[FeatureValueResponse](
            success=True,
            data=feature_value,
            message="Feature value written successfully",
        )
    except ValueError as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e),
        )
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to write feature value: {str(e)}",
        )


@router.post(
    "/features/values/batch",
    response_model=StandardResponse[list[FeatureValueResponse]],
    status_code=status.HTTP_201_CREATED,
    summary="Write feature values batch",
    description="Write multiple feature values in a single batch operation.",
)
async def write_feature_values_batch(
    batch_data: FeatureValueBatchWrite,
    service: FeatureStoreService = Depends(get_feature_store_service),
) -> StandardResponse[list[FeatureValueResponse]]:
    """Write multiple feature values in a batch."""
    try:
        feature_values = await service.write_feature_values_batch(batch_data.values)

        # Invalidate cache for affected entities
        cache = get_cache()
        entity_ids = list({v.entity_id for v in batch_data.values})
        for entity_id in entity_ids:
            await cache.invalidate_feature_values(entity_id)

        return StandardResponse[list[FeatureValueResponse]](
            success=True,
            data=feature_values,
            message=f"Successfully wrote {len(feature_values)} feature values",
        )
    except ValueError as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e),
        )
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to write feature values: {str(e)}",
        )


@router.post(
    "/features/values/read",
    response_model=StandardResponse[list[FeatureValueResponse]],
    summary="Read feature values",
    description="Read feature values for specified features and entities.",
)
async def read_feature_values(
    read_data: FeatureValueRead,
    service: FeatureStoreService = Depends(get_feature_store_service),
) -> StandardResponse[list[FeatureValueResponse]]:
    """Read feature values for given features and entities."""
    try:
        feature_values = await service.read_feature_values(
            feature_names=read_data.feature_names,
            entity_ids=read_data.entity_ids,
            timestamp=read_data.timestamp,
        )

        return StandardResponse[list[FeatureValueResponse]](
            success=True,
            data=feature_values,
            message=f"Found {len(feature_values)} feature values",
        )
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to read feature values: {str(e)}",
        )


@router.get(
    "/features/serve/{entity_id}",
    response_model=StandardResponse[FeatureServingResponse],
    summary="Serve features for online inference",
    description="Get feature values optimized for online serving and inference.",
)
async def serve_features(
    entity_id: str,
    feature_names: list[str] = Query(..., description="List of feature names to serve"),
    timestamp: datetime | None = Query(None, description="Point-in-time timestamp"),
    service: FeatureStoreService = Depends(get_feature_store_service),
) -> StandardResponse[FeatureServingResponse]:
    """Serve features for online inference."""
    try:
        # Try cache first
        cache = get_cache()
        cached_values = await cache.get_feature_values(entity_id, feature_names)

        if cached_values and not timestamp:  # Don't use cache for historical queries
            serving_response = FeatureServingResponse(
                entity_id=entity_id,
                features=cached_values.get("values", {}),
                timestamp=datetime.fromisoformat(cached_values["timestamp"]),
            )
        else:
            serving_response = await service.serve_features(
                feature_names=feature_names,
                entity_id=entity_id,
                timestamp=timestamp,
            )

            # Cache the result if it's a current query
            if not timestamp:
                await cache.set_feature_values(
                    entity_id, feature_names, serving_response.features
                )

        return StandardResponse[FeatureServingResponse](
            success=True,
            data=serving_response,
            message="Features served successfully",
        )
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to serve features: {str(e)}",
        )


@router.get(
    "/features/discover",
    response_model=StandardResponse[FeatureDiscoveryResponse],
    summary="Discover features",
    description="Discover available features and get statistics about the feature store.",
)
async def discover_features(
    service: FeatureStoreService = Depends(get_feature_store_service),
) -> StandardResponse[FeatureDiscoveryResponse]:
    """Discover available features and statistics."""
    try:
        discovery_data = await service.discover_features()

        return StandardResponse[FeatureDiscoveryResponse](
            success=True,
            data=discovery_data,
            message="Feature discovery completed successfully",
        )
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to discover features: {str(e)}",
        )


@router.get(
    "/health",
    response_model=StandardResponse[dict],
    summary="Feature store health check",
    description="Check the health of the feature store service and its dependencies.",
)
async def health_check() -> StandardResponse[dict]:
    """Health check endpoint for the feature store."""
    try:
        cache = get_cache()
        cache_status = await cache.health_check()

        health_data = {
            "service": "feature_store",
            "status": "healthy",
            "cache": cache_status,
            "timestamp": datetime.utcnow().isoformat(),
        }

        return StandardResponse[dict](
            success=True,
            data=health_data,
            message="Feature store is healthy",
        )
    except Exception as e:
        health_data = {
            "service": "feature_store",
            "status": "unhealthy",
            "error": str(e),
            "timestamp": datetime.utcnow().isoformat(),
        }

        return StandardResponse[dict](
            success=False,
            data=health_data,
            message="Feature store health check failed",
        )
