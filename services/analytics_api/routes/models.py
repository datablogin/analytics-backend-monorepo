"""ML Model Registry and Serving API endpoints."""

import tempfile
from datetime import datetime, timezone

from fastapi import (
    APIRouter,
    Depends,
    File,
    HTTPException,
    Query,
    Request,
    UploadFile,
)
from pydantic import BaseModel, Field

from libs.analytics_core.auth import get_current_user
from libs.analytics_core.models import User
from libs.api_common.response_models import APIMetadata, StandardResponse
from libs.ml_models import (
    InferenceRequest,
    InferenceResponse,
    ModelMetadata,
    ModelRegistry,
    ModelRegistryConfig,
    ModelServer,
    ModelServingConfig,
)

router = APIRouter(prefix="/models", tags=["ML Model Registry & Serving"])

# Initialize model registry and server (singleton pattern)
_model_registry = None
_model_server = None


def get_model_registry() -> ModelRegistry:
    """Get singleton model registry instance."""
    global _model_registry
    if _model_registry is None:
        _model_registry = ModelRegistry(ModelRegistryConfig())
    return _model_registry


def get_model_server() -> ModelServer:
    """Get singleton model server instance."""
    global _model_server
    if _model_server is None:
        _model_server = ModelServer(ModelServingConfig())
    return _model_server


class ModelRegistrationRequest(BaseModel):
    """Request for model registration."""

    name: str = Field(description="Model name")
    description: str | None = Field(default=None, description="Model description")
    model_type: str = Field(
        description="Type of model (classification, regression, etc.)"
    )
    algorithm: str = Field(description="ML algorithm used")
    framework: str = Field(description="ML framework (sklearn, lightgbm, etc.)")
    metrics: dict[str, float] = Field(
        default_factory=dict, description="Model performance metrics"
    )
    tags: dict[str, str] = Field(
        default_factory=dict, description="Custom tags for model"
    )
    training_dataset_version: str | None = Field(
        default=None, description="Version of training dataset"
    )


class ModelVersionResponse(BaseModel):
    """Response for model version information."""

    name: str
    version: str
    stage: str
    description: str | None
    model_type: str
    algorithm: str
    framework: str
    metrics: dict[str, float]
    training_dataset_version: str | None
    training_run_id: str | None
    model_artifact_uri: str | None
    created_by: str | None
    created_at: datetime
    tags: dict[str, str]


class ModelListResponse(BaseModel):
    """Response for model listing."""

    name: str
    description: str | None
    creation_time: datetime
    last_updated: datetime
    latest_versions: dict[str, str]
    tags: dict[str, str]


class StageTransitionRequest(BaseModel):
    """Request for model stage transition."""

    stage: str = Field(description="Target stage (None, Staging, Production, Archived)")
    archive_existing: bool = Field(
        default=False, description="Archive existing models in target stage"
    )


@router.post("", response_model=StandardResponse[dict])
async def register_model(
    request: Request,
    registration_request: ModelRegistrationRequest,
    model_file: UploadFile = File(...),
    current_user: User = Depends(get_current_user),
) -> StandardResponse[dict]:
    """Register a new ML model in the registry."""
    try:
        registry = get_model_registry()

        # Create model metadata
        metadata = ModelMetadata(
            name=registration_request.name,
            version="1",  # Initial version
            description=registration_request.description,
            model_type=registration_request.model_type,
            algorithm=registration_request.algorithm,
            framework=registration_request.framework,
            metrics=registration_request.metrics,
            tags=registration_request.tags,
            training_dataset_version=registration_request.training_dataset_version,
            created_by=current_user.username,
        )

        # Save uploaded model file temporarily
        with tempfile.NamedTemporaryFile(
            delete=False, suffix=f"_{model_file.filename}"
        ) as tmp_file:
            content = await model_file.read()
            tmp_file.write(content)
            tmp_file_path = tmp_file.name

        try:
            # Load model based on framework
            if registration_request.framework.lower() == "sklearn":
                import pickle

                with open(tmp_file_path, "rb") as f:
                    model = pickle.load(f)
            else:
                # For other frameworks, we'd need specific loading logic
                raise HTTPException(
                    status_code=400,
                    detail=f"Framework {registration_request.framework} not yet supported for file upload",
                )

            # Register model
            model_version = registry.register_model(
                model=model,
                metadata=metadata,
            )

            return StandardResponse(
                success=True,
                data={
                    "model_name": registration_request.name,
                    "version": model_version,
                    "message": f"Model {registration_request.name} version {model_version} registered successfully",
                },
                message="Model registered successfully",
                metadata=APIMetadata(
                    request_id=getattr(request.state, "request_id", "unknown"),
                    version="v1",
                    environment="development",
                ),
            )

        finally:
            # Clean up temporary file
            import os

            if os.path.exists(tmp_file_path):
                os.unlink(tmp_file_path)

    except Exception as e:
        raise HTTPException(
            status_code=500, detail=f"Failed to register model: {str(e)}"
        )


@router.get("", response_model=StandardResponse[list[ModelListResponse]])
async def list_models(
    request: Request,
    limit: int = Query(100, ge=1, le=1000),
    current_user: User = Depends(get_current_user),
) -> StandardResponse[list[ModelListResponse]]:
    """List all registered models."""
    try:
        registry = get_model_registry()
        models_data = registry.list_models(max_results=limit)

        models = [
            ModelListResponse(
                name=model["name"],
                description=model["description"],
                creation_time=model["creation_time"],
                last_updated=model["last_updated"],
                latest_versions=model["latest_versions"],
                tags=model["tags"],
            )
            for model in models_data
        ]

        return StandardResponse(
            success=True,
            data=models,
            message=f"Retrieved {len(models)} models",
            metadata=APIMetadata(
                request_id=getattr(request.state, "request_id", "unknown"),
                version="v1",
                environment="development",
            ),
        )

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to list models: {str(e)}")


@router.get("/{model_name}", response_model=StandardResponse[ModelVersionResponse])
async def get_model(
    request: Request,
    model_name: str,
    version: str | None = Query(
        None, description="Model version (latest if not specified)"
    ),
    stage: str | None = Query(
        None, description="Model stage (Production, Staging, etc.)"
    ),
    current_user: User = Depends(get_current_user),
) -> StandardResponse[ModelVersionResponse]:
    """Get model details by name and version/stage."""
    try:
        registry = get_model_registry()

        if version:
            metadata = registry.get_model_metadata(model_name, version)
        elif stage:
            latest_version = registry.get_latest_model_version(model_name, stage)
            metadata = registry.get_model_metadata(model_name, latest_version.version)
        else:
            latest_version = registry.get_latest_model_version(model_name)
            metadata = registry.get_model_metadata(model_name, latest_version.version)

        model_response = ModelVersionResponse(
            name=metadata.name,
            version=metadata.version,
            stage=metadata.stage,
            description=metadata.description,
            model_type=metadata.model_type,
            algorithm=metadata.algorithm,
            framework=metadata.framework,
            metrics=metadata.metrics,
            training_dataset_version=metadata.training_dataset_version,
            training_run_id=metadata.training_run_id,
            model_artifact_uri=metadata.model_artifact_uri,
            created_by=metadata.created_by,
            created_at=metadata.created_at,
            tags=metadata.tags,
        )

        return StandardResponse(
            success=True,
            data=model_response,
            message=f"Model {model_name} retrieved successfully",
            metadata=APIMetadata(
                request_id=getattr(request.state, "request_id", "unknown"),
                version="v1",
                environment="development",
            ),
        )

    except Exception as e:
        raise HTTPException(status_code=404, detail=f"Model not found: {str(e)}")


@router.get(
    "/{model_name}/versions",
    response_model=StandardResponse[list[ModelVersionResponse]],
)
async def list_model_versions(
    request: Request,
    model_name: str,
    limit: int = Query(100, ge=1, le=1000),
    current_user: User = Depends(get_current_user),
) -> StandardResponse[list[ModelVersionResponse]]:
    """List all versions of a specific model."""
    try:
        registry = get_model_registry()

        # Search for all versions of this model
        filter_string = f"name='{model_name}'"
        model_versions = registry.search_model_versions(
            filter_string=filter_string,
            max_results=limit,
            order_by=["version_number DESC"],
        )

        versions = []
        for mv in model_versions:
            try:
                metadata = registry.get_model_metadata(model_name, mv.version)
                versions.append(
                    ModelVersionResponse(
                        name=metadata.name,
                        version=metadata.version,
                        stage=metadata.stage,
                        description=metadata.description,
                        model_type=metadata.model_type,
                        algorithm=metadata.algorithm,
                        framework=metadata.framework,
                        metrics=metadata.metrics,
                        training_dataset_version=metadata.training_dataset_version,
                        training_run_id=metadata.training_run_id,
                        model_artifact_uri=metadata.model_artifact_uri,
                        created_by=metadata.created_by,
                        created_at=metadata.created_at,
                        tags=metadata.tags,
                    )
                )
            except Exception:
                # Skip versions that can't be loaded
                continue

        return StandardResponse(
            success=True,
            data=versions,
            message=f"Retrieved {len(versions)} versions for model {model_name}",
            metadata=APIMetadata(
                request_id=getattr(request.state, "request_id", "unknown"),
                version="v1",
                environment="development",
            ),
        )

    except Exception as e:
        raise HTTPException(
            status_code=500, detail=f"Failed to list model versions: {str(e)}"
        )


@router.post(
    "/{model_name}/versions/{version}/deploy", response_model=StandardResponse[dict]
)
async def deploy_model(
    request: Request,
    model_name: str,
    version: str,
    transition_request: StageTransitionRequest,
    current_user: User = Depends(get_current_user),
) -> StandardResponse[dict]:
    """Deploy a model version to a specific stage."""
    try:
        registry = get_model_registry()

        # Transition model to new stage
        model_version = registry.transition_model_stage(
            name=model_name,
            version=version,
            stage=transition_request.stage,
            archive_existing=transition_request.archive_existing,
        )

        return StandardResponse(
            success=True,
            data={
                "model_name": model_name,
                "version": version,
                "new_stage": transition_request.stage,
                "previous_stage": model_version.current_stage,
                "archive_existing": transition_request.archive_existing,
            },
            message=f"Model {model_name} version {version} deployed to {transition_request.stage}",
            metadata=APIMetadata(
                request_id=getattr(request.state, "request_id", "unknown"),
                version="v1",
                environment="development",
            ),
        )

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to deploy model: {str(e)}")


@router.post(
    "/{model_name}/predict", response_model=StandardResponse[InferenceResponse]
)
async def predict(
    request: Request,
    model_name: str,
    inference_request: InferenceRequest,
    current_user: User = Depends(get_current_user),
) -> StandardResponse[InferenceResponse]:
    """Make predictions using a deployed model."""
    try:
        server = get_model_server()

        # Override model name in request
        inference_request.model_name = model_name

        # Make prediction
        prediction_result = await server.predict(inference_request)

        return StandardResponse(
            success=True,
            data=prediction_result,
            message=f"Prediction completed for model {model_name}",
            metadata=APIMetadata(
                request_id=getattr(request.state, "request_id", "unknown"),
                version="v1",
                environment="development",
            ),
        )

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Prediction failed: {str(e)}")


@router.get("/{model_name}/status", response_model=StandardResponse[dict])
async def get_model_status(
    request: Request,
    model_name: str,
    current_user: User = Depends(get_current_user),
) -> StandardResponse[dict]:
    """Get model health status and serving statistics."""
    try:
        server = get_model_server()

        # Get model-specific stats
        model_stats = server.get_model_stats(model_name)

        if model_stats is None:
            # Model not loaded, try to get registry info
            registry = get_model_registry()
            try:
                latest_version = registry.get_latest_model_version(model_name)
                model_stats = {
                    "name": model_name,
                    "version": latest_version.version,
                    "stage": latest_version.current_stage,
                    "healthy": False,
                    "loaded": False,
                    "usage_count": 0,
                    "last_used": None,
                }
            except Exception:
                raise HTTPException(
                    status_code=404, detail=f"Model {model_name} not found"
                )
        else:
            model_stats["loaded"] = True

        return StandardResponse(
            success=True,
            data=model_stats,
            message=f"Status retrieved for model {model_name}",
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
            status_code=500, detail=f"Failed to get model status: {str(e)}"
        )


@router.get("/registry/stats", response_model=StandardResponse[dict])
async def get_registry_stats(
    request: Request,
    current_user: User = Depends(get_current_user),
) -> StandardResponse[dict]:
    """Get model registry statistics."""
    try:
        registry = get_model_registry()
        server = get_model_server()

        registry_stats = registry.get_registry_stats()
        server_stats = server.get_server_stats()

        combined_stats = {
            "registry": registry_stats,
            "serving": server_stats,
        }

        return StandardResponse(
            success=True,
            data=combined_stats,
            message="Registry and serving statistics retrieved successfully",
            metadata=APIMetadata(
                request_id=getattr(request.state, "request_id", "unknown"),
                version="v1",
                environment="development",
            ),
        )

    except Exception as e:
        raise HTTPException(
            status_code=500, detail=f"Failed to get registry stats: {str(e)}"
        )


@router.delete("/{model_name}", response_model=StandardResponse[dict])
async def delete_model(
    request: Request,
    model_name: str,
    current_user: User = Depends(get_current_user),
) -> StandardResponse[dict]:
    """Delete a registered model and all its versions."""
    try:
        registry = get_model_registry()

        # Delete model from registry
        registry.delete_model(model_name)

        return StandardResponse(
            success=True,
            data={"model_name": model_name, "deleted": True},
            message=f"Model {model_name} deleted successfully",
            metadata=APIMetadata(
                request_id=getattr(request.state, "request_id", "unknown"),
                version="v1",
                environment="development",
            ),
        )

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to delete model: {str(e)}")


@router.post("/search", response_model=StandardResponse[list[dict]])
async def search_models(
    request: Request,
    filter_string: str = Query("", description="Filter string for model search"),
    max_results: int = Query(100, ge=1, le=1000),
    order_by: list[str] | None = Query(None, description="Ordering criteria"),
    current_user: User = Depends(get_current_user),
) -> StandardResponse[list[dict]]:
    """Search models and versions with filters."""
    try:
        registry = get_model_registry()

        model_versions = registry.search_model_versions(
            filter_string=filter_string,
            max_results=max_results,
            order_by=order_by,
        )

        # Convert MLflow model versions to dict format
        results = []
        for mv in model_versions:
            results.append(
                {
                    "name": mv.name,
                    "version": mv.version,
                    "stage": mv.current_stage,
                    "creation_time": datetime.fromtimestamp(
                        mv.creation_timestamp / 1000, timezone.utc
                    ).isoformat(),
                    "last_updated": datetime.fromtimestamp(
                        mv.last_updated_timestamp / 1000, timezone.utc
                    ).isoformat(),
                    "description": mv.description,
                    "tags": dict(mv.tags) if mv.tags else {},
                    "run_id": mv.run_id,
                    "source": mv.source,
                }
            )

        return StandardResponse(
            success=True,
            data=results,
            message=f"Found {len(results)} matching models",
            metadata=APIMetadata(
                request_id=getattr(request.state, "request_id", "unknown"),
                version="v1",
                environment="development",
            ),
        )

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Model search failed: {str(e)}")
