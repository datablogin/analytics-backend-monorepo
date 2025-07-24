"""ML Model Registry and Serving API endpoints."""

import json
import tempfile
from datetime import datetime, timezone
from typing import Annotated

import structlog
from fastapi import (
    APIRouter,
    Depends,
    File,
    Form,
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

from .model_exceptions import (
    InvalidModelFileError,
    ModelLoadError,
    SecurityError,
    UnsupportedFrameworkError,
)
from .model_loader import (
    load_model_secure,
    safe_file_cleanup,
    safe_file_write,
    sanitize_filename,
)

logger = structlog.get_logger(__name__)
router = APIRouter(prefix="/models", tags=["ML Model Registry & Serving"])

# Maximum file size for uploads (100MB)
MAX_FILE_SIZE = 100 * 1024 * 1024


def get_model_registry_config() -> ModelRegistryConfig:
    """Get model registry configuration."""
    return ModelRegistryConfig()


def get_model_serving_config() -> ModelServingConfig:
    """Get model serving configuration."""
    return ModelServingConfig()


async def get_model_registry(
    config: Annotated[ModelRegistryConfig, Depends(get_model_registry_config)],
) -> ModelRegistry:
    """Get model registry instance using dependency injection."""
    return ModelRegistry(config)


async def get_model_server(
    config: Annotated[ModelServingConfig, Depends(get_model_serving_config)],
) -> ModelServer:
    """Get model server instance using dependency injection."""
    return ModelServer(config)


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
    name: str = Form(..., description="Model name"),
    model_type: str = Form(..., description="Type of model (classification, regression, etc.)"),
    algorithm: str = Form(..., description="ML algorithm used"),
    framework: str = Form(..., description="ML framework (sklearn, lightgbm, etc.)"),
    description: str | None = Form(None, description="Model description"),
    metrics: str = Form("{}", description="Model performance metrics as JSON string"),
    tags: str = Form("{}", description="Custom tags for model as JSON string"),
    training_dataset_version: str | None = Form(None, description="Version of training dataset"),
    model_file: UploadFile = File(..., description="Model file to upload"),
    current_user: User = Depends(get_current_user),
    registry: ModelRegistry = Depends(get_model_registry),
) -> StandardResponse[dict]:
    """Register a new ML model in the registry with secure file handling."""
    tmp_file_path = None

    try:
        # Parse JSON strings for metrics and tags
        try:
            parsed_metrics = json.loads(metrics) if metrics else {}
            parsed_tags = json.loads(tags) if tags else {}
        except json.JSONDecodeError as e:
            logger.error("Invalid JSON in form data", error=str(e))
            raise HTTPException(
                status_code=400, 
                detail=f"Invalid JSON format in metrics or tags: {str(e)}"
            )

        # Validate file size
        if model_file.size and model_file.size > MAX_FILE_SIZE:
            raise HTTPException(
                status_code=413,
                detail=f"File size {model_file.size} bytes exceeds maximum allowed size {MAX_FILE_SIZE} bytes",
            )

        # Sanitize filename
        safe_filename = sanitize_filename(model_file.filename)

        logger.info(
            "Starting model registration",
            model_name=name,
            framework=framework,
            filename=safe_filename,
            user=current_user.username,
        )

        # Create model metadata
        metadata = ModelMetadata(
            name=name,
            version="1",  # Initial version
            description=description,
            model_type=model_type,
            algorithm=algorithm,
            framework=framework,
            metrics=parsed_metrics,
            tags=parsed_tags,
            training_dataset_version=training_dataset_version,
            created_by=current_user.username,
        )

        # Save uploaded model file temporarily with secure filename
        with tempfile.NamedTemporaryFile(
            delete=False, suffix=f"_{safe_filename}", prefix="model_upload_"
        ) as tmp_file:
            tmp_file_path = tmp_file.name

        # Read and write file content using async I/O
        content = await model_file.read()
        await safe_file_write(content, tmp_file_path)

        # Load model securely based on framework
        model = await load_model_secure(
            tmp_file_path, framework, safe_filename
        )

        # Register model
        model_version = registry.register_model(
            model=model,
            metadata=metadata,
        )

        logger.info(
            "Model registered successfully",
            model_name=name,
            version=model_version,
            framework=framework,
        )

        return StandardResponse(
            success=True,
            data={
                "model_name": name,
                "version": model_version,
                "message": f"Model {name} version {model_version} registered successfully",
            },
            message="Model registered successfully",
            metadata=APIMetadata(
                request_id=getattr(request.state, "request_id", "unknown"),
                version="v1",
                environment="development",
            ),
        )

    except (InvalidModelFileError, SecurityError, UnsupportedFrameworkError) as e:
        logger.error(
            "Model registration failed",
            error=str(e),
            model_name=name,
        )
        raise HTTPException(status_code=400, detail=str(e))
    except ModelLoadError as e:
        logger.error(
            "Model loading failed", error=str(e), model_name=name
        )
        raise HTTPException(status_code=422, detail=f"Model loading failed: {str(e)}")
    except Exception as e:
        logger.error(
            "Unexpected error during model registration",
            error=str(e),
            model_name=name,
        )
        raise HTTPException(
            status_code=500, detail=f"Failed to register model: {str(e)}"
        )
    finally:
        # Clean up temporary file
        if tmp_file_path:
            await safe_file_cleanup(tmp_file_path)


@router.get("", response_model=StandardResponse[list[ModelListResponse]])
async def list_models(
    request: Request,
    limit: int = Query(100, ge=1, le=1000),
    current_user: User = Depends(get_current_user),
    registry: ModelRegistry = Depends(get_model_registry),
) -> StandardResponse[list[ModelListResponse]]:
    """List all registered models."""
    try:
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

        logger.info("Listed models", count=len(models), user=current_user.username)

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
        logger.error("Failed to list models", error=str(e), user=current_user.username)
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
    registry: ModelRegistry = Depends(get_model_registry),
) -> StandardResponse[ModelVersionResponse]:
    """Get model details by name and version/stage."""
    try:
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
    registry: ModelRegistry = Depends(get_model_registry),
) -> StandardResponse[list[ModelVersionResponse]]:
    """List all versions of a specific model."""
    try:
        # Search for all versions of this model
        filter_string = f"name='{model_name}'"
        model_versions = registry.search_model_versions(
            filter_string=filter_string,
            max_results=limit,
            order_by=["version_number DESC"],
        )

        versions = []
        skipped_count = 0

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
            except Exception as e:
                # Log the error instead of silently skipping
                logger.warning(
                    "Failed to load model version metadata",
                    model_name=model_name,
                    version=mv.version,
                    error=str(e),
                )
                skipped_count += 1

        if skipped_count > 0:
            logger.info(
                "Model versions list completed with some skipped",
                model_name=model_name,
                loaded_count=len(versions),
                skipped_count=skipped_count,
            )

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
        logger.error(
            "Failed to list model versions", model_name=model_name, error=str(e)
        )
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
    registry: ModelRegistry = Depends(get_model_registry),
) -> StandardResponse[dict]:
    """Deploy a model version to a specific stage."""
    try:
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
    server: ModelServer = Depends(get_model_server),
) -> StandardResponse[InferenceResponse]:
    """Make predictions using a deployed model."""
    try:
        # Override model name in request
        inference_request.model_name = model_name

        logger.info(
            "Starting model prediction",
            model_name=model_name,
            user=current_user.username,
            request_id=inference_request.request_id,
        )

        # Make prediction
        prediction_result = await server.predict(inference_request)

        logger.info(
            "Model prediction completed",
            model_name=model_name,
            predictions_count=len(prediction_result.predictions),
            inference_time_ms=prediction_result.inference_time_ms,
        )

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
    server: ModelServer = Depends(get_model_server),
    registry: ModelRegistry = Depends(get_model_registry),
) -> StandardResponse[dict]:
    """Get model health status and serving statistics."""
    try:
        # Get model-specific stats
        model_stats = server.get_model_stats(model_name)

        if model_stats is None:
            # Model not loaded, try to get registry info
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
    registry: ModelRegistry = Depends(get_model_registry),
    server: ModelServer = Depends(get_model_server),
) -> StandardResponse[dict]:
    """Get model registry statistics."""
    try:
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
    registry: ModelRegistry = Depends(get_model_registry),
) -> StandardResponse[dict]:
    """Delete a registered model and all its versions."""
    try:
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
    registry: ModelRegistry = Depends(get_model_registry),
) -> StandardResponse[list[dict]]:
    """Search models and versions with filters."""
    try:
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
