"""ML inference service for production model serving."""

import asyncio
from datetime import datetime, timezone

import structlog
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field

from libs.ml_models import (
    InferenceRequest,
    InferenceResponse,
    ModelServer,
    ModelServingConfig,
)
from libs.ml_models.config import BaseConfig
from libs.observability import (
    ObservabilityConfig,
    configure_observability,
    request_middleware,
)

logger = structlog.get_logger(__name__)


class MLInferenceConfig(BaseConfig):
    """Configuration for ML inference service."""

    # Service settings
    service_name: str = Field(default="ml-inference", description="Service name")
    version: str = Field(default="1.0.0", description="Service version")
    debug: bool = Field(default=False, description="Debug mode")

    # Server settings
    host: str = Field(default="0.0.0.0", description="Host to bind to")
    port: int = Field(default=8003, description="Port to bind to")
    workers: int = Field(default=1, description="Number of worker processes")

    # CORS settings
    cors_origins: list[str] = Field(default=["*"], description="Allowed CORS origins")
    cors_methods: list[str] = Field(
        default=["GET", "POST"], description="Allowed CORS methods"
    )

    # Model serving configuration
    model_serving_config: ModelServingConfig = Field(
        default_factory=ModelServingConfig, description="Model serving configuration"
    )

    # Observability configuration
    observability_config: ObservabilityConfig = Field(
        default_factory=ObservabilityConfig, description="Observability configuration"
    )


class HealthResponse(BaseModel):
    """Health check response."""

    status: str = Field(description="Service status")
    timestamp: datetime = Field(description="Response timestamp")
    version: str = Field(description="Service version")
    uptime_seconds: float = Field(description="Service uptime in seconds")
    loaded_models: int = Field(description="Number of loaded models")
    healthy_models: int = Field(description="Number of healthy models")
    total_requests: int = Field(description="Total inference requests")
    error_rate: float = Field(description="Error rate percentage")


class ModelStatusResponse(BaseModel):
    """Model status response."""

    model_name: str = Field(description="Model name")
    model_version: str = Field(description="Model version")
    model_stage: str = Field(description="Model stage")
    status: str = Field(description="Model status (healthy/unhealthy)")
    usage_count: int = Field(description="Number of times used")
    failure_count: int = Field(description="Number of failures")
    last_used: datetime = Field(description="Last usage timestamp")
    age_seconds: float = Field(description="Model age in seconds")


class BatchInferenceRequest(BaseModel):
    """Batch inference request."""

    requests: list[InferenceRequest] = Field(description="List of inference requests")
    parallel: bool = Field(default=True, description="Process requests in parallel")
    max_batch_size: int = Field(default=100, description="Maximum batch size")


class BatchInferenceResponse(BaseModel):
    """Batch inference response."""

    responses: list[InferenceResponse] = Field(
        description="List of inference responses"
    )
    total_requests: int = Field(description="Total number of requests")
    successful_requests: int = Field(description="Number of successful requests")
    failed_requests: int = Field(description="Number of failed requests")
    total_time_ms: float = Field(description="Total processing time in milliseconds")


# Global variables
config = MLInferenceConfig()
model_server: ModelServer | None = None
start_time = datetime.now(timezone.utc)

# FastAPI app
app = FastAPI(
    title="ML Inference Service",
    description="Production ML model serving infrastructure",
    version=config.version,
    debug=config.debug,
)

# CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=config.cors_origins,
    allow_credentials=True,
    allow_methods=config.cors_methods,
    allow_headers=["*"],
)


@app.on_event("startup")
async def startup_event():
    """Initialize service on startup."""
    global model_server

    # Configure observability
    configure_observability(
        service_name=config.service_name,
        config=config.observability_config,
    )

    # Add observability middleware
    app.add_middleware(
        request_middleware,
        service_name=config.service_name,
    )

    # Initialize model server
    model_server = ModelServer(config.model_serving_config)
    await model_server.start()

    logger.info(
        "ML Inference service started",
        service_name=config.service_name,
        version=config.version,
        host=config.host,
        port=config.port,
    )


@app.on_event("shutdown")
async def shutdown_event():
    """Cleanup on service shutdown."""
    global model_server

    if model_server:
        await model_server.stop()

    logger.info("ML Inference service stopped")


@app.get("/health", response_model=HealthResponse)
async def health_check():
    """Health check endpoint."""
    try:
        if not model_server:
            raise HTTPException(status_code=503, detail="Model server not initialized")

        stats = model_server.get_server_stats()
        uptime = (datetime.now(timezone.utc) - start_time).total_seconds()

        return HealthResponse(
            status="healthy",
            timestamp=datetime.now(timezone.utc),
            version=config.version,
            uptime_seconds=uptime,
            loaded_models=stats["loaded_models"],
            healthy_models=stats["healthy_models"],
            total_requests=stats["request_count"],
            error_rate=stats["error_rate"] * 100,
        )

    except Exception as error:
        logger.error("Health check failed", error=str(error))
        raise HTTPException(status_code=500, detail=str(error))


@app.get("/models/{model_name}/status", response_model=ModelStatusResponse)
async def get_model_status(model_name: str):
    """Get status of a specific model."""
    try:
        if not model_server:
            raise HTTPException(status_code=503, detail="Model server not initialized")

        stats = model_server.get_model_stats(model_name)
        if not stats:
            raise HTTPException(
                status_code=404, detail=f"Model '{model_name}' not found"
            )

        return ModelStatusResponse(
            model_name=stats["name"],
            model_version=stats["version"],
            model_stage=stats["stage"],
            status="healthy" if stats["healthy"] else "unhealthy",
            usage_count=stats["usage_count"],
            failure_count=stats["failure_count"],
            last_used=datetime.fromisoformat(stats["last_used"]),
            age_seconds=stats["age_seconds"],
        )

    except HTTPException:
        raise
    except Exception as error:
        logger.error(
            "Failed to get model status", model_name=model_name, error=str(error)
        )
        raise HTTPException(status_code=500, detail=str(error))


@app.post("/predict", response_model=InferenceResponse)
async def predict(request: InferenceRequest):
    """Make a single prediction."""
    try:
        if not model_server:
            raise HTTPException(status_code=503, detail="Model server not initialized")

        response = await model_server.predict(request)

        if response.status == "error":
            raise HTTPException(status_code=500, detail=response.error_message)

        return response

    except HTTPException:
        raise
    except Exception as error:
        logger.error("Prediction failed", error=str(error))
        raise HTTPException(status_code=500, detail=str(error))


@app.post("/predict/batch", response_model=BatchInferenceResponse)
async def predict_batch(batch_request: BatchInferenceRequest):
    """Make batch predictions."""
    try:
        if not model_server:
            raise HTTPException(status_code=503, detail="Model server not initialized")

        if len(batch_request.requests) > batch_request.max_batch_size:
            raise HTTPException(
                status_code=400,
                detail=f"Batch size {len(batch_request.requests)} exceeds maximum {batch_request.max_batch_size}",
            )

        start_time = datetime.now(timezone.utc)
        responses = []

        if batch_request.parallel:
            # Process requests in parallel
            tasks = [
                model_server.predict(request) for request in batch_request.requests
            ]
            responses = await asyncio.gather(*tasks, return_exceptions=True)

            # Convert exceptions to error responses
            for i, response in enumerate(responses):
                if isinstance(response, Exception):
                    responses[i] = InferenceResponse(
                        predictions=[],
                        model_name=batch_request.requests[i].model_name,
                        model_version="unknown",
                        inference_time_ms=0,
                        preprocessing_time_ms=0,
                        postprocessing_time_ms=0,
                        status="error",
                        error_message=str(response),
                    )
        else:
            # Process requests sequentially
            for request in batch_request.requests:
                try:
                    response = await model_server.predict(request)
                    responses.append(response)
                except Exception as error:
                    responses.append(
                        InferenceResponse(
                            predictions=[],
                            model_name=request.model_name,
                            model_version="unknown",
                            inference_time_ms=0,
                            preprocessing_time_ms=0,
                            postprocessing_time_ms=0,
                            status="error",
                            error_message=str(error),
                        )
                    )

        # Calculate batch statistics
        total_time = (datetime.now(timezone.utc) - start_time).total_seconds() * 1000
        successful_count = sum(1 for r in responses if r.status == "success")
        failed_count = len(responses) - successful_count

        return BatchInferenceResponse(
            responses=responses,
            total_requests=len(batch_request.requests),
            successful_requests=successful_count,
            failed_requests=failed_count,
            total_time_ms=total_time,
        )

    except HTTPException:
        raise
    except Exception as error:
        logger.error("Batch prediction failed", error=str(error))
        raise HTTPException(status_code=500, detail=str(error))


@app.get("/models")
async def list_models():
    """List all available models."""
    try:
        if not model_server:
            raise HTTPException(status_code=503, detail="Model server not initialized")

        # This is a simplified implementation
        # In practice, you'd query the model registry
        loaded_models = []
        for cache_key, model_container in model_server.model_cache.items():
            loaded_models.append(
                {
                    "name": model_container.name,
                    "version": model_container.version,
                    "stage": model_container.stage,
                    "status": "healthy" if model_container.healthy else "unhealthy",
                    "usage_count": model_container.usage_count,
                    "age_seconds": model_container.age_seconds,
                }
            )

        return {
            "loaded_models": loaded_models,
            "total_models": len(loaded_models),
            "healthy_models": sum(1 for m in loaded_models if m["status"] == "healthy"),
        }

    except HTTPException:
        raise
    except Exception as error:
        logger.error("Failed to list models", error=str(error))
        raise HTTPException(status_code=500, detail=str(error))


@app.post("/models/{model_name}/load")
async def load_model(
    model_name: str, version: str | None = None, stage: str | None = None
):
    """Pre-load a model into cache."""
    try:
        if not model_server:
            raise HTTPException(status_code=503, detail="Model server not initialized")

        # Create a dummy inference request to trigger model loading
        dummy_request = InferenceRequest(
            model_name=model_name,
            model_version=version,
            model_stage=stage,
            inputs={"dummy": "data"},  # This will be validated by the model
        )

        try:
            # This will load the model into cache
            model_container = await model_server._get_model(model_name, version, stage)

            return {
                "message": f"Model '{model_name}' loaded successfully",
                "model_name": model_container.name,
                "model_version": model_container.version,
                "model_stage": model_container.stage,
                "status": "loaded",
            }

        except Exception as load_error:
            logger.error(
                "Failed to load model",
                model_name=model_name,
                version=version,
                stage=stage,
                error=str(load_error),
            )
            raise HTTPException(
                status_code=400,
                detail=f"Failed to load model '{model_name}': {str(load_error)}",
            )

    except HTTPException:
        raise
    except Exception as error:
        logger.error("Model loading failed", error=str(error))
        raise HTTPException(status_code=500, detail=str(error))


@app.delete("/models/{model_name}/unload")
async def unload_model(model_name: str):
    """Unload a model from cache."""
    try:
        if not model_server:
            raise HTTPException(status_code=503, detail="Model server not initialized")

        # Find and remove model from cache
        removed_models = []
        cache_keys_to_remove = []

        async with model_server.cache_lock:
            for cache_key, model_container in model_server.model_cache.items():
                if model_container.name == model_name:
                    cache_keys_to_remove.append(cache_key)
                    removed_models.append(
                        {
                            "name": model_container.name,
                            "version": model_container.version,
                            "stage": model_container.stage,
                        }
                    )

            for cache_key in cache_keys_to_remove:
                del model_server.model_cache[cache_key]

        if not removed_models:
            raise HTTPException(
                status_code=404,
                detail=f"Model '{model_name}' not found in cache",
            )

        return {
            "message": f"Model '{model_name}' unloaded successfully",
            "unloaded_models": removed_models,
        }

    except HTTPException:
        raise
    except Exception as error:
        logger.error("Model unloading failed", error=str(error))
        raise HTTPException(status_code=500, detail=str(error))


@app.get("/metrics")
async def get_metrics():
    """Get service metrics."""
    try:
        if not model_server:
            raise HTTPException(status_code=503, detail="Model server not initialized")

        stats = model_server.get_server_stats()
        uptime = (datetime.now(timezone.utc) - start_time).total_seconds()

        return {
            "service_metrics": {
                "uptime_seconds": uptime,
                "version": config.version,
                "status": "healthy",
            },
            "server_metrics": stats,
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }

    except HTTPException:
        raise
    except Exception as error:
        logger.error("Failed to get metrics", error=str(error))
        raise HTTPException(status_code=500, detail=str(error))


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(
        "main:app",
        host=config.host,
        port=config.port,
        workers=config.workers,
        reload=config.debug,
        access_log=True,
        log_level="debug" if config.debug else "info",
    )
