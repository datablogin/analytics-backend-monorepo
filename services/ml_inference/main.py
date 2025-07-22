"""ML inference service for production model serving."""

import asyncio
from datetime import datetime, timezone
from typing import Any

import structlog
from fastapi import Depends, FastAPI, HTTPException, Request
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
    service_name: str = Field(default="ml_inference", description="Service name")
    version: str = Field(default="1.0.0", description="Service version")
    host: str = Field(default="0.0.0.0", description="Host address")
    port: int = Field(default=8080, description="Port number")
    debug: bool = Field(default=False, description="Debug mode")

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

    # Batch processing
    max_batch_requests: int = Field(
        default=10, description="Maximum number of batch requests"
    )
    batch_timeout_seconds: float = Field(
        default=5.0, description="Batch processing timeout"
    )


class HealthResponse(BaseModel):
    """Health check response."""

    status: str = Field(description="Service status")
    timestamp: datetime = Field(description="Response timestamp")
    version: str = Field(description="Service version")
    uptime_seconds: float = Field(description="Service uptime in seconds")
    loaded_models: int = Field(description="Number of loaded models")
    healthy_models: int = Field(description="Number of healthy models")
    total_requests: int = Field(description="Total requests processed")
    error_rate: float = Field(description="Error rate percentage")


class ModelStatusResponse(BaseModel):
    """Model status response."""

    model_name: str = Field(description="Model name")
    status: str = Field(description="Model status")
    version: str | None = Field(description="Model version")
    stage: str | None = Field(description="Model stage")
    loaded_at: datetime | None = Field(description="Model load timestamp")
    usage_count: int = Field(description="Usage count")
    last_used: datetime | None = Field(description="Last usage timestamp")


class BatchInferenceRequest(BaseModel):
    """Batch inference request."""

    requests: list[InferenceRequest] = Field(description="List of inference requests")
    max_concurrent: int = Field(default=5, description="Maximum concurrent requests")
    fail_fast: bool = Field(default=False, description="Stop on first error")


class BatchInferenceResponse(BaseModel):
    """Batch inference response."""

    responses: list[InferenceResponse] = Field(description="List of responses")
    successful_requests: int = Field(description="Number of successful requests")
    failed_requests: int = Field(description="Number of failed requests")
    total_time_ms: float = Field(description="Total processing time in milliseconds")


class MLInferenceApp:
    """ML Inference application with proper dependency management."""

    def __init__(self, config: MLInferenceConfig | None = None):
        self.config = config or MLInferenceConfig()
        self.model_server: ModelServer | None = None
        self.start_time = datetime.now(timezone.utc)

    def create_app(self) -> FastAPI:
        """Create FastAPI application with proper configuration."""
        app = FastAPI(
            title="ML Inference Service",
            description="Production ML model serving infrastructure",
            version=self.config.version,
            debug=self.config.debug,
        )

        # Add CORS middleware
        app.add_middleware(
            CORSMiddleware,
            allow_origins=self.config.cors_origins,
            allow_credentials=True,
            allow_methods=self.config.cors_methods,
            allow_headers=["*"],
        )

        # Store app instance reference for dependency injection
        app.state.ml_app = self

        # Register event handlers
        app.add_event_handler("startup", self.startup_event)
        app.add_event_handler("shutdown", self.shutdown_event)

        # Register routes
        self._register_routes(app)

        return app

    async def startup_event(self):
        """Initialize service on startup."""
        # Configure observability
        configure_observability(
            service_name=self.config.service_name,
            config=self.config.observability_config,
        )

        # Initialize model server
        self.model_server = ModelServer(self.config.model_serving_config)
        await self.model_server.start()

        logger.info(
            "ML Inference service started",
            service_name=self.config.service_name,
            version=self.config.version,
            host=self.config.host,
            port=self.config.port,
        )

    async def shutdown_event(self):
        """Cleanup on shutdown."""
        if self.model_server:
            await self.model_server.stop()

        logger.info("ML Inference service stopped")

    def _register_routes(self, app: FastAPI):
        """Register all routes with the FastAPI app."""

        @app.get("/health", response_model=HealthResponse)
        async def health_check(request: Request):
            """Health check endpoint."""
            try:
                ml_app = get_ml_app(request)

                if not ml_app.model_server:
                    raise HTTPException(
                        status_code=503, detail="Model server not initialized"
                    )

                stats = ml_app.model_server.get_server_stats()
                uptime = (
                    datetime.now(timezone.utc) - ml_app.start_time
                ).total_seconds()

                return HealthResponse(
                    status="healthy",
                    timestamp=datetime.now(timezone.utc),
                    version=ml_app.config.version,
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
        async def get_model_status(model_name: str, request: Request):
            """Get status of a specific model."""
            try:
                ml_app = get_ml_app(request)

                if not ml_app.model_server:
                    raise HTTPException(
                        status_code=503, detail="Model server not initialized"
                    )

                model_stats = ml_app.model_server.get_model_stats(model_name)
                if not model_stats:
                    raise HTTPException(
                        status_code=404, detail=f"Model '{model_name}' not found"
                    )

                return ModelStatusResponse(
                    model_name=model_stats["name"],
                    status="healthy" if model_stats["healthy"] else "unhealthy",
                    version=model_stats["version"],
                    stage=model_stats["stage"],
                    loaded_at=datetime.fromisoformat(model_stats["last_used"])
                    if model_stats["last_used"]
                    else None,
                    usage_count=model_stats["usage_count"],
                    last_used=datetime.fromisoformat(model_stats["last_used"])
                    if model_stats["last_used"]
                    else None,
                )

            except HTTPException:
                raise
            except Exception as error:
                logger.error(
                    "Failed to get model status",
                    model_name=model_name,
                    error=str(error),
                )
                raise HTTPException(status_code=500, detail=str(error))

        @app.post("/predict", response_model=InferenceResponse)
        async def predict(request_data: InferenceRequest, request: Request):
            """Single model inference endpoint."""
            ml_app = get_ml_app(request)

            if not ml_app.model_server:
                raise HTTPException(
                    status_code=503, detail="Model server not available"
                )

            try:
                response = await ml_app.model_server.predict(request_data)
                return response

            except Exception as error:
                logger.error("Inference request failed", error=str(error))
                raise HTTPException(
                    status_code=500, detail=f"Inference failed: {str(error)}"
                )

        @app.post("/predict/batch", response_model=BatchInferenceResponse)
        async def predict_batch(batch_request: BatchInferenceRequest, request: Request):
            """Batch model inference endpoint."""
            ml_app = get_ml_app(request)

            if not ml_app.model_server:
                raise HTTPException(
                    status_code=503, detail="Model server not available"
                )

            if len(batch_request.requests) > ml_app.config.max_batch_requests:
                raise HTTPException(
                    status_code=400,
                    detail=f"Batch size {len(batch_request.requests)} exceeds maximum {ml_app.config.max_batch_requests}",
                )

            start_time = datetime.now(timezone.utc)
            responses = []
            successful_requests = 0
            failed_requests = 0

            # Process requests with concurrency control
            semaphore = asyncio.Semaphore(batch_request.max_concurrent)

            async def process_request(req: InferenceRequest) -> InferenceResponse:
                async with semaphore:
                    return await ml_app.model_server.predict(req)

            # Execute batch requests
            try:
                tasks = [process_request(req) for req in batch_request.requests]

                # Use timeout for batch processing
                results = await asyncio.wait_for(
                    asyncio.gather(*tasks, return_exceptions=True),
                    timeout=ml_app.config.batch_timeout_seconds,
                )

                for i, result in enumerate(results):
                    if isinstance(result, Exception):
                        failed_requests += 1
                        if batch_request.fail_fast:
                            break
                        # Create error response
                        responses.append(
                            InferenceResponse(
                                predictions=[],
                                model_name=batch_request.requests[i].model_name,
                                model_version="unknown",
                                model_stage=None,
                                inference_time_ms=0,
                                preprocessing_time_ms=0,
                                postprocessing_time_ms=0,
                                request_id=batch_request.requests[i].request_id,
                                status="error",
                                error_message=str(result),
                            )
                        )
                    else:
                        successful_requests += 1
                        responses.append(result)

            except asyncio.TimeoutError:
                logger.error("Batch processing timed out")
                raise HTTPException(
                    status_code=408, detail="Batch processing timed out"
                )
            except Exception as error:
                logger.error("Batch processing failed", error=str(error))
                raise HTTPException(
                    status_code=500, detail=f"Batch processing failed: {str(error)}"
                )

            total_time = (
                datetime.now(timezone.utc) - start_time
            ).total_seconds() * 1000

            return BatchInferenceResponse(
                responses=responses,
                successful_requests=successful_requests,
                failed_requests=failed_requests,
                total_time_ms=total_time,
            )


# Dependency injection function
def get_ml_app(request: Request) -> MLInferenceApp:
    """Get ML application instance from request state."""
    return request.app.state.ml_app


# Create application instance
ml_inference_app = MLInferenceApp()
app = ml_inference_app.create_app()


if __name__ == "__main__":
    import uvicorn

    config = MLInferenceConfig()
    uvicorn.run(
        "main:app",
        host=config.host,
        port=config.port,
        reload=config.debug,
        log_config=None,  # Use structlog configuration
    )
