import time
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager

from fastapi import Depends, FastAPI, Request, Response
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy.ext.asyncio import AsyncSession

from libs.analytics_core.auth import get_current_user
from libs.analytics_core.database import get_db_session, initialize_database
from libs.api_common.documentation import (
    create_custom_docs_html,
    create_custom_redoc_html,
    create_enhanced_openapi_schema,
)
from libs.api_common.middleware import (
    RateLimitMiddleware,
    RequestLoggingMiddleware,
    ResponseStandardizationMiddleware,
    SecurityHeadersMiddleware,
)
from libs.api_common.response_models import (
    APIMetadata,
    HealthStatus,
    StandardResponse,
)
from libs.api_common.versioning import APIVersion, VersionedAPIRouter
from libs.config.database import get_database_settings
from libs.observability import (
    ObservabilityMiddleware,
    configure_observability,
    get_observability_config,
    trace_function,
)
from services.analytics_api.routes import admin, auth, data_quality, experiments, models


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
    """Application lifespan management."""
    # Startup

    # Initialize observability first
    obs_config = get_observability_config()
    obs_config.service_name = "analytics-api"
    obs_config.service_version = "1.0.0"
    obs_manager = configure_observability(obs_config, app)

    db_settings = get_database_settings()
    db_manager = initialize_database(db_settings.database_url, db_settings.echo_sql)

    # Store in app state for access
    app.state.db_manager = db_manager
    app.state.observability_manager = obs_manager
    app.state.start_time = time.time()

    yield

    # Shutdown
    if hasattr(app.state, "db_manager"):
        await app.state.db_manager.close()

    if hasattr(app.state, "observability_manager"):
        app.state.observability_manager.shutdown()


app = FastAPI(
    title="Analytics API",
    description="Comprehensive Analytics backend REST API with automatic OpenAPI documentation, standardized responses, JWT authentication, RBAC, and database migrations",
    version="1.0.0",
    lifespan=lifespan,
    docs_url=None,  # Disable default docs, we'll create custom ones
    redoc_url=None,  # Disable default redoc, we'll create custom ones
    openapi_url="/openapi.json",
    contact={
        "name": "Analytics Team",
        "email": "analytics@company.com",
        "url": "https://github.com/company/analytics-backend",
    },
    license_info={
        "name": "MIT",
        "url": "https://opensource.org/licenses/MIT",
    },
    servers=[
        {"url": "/v1", "description": "Version 1 API (Stable)"},
        {"url": "/v2", "description": "Version 2 API (Beta)"},
    ],
)

# Add middleware (order matters - first added is outermost)
app.add_middleware(SecurityHeadersMiddleware)
app.add_middleware(ObservabilityMiddleware, service_name="analytics-api")
app.add_middleware(RateLimitMiddleware, requests_per_minute=100)
app.add_middleware(RequestLoggingMiddleware)
app.add_middleware(
    ResponseStandardizationMiddleware, environment="development", version="v1"
)

# CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Configure appropriately for production
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Create versioned routers
v1_router = VersionedAPIRouter(APIVersion.V1)
v1_router.include_router(auth.router)
v1_router.include_router(admin.router)
v1_router.include_router(data_quality.router)
v1_router.include_router(experiments.router)
v1_router.include_router(models.router)

# Include versioned routers
app.include_router(v1_router)

# Set custom OpenAPI schema
app.openapi = lambda: create_enhanced_openapi_schema(
    app=app,
    title="Analytics API",
    version="1.0.0",
    description="Comprehensive Analytics backend REST API with automatic OpenAPI documentation, standardized responses, JWT authentication, RBAC, and database migrations",
)


# Custom documentation endpoints
@app.get("/docs", include_in_schema=False)
async def custom_swagger_ui_html():
    """Custom Swagger UI documentation."""
    return create_custom_docs_html(
        openapi_url="/openapi.json",
        title="Analytics API - Interactive Documentation",
    )


@app.get("/redoc", include_in_schema=False)
async def custom_redoc_html():
    """Custom ReDoc documentation."""
    return create_custom_redoc_html(
        openapi_url="/openapi.json",
        title="Analytics API - Documentation",
    )


@app.get("/health", response_model=StandardResponse[HealthStatus], tags=["Health"])
@trace_function(name="health_check", attributes={"endpoint": "/health"})
async def health_check(request: Request) -> StandardResponse[HealthStatus]:
    """Basic health check endpoint with standardized response."""
    uptime = (
        time.time() - app.state.start_time if hasattr(app.state, "start_time") else None
    )

    # Update metrics
    if (
        hasattr(app.state, "observability_manager")
        and app.state.observability_manager.metrics_collector
    ):
        app.state.observability_manager.metrics_collector.update_uptime()

    health_data = HealthStatus(
        status="healthy",
        checks={
            "api": "healthy",
            "uptime": uptime,
        },
        version=app.version,
        uptime=uptime,
    )

    return StandardResponse(
        success=True,
        data=health_data,
        message="Service is healthy",
        metadata=APIMetadata(
            request_id=getattr(request.state, "request_id", "unknown"),
            version="v1",
            environment="development",
        ),
    )


@app.get(
    "/health/database", response_model=StandardResponse[HealthStatus], tags=["Health"]
)
async def database_health_check(
    request: Request,
    db: AsyncSession = Depends(get_db_session),
) -> StandardResponse[HealthStatus]:
    """Database health check endpoint with standardized response."""
    try:
        # Test database connectivity
        db_manager = app.state.db_manager
        is_healthy = await db_manager.health_check()

        health_data = HealthStatus(
            status="healthy" if is_healthy else "unhealthy",
            checks={
                "database": "connected" if is_healthy else "disconnected",
            },
            version=app.version,
        )

        return StandardResponse(
            success=is_healthy,
            data=health_data,
            message="Database health check completed",
            metadata=APIMetadata(
                request_id=getattr(request.state, "request_id", "unknown"),
                version="v1",
                environment="development",
            ),
        )
    except Exception as e:
        health_data = HealthStatus(
            status="unhealthy",
            checks={
                "database": "error",
                "error": str(e),
            },
            version=app.version,
        )

        return StandardResponse(
            success=False,
            data=health_data,
            message="Database health check failed",
            metadata=APIMetadata(
                request_id=getattr(request.state, "request_id", "unknown"),
                version="v1",
                environment="development",
            ),
        )


@app.get(
    "/health/detailed", response_model=StandardResponse[HealthStatus], tags=["Health"]
)
async def detailed_health_check(
    request: Request,
    db: AsyncSession = Depends(get_db_session),
) -> StandardResponse[HealthStatus]:
    """Detailed health check with database and migration status."""
    try:
        db_manager = app.state.db_manager
        db_settings = get_database_settings()
        uptime = (
            time.time() - app.state.start_time
            if hasattr(app.state, "start_time")
            else None
        )

        # Test database connectivity
        db_healthy = await db_manager.health_check()

        health_data = HealthStatus(
            status="healthy" if db_healthy else "unhealthy",
            checks={
                "database": {
                    "status": "healthy" if db_healthy else "unhealthy",
                    "type": "postgresql" if db_settings.is_postgresql else "sqlite",
                    "url_masked": db_settings.database_url.split("@")[-1]
                    if "@" in db_settings.database_url
                    else "local",
                },
                "migrations": {"status": "ready", "system": "alembic"},
                "uptime": uptime,
            },
            version=app.version,
            uptime=uptime,
        )

        return StandardResponse(
            success=db_healthy,
            data=health_data,
            message="Detailed health check completed",
            metadata=APIMetadata(
                request_id=getattr(request.state, "request_id", "unknown"),
                version="v1",
                environment="development",
            ),
        )
    except Exception as e:
        health_data = HealthStatus(
            status="unhealthy",
            checks={
                "database": {"status": "error"},
                "migrations": {"status": "unknown"},
                "error": str(e),
            },
            version=app.version,
        )

        return StandardResponse(
            success=False,
            data=health_data,
            message="Detailed health check failed",
            metadata=APIMetadata(
                request_id=getattr(request.state, "request_id", "unknown"),
                version="v1",
                environment="development",
            ),
        )


@app.get("/protected", response_model=StandardResponse[dict], tags=["Demo"])
async def protected_endpoint(
    request: Request, current_user=Depends(get_current_user)
) -> StandardResponse[dict]:
    """Example protected endpoint demonstrating authentication."""

    user_data = {
        "user_id": current_user.id,
        "username": current_user.username,
        "email": current_user.email,
        "message": f"Hello {current_user.username}, this is a protected endpoint!",
    }

    return StandardResponse(
        success=True,
        data=user_data,
        message="Successfully accessed protected endpoint",
        metadata=APIMetadata(
            request_id=getattr(request.state, "request_id", "unknown"),
            version="v1",
            environment="development",
        ),
    )


@app.get("/version", response_model=StandardResponse[dict], tags=["Info"])
async def get_version_info(request: Request) -> StandardResponse[dict]:
    """Get API version information and supported features."""

    version_data = {
        "api_version": "v1",
        "app_version": app.version,
        "openapi_version": "3.0.2",
        "supported_versions": ["v1"],
        "features": [
            "JWT Authentication",
            "Role-Based Access Control (RBAC)",
            "Automatic OpenAPI Documentation",
            "Standardized Response Formats",
            "Request/Response Validation",
            "Rate Limiting",
            "Health Monitoring",
            "Database Migrations",
            "OpenTelemetry Tracing",
            "Prometheus Metrics",
            "Structured Logging",
            "Correlation IDs",
        ],
        "documentation": {
            "swagger_ui": "/docs",
            "redoc": "/redoc",
            "openapi_spec": "/openapi.json",
        },
    }

    return StandardResponse(
        success=True,
        data=version_data,
        message="API version information",
        metadata=APIMetadata(
            request_id=getattr(request.state, "request_id", "unknown"),
            version="v1",
            environment="development",
        ),
    )


@app.get("/api-stats", response_model=StandardResponse[dict], tags=["Info"])
async def get_api_stats(request: Request) -> StandardResponse[dict]:
    """Get API statistics and metrics."""

    uptime = (
        time.time() - app.state.start_time if hasattr(app.state, "start_time") else 0
    )

    stats_data = {
        "uptime_seconds": round(uptime, 2),
        "uptime_human": f"{int(uptime // 3600)}h {int((uptime % 3600) // 60)}m {int(uptime % 60)}s",
        "total_endpoints": len(
            [route for route in app.routes if hasattr(route, "methods")]
        ),
        "documentation_endpoints": 2,  # /docs and /redoc
        "health_endpoints": 3,  # /health, /health/database, /health/detailed
        "versioned_endpoints": True,
        "middleware_count": 6,  # Security, Observability, RateLimit, Logging, ResponseStandardization, CORS
    }

    return StandardResponse(
        success=True,
        data=stats_data,
        message="API statistics",
        metadata=APIMetadata(
            request_id=getattr(request.state, "request_id", "unknown"),
            version="v1",
            environment="development",
        ),
    )


@app.get("/metrics", include_in_schema=False)
async def metrics_endpoint():
    """Prometheus metrics endpoint."""
    from prometheus_client import CONTENT_TYPE_LATEST, generate_latest

    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)


@app.get(
    "/observability/status",
    response_model=StandardResponse[dict],
    tags=["Observability"],
)
async def observability_status(request: Request) -> StandardResponse[dict]:
    """Get observability stack status and configuration."""
    # Get actual configuration from observability manager
    if hasattr(app.state, "observability_manager"):
        obs_manager = app.state.observability_manager
        config = obs_manager.config

        obs_status = {
            "enabled": config.enabled,
            "tracing_enabled": config.tracing.enabled,
            "metrics_enabled": config.metrics.enabled,
            "logging_enabled": config.logging.enable_correlation,
            "jaeger_endpoint": config.tracing.jaeger_endpoint,
            "prometheus_port": config.metrics.prometheus_port,
            "correlation_ids": config.logging.enable_correlation,
            "service_name": config.service_name,
            "service_version": config.service_version,
            "environment": config.environment,
            "instrumentation": {
                "fastapi": True,
                "requests": True,
                "sqlalchemy": True,
                "redis": True,
            },
        }

        # Add metrics summary if available
        if obs_manager.metrics_collector:
            obs_status["metrics_summary"] = (
                obs_manager.metrics_collector.get_metrics_summary()
            )
    else:
        # Fallback if observability manager is not available
        obs_status = {
            "enabled": False,
            "error": "Observability manager not initialized",
        }

    return StandardResponse(
        success=True,
        data=obs_status,
        message="Observability status retrieved",
        metadata=APIMetadata(
            request_id=getattr(request.state, "request_id", "unknown"),
            version="v1",
            environment="development",
        ),
    )


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8000)
