from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from datetime import datetime

from fastapi import Depends, FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

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
from libs.observability import (
    ObservabilityMiddleware,
    configure_observability,
    trace_function,
)

from .routes import dashboard, exports, reports


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
    """Application lifespan management."""
    # Startup
    import os

    database_url = os.getenv(
        "DATABASE_URL", "sqlite+aiosqlite:///./reporting_engine.db"
    )
    initialize_database(database_url)
    configure_observability()

    # Initialize Celery if needed
    from .celery_app import init_celery

    init_celery()

    yield

    # Shutdown
    # Clean up resources if needed


app = FastAPI(
    title="Reporting Engine API",
    description="Analytics reporting and dashboard generation service",
    version="1.0.0",
    openapi_url="/api/v1/openapi.json",
    docs_url=None,  # Disabled default docs
    redoc_url=None,  # Disabled default redoc
    lifespan=lifespan,
)

# Add middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Configure properly for production
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.add_middleware(SecurityHeadersMiddleware)
app.add_middleware(RateLimitMiddleware, requests_per_minute=100)
app.add_middleware(ResponseStandardizationMiddleware)
app.add_middleware(RequestLoggingMiddleware)
app.add_middleware(ObservabilityMiddleware)


# Create versioned API router
v1_router = VersionedAPIRouter(
    version=APIVersion.V1,
    prefix="/api/v1",
    tags=["v1"],
)

# Include route modules
v1_router.include_router(reports.router, prefix="/reports", tags=["Reports"])
v1_router.include_router(dashboard.router, prefix="/dashboard", tags=["Dashboard"])
v1_router.include_router(exports.router, prefix="/exports", tags=["Exports"])

app.include_router(v1_router)


@app.get(
    "/health",
    response_model=StandardResponse[HealthStatus],
    summary="Health Check",
    description="Check service health and readiness",
)
@trace_function("health_check")
async def health_check(
    request: Request,
    db: AsyncSession = Depends(get_db_session),
) -> StandardResponse[HealthStatus]:
    """Health check endpoint."""

    # Test database connection
    try:
        await db.execute(text("SELECT 1"))
        db_status = "healthy"
    except Exception:
        db_status = "unhealthy"

    # Test Celery connection
    try:
        from .celery_app import celery_app

        celery_inspect = celery_app.control.inspect()
        active_queues = celery_inspect.active_queues()
        celery_status = "healthy" if active_queues is not None else "unhealthy"
    except Exception:
        celery_status = "unhealthy"

    overall_status = (
        "healthy"
        if all([db_status == "healthy", celery_status == "healthy"])
        else "unhealthy"
    )

    health_data = HealthStatus(
        status=overall_status,
        version="1.0.0",
        checks={
            "database": db_status,
            "celery": celery_status,
        },
    )

    return StandardResponse(
        success=True,
        data=health_data,
        metadata=APIMetadata(
            version="v1",
            timestamp=datetime.utcnow(),
        ),
    )


@app.get("/")
async def root():
    """Root endpoint with service information."""
    return {
        "service": "Reporting Engine API",
        "version": "1.0.0",
        "description": "Analytics reporting and dashboard generation service",
        "docs": "/docs",
        "health": "/health",
    }


# Custom documentation endpoints
@app.get("/docs", include_in_schema=False)
async def custom_swagger_ui_html():
    """Custom Swagger UI documentation."""
    return create_custom_docs_html(
        openapi_url="/api/v1/openapi.json",
        title="Reporting Engine API Documentation",
    )


@app.get("/redoc", include_in_schema=False)
async def redoc_html():
    """Custom ReDoc documentation."""
    return create_custom_redoc_html(
        openapi_url="/api/v1/openapi.json",
        title="Reporting Engine API Documentation",
    )


# Override OpenAPI schema
def custom_openapi():
    if app.openapi_schema:
        return app.openapi_schema

    app.openapi_schema = create_enhanced_openapi_schema(
        app=app,
        title="Reporting Engine API",
        version="1.0.0",
        description="Analytics reporting and dashboard generation service",
    )
    return app.openapi_schema


app.openapi = custom_openapi


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=8002,
        reload=True,
        log_level="info",
    )
