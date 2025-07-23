"""Feature Store Service - Main FastAPI application."""

import os
from contextlib import asynccontextmanager

import structlog
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from libs.analytics_core.database import initialize_database
from libs.api_common.middleware import (
    RequestLoggingMiddleware,
    ResponseStandardizationMiddleware,
    SecurityHeadersMiddleware,
)
from libs.api_common.versioning import APIVersion, VersionedAPIRouter
from libs.config import get_database_settings

from .routes import router as feature_store_router

# Configure structured logging
structlog.configure(
    processors=[
        structlog.stdlib.filter_by_level,
        structlog.stdlib.add_logger_name,
        structlog.stdlib.add_log_level,
        structlog.stdlib.PositionalArgumentsFormatter(),
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.processors.StackInfoRenderer(),
        structlog.processors.format_exc_info,
        structlog.processors.UnicodeDecoder(),
        structlog.processors.JSONRenderer(),
    ],
    context_class=dict,
    logger_factory=structlog.stdlib.LoggerFactory(),
    wrapper_class=structlog.stdlib.BoundLogger,
    cache_logger_on_first_use=True,
)
logger = structlog.get_logger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan manager."""
    logger.info("Starting Feature Store service", service="feature_store")

    # Initialize database
    db_settings = get_database_settings()
    db_manager = initialize_database(db_settings.url, echo=db_settings.echo)  # type: ignore

    # Store database manager in app state
    app.state.db_manager = db_manager

    logger.info(
        "Database initialized", service="feature_store", database_url=db_settings.url  # type: ignore
    )

    yield

    # Cleanup
    await db_manager.close()
    logger.info("Feature Store service stopped", service="feature_store")


# Create FastAPI application
app = FastAPI(
    title="Feature Store Service",
    description="ML Feature Store for managing and serving features",
    version="1.0.0",
    docs_url="/docs",
    redoc_url="/redoc",
    lifespan=lifespan,
)

# Add middleware with secure CORS configuration
allowed_origins = os.getenv(
    "CORS_ALLOWED_ORIGINS", "http://localhost:3000,http://localhost:3001"
).split(",")
allowed_methods = os.getenv(
    "CORS_ALLOWED_METHODS", "GET,POST,PUT,DELETE,OPTIONS"
).split(",")
allowed_headers = os.getenv(
    "CORS_ALLOWED_HEADERS", "Content-Type,Authorization,X-Requested-With"
).split(",")

app.add_middleware(
    CORSMiddleware,
    allow_origins=allowed_origins,
    allow_credentials=True,
    allow_methods=allowed_methods,
    allow_headers=allowed_headers,
)

app.add_middleware(SecurityHeadersMiddleware)
app.add_middleware(RequestLoggingMiddleware)
app.add_middleware(ResponseStandardizationMiddleware)

# Create versioned router
v1_router = VersionedAPIRouter(version=APIVersion.V1)
v1_router.include_router(feature_store_router)

# Include routers
app.include_router(v1_router, prefix="/api")


# Root endpoint
@app.get("/")
async def root():
    """Root endpoint."""
    return {
        "service": "feature_store",
        "version": "1.0.0",
        "status": "healthy",
        "docs": "/docs",
    }


# Health check endpoint
@app.get("/health")
async def health():
    """Health check endpoint."""
    return {
        "service": "feature_store",
        "status": "healthy",
        "database": "connected" if hasattr(app.state, "db_manager") else "disconnected",
    }


if __name__ == "__main__":
    import uvicorn

    db_settings = get_database_settings()
    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=8001,  # Different port from analytics_api
        reload=True,
        log_level="info",
    )
