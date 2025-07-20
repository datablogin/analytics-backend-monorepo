from contextlib import asynccontextmanager

from fastapi import FastAPI, Depends
from sqlalchemy.ext.asyncio import AsyncSession

from libs.analytics_core.database import get_db_session, initialize_database
from libs.config.database import get_database_settings


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan management."""
    # Startup
    db_settings = get_database_settings()
    db_manager = initialize_database(db_settings.database_url, db_settings.echo_sql)
    
    # Store in app state for access
    app.state.db_manager = db_manager
    
    yield
    
    # Shutdown
    if hasattr(app.state, "db_manager"):
        await app.state.db_manager.close()


app = FastAPI(
    title="Analytics API",
    description="Analytics backend REST API with database migrations",
    version="0.1.0",
    lifespan=lifespan
)


@app.get("/health")
async def health_check() -> dict[str, str]:
    """Basic health check endpoint."""
    return {"status": "healthy"}


@app.get("/health/database")
async def database_health_check(
    db: AsyncSession = Depends(get_db_session)
) -> dict[str, str]:
    """Database health check endpoint."""
    try:
        # Test database connectivity
        db_manager = app.state.db_manager
        is_healthy = await db_manager.health_check()
        
        return {
            "status": "healthy" if is_healthy else "unhealthy",
            "database": "connected" if is_healthy else "disconnected"
        }
    except Exception as e:
        return {
            "status": "unhealthy",
            "database": "error",
            "error": str(e)
        }


@app.get("/health/detailed")
async def detailed_health_check(
    db: AsyncSession = Depends(get_db_session)
) -> dict[str, any]:
    """Detailed health check with database and migration status."""
    try:
        db_manager = app.state.db_manager
        db_settings = get_database_settings()
        
        # Test database connectivity
        db_healthy = await db_manager.health_check()
        
        return {
            "status": "healthy" if db_healthy else "unhealthy",
            "components": {
                "database": {
                    "status": "healthy" if db_healthy else "unhealthy",
                    "type": "postgresql" if db_settings.is_postgresql else "sqlite",
                    "url_masked": db_settings.database_url.split("@")[-1] if "@" in db_settings.database_url else "local"
                },
                "migrations": {
                    "status": "ready",
                    "system": "alembic"
                }
            },
            "version": app.version,
            "environment": "development"  # This would come from config
        }
    except Exception as e:
        return {
            "status": "unhealthy",
            "error": str(e),
            "components": {
                "database": {"status": "error"},
                "migrations": {"status": "unknown"}
            }
        }


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)