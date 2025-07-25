# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

# Analytics Backend Monorepo

## Essential Commands

### Development Workflow
- `make dev-install`: Install all dependencies including dev tools
- `make test`: Run the complete test suite
- `make lint`: Run ruff linter
- `make format`: Format code with ruff  
- `make type-check`: Run mypy type checking
- `make ci`: Run full CI pipeline locally (lint + format + type-check + test)
- `make clean`: Clean build artifacts

### Running Services
- `make run-api`: Start analytics API service at http://localhost:8000
- `cd services/analytics_api && uvicorn main:app --reload`: Run API with hot reload
- `cd docker && docker-compose up -d`: Start all services with Docker

### Testing Patterns
- `pytest`: Run all tests
- `pytest -k test_streaming`: Run streaming analytics tests only
- `pytest tests/libs/test_data_processing.py`: Run specific test file
- `pytest --cov=libs --cov=services`: Run with coverage reporting
- `python -m libs.data_processing.data_quality_simple`: Test data quality framework

### Database Operations  
- `make migrate-create MESSAGE='description'`: Create new migration
- `make migrate-upgrade`: Apply pending migrations
- `make migrate-downgrade`: Rollback last migration

### Package Management
- `uv sync`: Install dependencies from lock file
- `uv add <package>`: Add new dependency
- `uv remove <package>`: Remove dependency

## Architecture Overview

This is a **Python monorepo** with a **shared libraries + microservices** architecture:

### Core Design Principles
- **Shared Libraries First**: Common functionality lives in `libs/` and is imported by services
- **Independent Services**: Each service in `services/` has its own `pyproject.toml` and can be deployed independently  
- **Type Safety**: Strict mypy typing with Pydantic for data validation
- **Async-First**: All I/O operations use async/await patterns
- **Structured Logging**: Using structlog for consistent, contextual logging

### Library Architecture (`libs/`)

**Core Libraries:**
- `analytics_core/`: Database models, authentication, core utilities
- `api_common/`: FastAPI middleware, response models, versioning, documentation
- `config/`: Pydantic Settings-based configuration management
- `observability/`: OpenTelemetry instrumentation, metrics, tracing, logging

**Data & ML Libraries:**
- `data_processing/`: ETL, data quality framework, profiling, lineage tracking
- `ml_models/`: MLflow integration, model registry, serving, experiment tracking
- `data_warehouse/`: Multi-cloud data warehouse connectors (BigQuery, Snowflake, Redshift)
- `workflow_orchestration/`: DAG execution engine, scheduling, retry mechanisms

**Real-time Processing:**
- `streaming_analytics/`: Complete streaming platform with Kafka, WebSockets, real-time ML

### Service Architecture (`services/`)

**Production Services:**
- `analytics_api/`: Main REST API with comprehensive middleware stack
- `data_ingestion/`: Data pipeline service with quality checkpoints
- `ml_inference/`: ML prediction service
- `batch_processor/`: Background job processing with Celery
- `feature_store/`: ML feature store with lineage and monitoring

**Incomplete Services:**
- `reporting_engine/`: Report generation (has pyproject.toml only - see Issue #46)

### Critical Architecture Patterns

**Service Bootstrapping Pattern:**
All services follow the same initialization pattern:
```python
@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
    # Initialize database, observability, etc.
    yield
    # Cleanup

app = FastAPI(lifespan=lifespan)
```

**Shared Library Import Pattern:**
Services import shared libraries as local packages:
```python
from libs.analytics_core.auth import get_current_user
from libs.api_common.middleware import RequestLoggingMiddleware
from libs.observability import ObservabilityMiddleware
```

**Configuration Management Pattern:**
All configuration uses Pydantic Settings with environment variable overrides:
```python
from libs.config.database import get_database_settings
settings = get_database_settings()
```

**Database Session Pattern:**
Async database sessions are managed via dependency injection:
```python
from libs.analytics_core.database import get_db_session
async def endpoint(db: AsyncSession = Depends(get_db_session)):
```

## Streaming Analytics Infrastructure

The `libs/streaming_analytics/` library provides a production-ready streaming platform:

**Core Components:**
- `KafkaManager`: Async Kafka producer/consumer with topic management
- `StreamProcessor`/`WindowedProcessor`: Stream processing with windowing (tumbling, sliding, session)
- `RealtimeMLPipeline`: Real-time ML inference with model caching
- `WebSocketServer`: Live dashboard updates with authentication
- `StreamingMetrics`: Monitoring, alerting, and auto-scaling

**Performance Targets:**
- < 100ms end-to-end latency
- 100,000+ events/sec throughput
- < 50ms ML inference latency
- 99.9% uptime with auto-scaling

**Integration Pattern:**
```python
from libs.streaming_analytics import (
    StreamingConfig, KafkaManager, StreamProcessor, 
    WindowType, AggregationType, RealtimeMLPipeline
)
```

## Data Quality Framework

The `libs/data_processing/` library includes a comprehensive data quality system:

**Key Components:**
- `data_quality_simple.py`: Main validation framework with configurable expectations
- `profiling.py`: Statistical analysis and quality assessment
- `lineage.py`: Data asset and transformation tracking
- `alerting.py`: Multi-channel quality violation alerts

**Quality KPIs:**
- Validation Success Rate: Target 99.5% for production datasets
- Lineage Coverage: 100% coverage for production data assets
- Detection Time: <15 minutes for quality violations

**API Integration:**
Quality metrics are exposed via `services/analytics_api/routes/data_quality.py`

## Development Standards

**Code Quality Requirements:**
- Python 3.11+ features required
- 88 character line length (ruff formatting)
- Type hints on all functions (strict mypy)
- Structured logging with contextual information
- Async/await for all I/O operations
- Test coverage >80%

**Python Version Compatibility:**
- Primary: Python 3.11+  
- CI Testing: Python 3.10, 3.11, 3.12
- Type checking requires tuple syntax: `isinstance(v, (int, float))` not `int | float` for Python 3.10 compatibility

**Import Guidelines:**
- Shared libraries: `from libs.{library}.{module} import {item}`
- Services are independent and don't import from each other
- External dependencies managed via `uv` with lock files
- Each service has its own dependency isolation

## Testing Architecture

**Test Organization:**
- `tests/libs/`: Unit tests for shared libraries
- `tests/test_{feature}.py`: Integration tests for cross-cutting features
- `services/{service}/tests/`: Service-specific tests
- `tests/conftest.py`: Pytest fixtures for database, auth, etc.

**Testing Patterns:**
- Async test support with pytest-asyncio
- Database fixtures with cleanup
- Mock external dependencies (Kafka, ML models)
- Comprehensive streaming analytics test suite (10+ tests)

## Error Handling & Observability

**Structured Logging:**
All services use structlog with contextual information:
```python
logger.info("Event processed", event_id=event.id, duration_ms=processing_time)
```

**OpenTelemetry Integration:**
Full observability stack via `libs/observability/`:
- Distributed tracing with Jaeger
- Metrics with Prometheus
- Request instrumentation for FastAPI, SQLAlchemy, Redis

**Error Response Standardization:**
All APIs use `libs/api_common/response_models.py` for consistent error formats.

## GitHub Workflow Integration

**Issue Linking Requirements:**
- Every PR must include `Closes #123` or `Fixes #123` in description
- PR titles must include issue number: "Issue #123: Feature description"
- Use GitHub CLI: `gh issue create`, `gh pr create`, `gh issue list`

**CI/CD Pipeline:**
- Multi-Python testing (3.10, 3.11, 3.12)
- Ruff linting and formatting validation
- MyPy type checking with excluded problematic files
- Docker build verification
- Security scanning

## Key Architectural Dependencies

**Critical Library Interdependencies:**
- `analytics_core` → Foundation for all services (database, auth, models)
- `api_common` → Required by all FastAPI services (middleware, responses)
- `config` → Configuration management for all components
- `observability` → Monitoring/tracing for all services
- `streaming_analytics` → Integrates with `ml_models` for real-time inference

**External System Integrations:**
- **Kafka**: Event streaming (aiokafka, confluent-kafka)
- **MLflow**: Model registry and experiment tracking  
- **PostgreSQL**: Primary database with async sessions (asyncpg)
- **Redis**: Caching and session storage
- **Multiple Data Warehouses**: BigQuery, Snowflake, Redshift connectors

**Performance-Critical Paths:**
- Streaming pipeline: Kafka → Stream Processing → ML Inference → WebSocket delivery
- Data quality: Ingestion → Validation → Profiling → Alerting
- API requests: Middleware stack → Authentication → Business logic → Response standardization

## Active Development Areas

**Current Open Issues (recently created):**
- Issue #46: Implement Reporting Engine Service
- Issue #47: End-to-End Integration Testing for Streaming Analytics  
- Issue #48: Complete Streaming Analytics Documentation
- Issue #49: Production Deployment Configuration
- Issue #50: Complete ML Model Store Integration
- Issue #51: Security Hardening for Streaming Analytics
- Issue #52: Performance Optimization and Benchmarking