# Database Migration System

This document describes the database migration system using Alembic for the Analytics Backend Monorepo.

## Overview

The migration system provides:
- Automated schema version management
- Safe migration execution with rollback capability
- Environment-specific migration tracking
- Integration with CI/CD pipeline for automated deployments

## Quick Start

### Prerequisites

1. Set the `DATABASE_URL` environment variable:
   ```bash
   export DATABASE_URL="postgresql+asyncpg://user:password@localhost:5432/analytics"
   ```

2. Install dependencies:
   ```bash
   make dev-install
   ```

### Creating Your First Migration

1. Create an initial migration:
   ```bash
   make migrate-create MESSAGE="Initial database schema"
   ```

2. Apply the migration:
   ```bash
   make migrate-upgrade
   ```

3. Check the current migration:
   ```bash
   make migrate-current
   ```

## Migration Commands

### Creating Migrations

```bash
# Create a new migration with autogenerate
make migrate-create MESSAGE="Add user table"

# Manual migration (when autogenerate isn't sufficient)
alembic revision -m "Custom migration"
```

### Applying Migrations

```bash
# Apply all pending migrations
make migrate-upgrade

# Upgrade to specific revision
alembic upgrade <revision>

# Upgrade by relative number
alembic upgrade +2
```

### Rolling Back Migrations

```bash
# Rollback last migration
make migrate-downgrade

# Downgrade to specific revision
alembic downgrade <revision>

# Downgrade by relative number
alembic downgrade -1
```

### Migration Information

```bash
# Show migration history
make migrate-history

# Show current migration
make migrate-current

# Show specific migration details
alembic show <revision>
```

## Environment Configuration

### Database URLs

The system supports multiple database backends:

```bash
# PostgreSQL (recommended for production)
DATABASE_URL="postgresql+asyncpg://user:password@host:5432/database"

# SQLite (for development/testing)
DATABASE_URL="sqlite+aiosqlite:///./analytics.db"

# PostgreSQL with connection options
DATABASE_URL="postgresql+asyncpg://user:password@host:5432/db?sslmode=require"
```

### Environment Variables

| Variable | Description | Default |
|----------|-------------|---------|
| `DATABASE_URL` | Database connection URL | Required |
| `DB_ECHO_SQL` | Echo SQL statements | `false` |
| `DB_POOL_SIZE` | Connection pool size | `5` |
| `DB_MAX_OVERFLOW` | Max connection overflow | `10` |
| `DB_POOL_TIMEOUT` | Pool timeout (seconds) | `30` |

## Model Development

### Creating Models

1. Define models in `libs/analytics_core/models.py`:

```python
from sqlalchemy import String, Integer
from sqlalchemy.orm import Mapped, mapped_column
from libs.analytics_core.database import Base

class Product(Base):
    __tablename__ = "products"
    
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    name: Mapped[str] = mapped_column(String(255), nullable=False)
    price: Mapped[float] = mapped_column(nullable=False)
```

2. Generate migration:
```bash
make migrate-create MESSAGE="Add product table"
```

3. Review the generated migration file in `alembic/versions/`

4. Apply the migration:
```bash
make migrate-upgrade
```

### Best Practices

1. **Always review generated migrations** before applying
2. **Test migrations on development data** before production
3. **Use descriptive migration messages** for better tracking
4. **Backup databases** before applying migrations in production
5. **Keep migrations small and focused** on single changes

## Production Deployment

### Pre-deployment Checklist

- [ ] Review all pending migrations
- [ ] Test migrations on staging environment
- [ ] Backup production database
- [ ] Verify rollback plan
- [ ] Check migration execution time

### Deployment Process

1. **Backup the database**:
   ```bash
   pg_dump $DATABASE_URL > backup_$(date +%Y%m%d_%H%M%S).sql
   ```

2. **Apply migrations**:
   ```bash
   make migrate-upgrade
   ```

3. **Verify deployment**:
   ```bash
   make migrate-current
   curl http://localhost:8000/health/database
   ```

### Zero-Downtime Migrations

For zero-downtime deployments:

1. **Additive changes first** (new columns, tables)
2. **Deploy application** that works with both old and new schema
3. **Remove old code paths** in subsequent deployment
4. **Clean up old schema** in final migration

Example sequence:
```bash
# Step 1: Add new column (nullable)
make migrate-create MESSAGE="Add optional email_verified column"

# Step 2: Deploy app that can handle both states
# Step 3: Backfill data
make migrate-create MESSAGE="Backfill email_verified column"

# Step 4: Make column non-nullable
make migrate-create MESSAGE="Make email_verified non-nullable"
```

## Troubleshooting

### Common Issues

1. **Migration conflicts**:
   ```bash
   # Merge conflicting heads
   alembic merge heads -m "Merge migrations"
   ```

2. **Out-of-sync database**:
   ```bash
   # Stamp current state without running migrations
   alembic stamp head
   ```

3. **Failed migration**:
   ```bash
   # Check current state
   make migrate-current
   
   # Manual rollback if needed
   make migrate-downgrade
   ```

### Performance Issues

1. **Large table migrations**:
   - Use `op.bulk_insert()` for data migrations
   - Consider `CREATE INDEX CONCURRENTLY` for PostgreSQL
   - Break large changes into smaller batches

2. **Migration timeouts**:
   ```bash
   # Increase timeout
   export DB_MIGRATION_TIMEOUT=600
   ```

## Testing

### Running Migration Tests

```bash
# Run migration-specific tests
pytest tests/test_database_migrations.py -v

# Test with different database backends
TEST_DATABASE_URL="sqlite:///test.db" pytest tests/test_database_migrations.py
```

### Test Database Setup

For testing, use a separate test database:

```bash
export TEST_DATABASE_URL="postgresql+asyncpg://user:password@localhost:5432/analytics_test"
```

## Integration with Services

### FastAPI Integration

The analytics API automatically initializes the database on startup:

```python
from libs.analytics_core.database import get_db_session

@app.get("/users")
async def get_users(db: AsyncSession = Depends(get_db_session)):
    # Use database session
    pass
```

### Health Checks

Monitor database health:

```bash
# Basic health check
curl http://localhost:8000/health

# Database-specific health check
curl http://localhost:8000/health/database

# Detailed health check with migration status
curl http://localhost:8000/health/detailed
```

## Security Considerations

1. **Never commit database URLs** with real credentials
2. **Use environment variables** for all database configuration
3. **Limit migration user permissions** in production
4. **Audit migration history** for compliance
5. **Encrypt database connections** in production

## Performance Monitoring

Key metrics to monitor:

- Migration execution time
- Database connection pool utilization
- Query performance after schema changes
- Lock duration during migrations
- Rollback frequency and success rate

Example monitoring queries:

```sql
-- Check migration history
SELECT * FROM alembic_version;

-- Monitor active connections
SELECT count(*) FROM pg_stat_activity WHERE state = 'active';

-- Check for long-running queries
SELECT query, state, query_start 
FROM pg_stat_activity 
WHERE query_start < now() - interval '1 minute';
```