# Analytics Backend Monorepo

## Bash Commands

### Development
- `make dev-install`: Install all dependencies including dev tools
- `make install`: Install production dependencies only
- `make test`: Run the test suite
- `make lint`: Run ruff linter
- `make format`: Format code with ruff
- `make type-check`: Run mypy type checking
- `make clean`: Clean build artifacts

### Services
- `make run-api`: Start the analytics API service
- `cd services/analytics_api && uvicorn main:app --reload`: Run API with hot reload

### Database Migrations
- `make migrate-create MESSAGE='description'`: Create new migration
- `make migrate-upgrade`: Apply pending migrations
- `make migrate-downgrade`: Rollback last migration
- `make migrate-history`: Show migration history
- `make migrate-current`: Show current migration

### Package Management
- `uv sync`: Install dependencies from lock file
- `uv add <package>`: Add new dependency
- `uv remove <package>`: Remove dependency

### Data Quality Commands
- `make test -k test_data_quality`: Run data quality framework tests
- `python -m libs.data_processing.data_quality_simple`: Test validation framework
- `python -c "from libs.data_processing import DataProfiler; print('Profiler ready')"`: Test profiler
- `python -c "from libs.data_processing import DataLineageTracker; print('Lineage ready')"`: Test lineage tracker

### Issue Management Commands
- `gh issue list --state open`: List all open issues
- `gh issue list --label "needs-triage"`: List issues needing triage
- `gh issue create --template bug_report.yml`: Create bug report
- `gh issue create --template feature_request.yml`: Create feature request
- `gh issue create --template enhancement.yml`: Create enhancement request
- `/groom-backlog`: Run backlog grooming process

## Project Structure

```
/
├── libs/                          # Shared libraries
│   ├── analytics_core/            # Core analytics utilities
│   ├── data_processing/           # ETL, data transformation & quality
│   ├── ml_models/                # ML model utilities
│   ├── api_common/               # Shared API components
│   └── config/                   # Configuration management
├── services/                     # Backend microservices
│   ├── analytics_api/            # Analytics REST API
│   ├── data_ingestion/           # Data pipeline service
│   ├── ml_inference/             # ML prediction service
│   ├── reporting_engine/         # Report generation
│   └── batch_processor/          # Background job processing
├── tools/                        # CLI tools and scripts
├── tests/                        # Integration tests
├── docker/                       # Service containers
├── notebooks/                    # Data exploration
└── docs/                         # API documentation
```

## Code Style

- Use Python 3.11+ features
- Follow ruff formatting (88 char line length)
- Add type hints to all functions
- Use Pydantic for data validation
- Use structured logging with structlog
- Follow async/await patterns for I/O operations

## Development Workflow

1. **Setup**: Run `make dev-install` to install all dependencies
2. **Development**: 
   - Make changes to code
   - Run `make format` to format code
   - Run `make lint` to check for issues
   - Run `make type-check` to verify types
3. **Testing**: Run `make test` before committing
4. **Services**: Use `make run-api` to test the API locally

## Architecture Patterns

- **Shared Libraries**: Common functionality in `libs/`
- **Microservices**: Independent services in `services/`
- **Dependency Management**: Each service has its own `pyproject.toml`
- **Configuration**: Use Pydantic Settings for environment config
- **Logging**: Structured logging with contextual information
- **Error Handling**: Consistent error responses across services

## Repository Etiquette

- Each service is independently deployable
- Shared libraries are imported as local packages
- Use feature branches for development
- All code must pass linting, type checking, and tests
- Document API changes in service-specific docs

## Issue Management Workflow

### Creating Issues
- Use GitHub issue templates for consistency:
  - 🐛 **Bug Report**: For bugs and unexpected behavior
  - ✨ **Feature Request**: For new functionality
  - 🚀 **Enhancement**: For improving existing features
- Include all required information in templates
- Apply appropriate labels (service, priority, type)
- Reference related issues and PRs

### Pull Request Process
- Use the PR template for all submissions
- **REQUIRED**: Always include issue linking syntax in PR description: `Closes #123` or `Fixes #123`
- Include comprehensive test coverage
- Ensure all CI/CD checks pass
- Request review from appropriate team members
- **IMPORTANT**: Always include the Issue Number in the title of the PR you create and push to github

### GitHub Issue Linking Rules
**CRITICAL**: Every PR must include proper GitHub linking syntax to automatically close related issues:
- Use `Closes #123` for feature implementations
- Use `Fixes #123` for bug fixes  
- Use `Resolves #123` for general issue resolution
- Place the linking text in the PR description (not just commit messages)
- Multiple issues can be referenced: `Closes #123, Fixes #456`

### Backlog Grooming
- Regular backlog review using `/groom-backlog` command
- Weekly review of `needs-triage` issues
- Monthly deep-dive on all open issues
- Close obsolete or completed issues promptly
- Maintain clear priority labels and descriptions

### Issue Labels
- **Type**: `bug`, `feature`, `enhancement`, `documentation`
- **Priority**: `priority/critical`, `priority/high`, `priority/medium`, `priority/low`
- **Service**: `analytics-api`, `data-ingestion`, `ml-inference`, etc.
- **Status**: `needs-triage`, `ready-for-development`, `in-progress`, `blocked`
- **Effort**: `effort/small`, `effort/medium`, `effort/large`, `effort/xl`

## Environment Setup

- Python 3.11+ required
- Use `uv` for fast package management
- Set up virtual environment with `uv venv`
- Install pre-commit hooks for code quality

## Testing

- Unit tests for individual functions
- Integration tests for service interactions
- Use pytest with async support
- Test coverage should be >80%
- Mock external dependencies in tests

## Data Quality Framework

The platform includes a comprehensive data quality framework with the following capabilities:

### Core Features
- **Data Validation**: Automated quality checks using configurable expectations
- **Data Profiling**: Statistical analysis and quality assessment  
- **Lineage Tracking**: Complete data asset and transformation tracking
- **Quality Monitoring**: Real-time dashboards and KPI tracking via API endpoints
- **Alerting System**: Multi-channel alerts for quality violations

### Key Components
- `libs/data_processing/data_quality_simple.py`: Main validation framework
- `libs/data_processing/profiling.py`: Data profiling and statistics
- `libs/data_processing/lineage.py`: Data lineage tracking system
- `libs/data_processing/alerting.py`: Quality violation alerting
- `services/analytics_api/routes/data_quality.py`: Monitoring API endpoints
- `services/data_ingestion/main.py`: Integrated quality checkpoints

### Quality KPIs
- **Validation Success Rate**: Target 99.5% for production datasets
- **Lineage Coverage**: 100% coverage for production data assets
- **Detection Time**: <15 minutes for quality violations
- **Data Quality Score**: Overall quality percentage across all datasets

### API Endpoints
- `GET /data-quality/metrics`: Overall quality metrics and KPIs
- `GET /data-quality/validations`: Recent validation results
- `GET /data-quality/profiles`: Data profiling results
- `GET /data-quality/lineage/{asset_name}`: Asset lineage information
- `GET /data-quality/alerts`: Quality violation alerts

### Usage Documentation
See `docs/data_quality_framework.md` for comprehensive setup and usage instructions.