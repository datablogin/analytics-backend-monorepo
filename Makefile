.PHONY: help install dev-install test test-integration test-performance lint format type-check clean run-api setup-hooks ci migrate-create migrate-upgrade migrate-downgrade migrate-history migrate-current

help:
	@echo "Available commands:"
	@echo "  install      - Install production dependencies"
	@echo "  dev-install  - Install development dependencies"
	@echo "  setup-hooks  - Install pre-commit hooks"
	@echo "  test         - Run unit tests"
	@echo "  test-integration - Run integration tests"
	@echo "  test-performance - Run integration tests with performance tests"
	@echo "  lint         - Run ruff linter"
	@echo "  format       - Format code with ruff"
	@echo "  type-check   - Run mypy type checking"
	@echo "  ci           - Run full CI pipeline locally"
	@echo "  clean        - Clean build artifacts"
	@echo "  run-api      - Run analytics API service"
	@echo ""
	@echo "Database Migration Commands:"
	@echo "  migrate-create   - Create new migration (requires MESSAGE)"
	@echo "  migrate-upgrade  - Apply pending migrations"
	@echo "  migrate-downgrade- Rollback last migration"
	@echo "  migrate-history  - Show migration history"
	@echo "  migrate-current  - Show current migration"

install:
	uv sync

dev-install:
	uv sync --extra dev --extra test --extra ml

test:
	pytest --ignore=tests/integration

test-integration:
	@echo "🧪 Running integration tests for streaming analytics..."
	RUN_INTEGRATION_TESTS=1 ./scripts/run_integration_tests.sh

test-performance:
	@echo "🏃 Running integration tests with performance tests..."
	RUN_INTEGRATION_TESTS=1 ./scripts/run_integration_tests.sh --performance

lint:
	ruff check .

format:
	ruff format .

type-check:
	mypy libs/ services/ --ignore-missing-imports --explicit-package-bases

clean:
	find . -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null || true
	find . -type f -name "*.pyc" -delete
	find . -type d -name "*.egg-info" -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name ".pytest_cache" -exec rm -rf {} + 2>/dev/null || true

setup-hooks:
	uv run pre-commit install

ci: lint format type-check test
	@echo "✅ All CI checks passed!"

run-api:
	cd services/analytics_api && uvicorn main:app --reload --host 0.0.0.0 --port 8000

# Database Migration Commands
migrate-create:
	@if [ -z "$(MESSAGE)" ]; then echo "Usage: make migrate-create MESSAGE='migration description'"; exit 1; fi
	alembic revision --autogenerate -m "$(MESSAGE)"

migrate-upgrade:
	alembic upgrade head

migrate-downgrade:
	alembic downgrade -1

migrate-history:
	alembic history

migrate-current:
	alembic current