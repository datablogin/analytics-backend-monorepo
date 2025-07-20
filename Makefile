.PHONY: help install dev-install test lint format type-check clean run-api migrate-create migrate-upgrade migrate-downgrade migrate-history migrate-current

help:
	@echo "Available commands:"
	@echo "  install      - Install production dependencies"
	@echo "  dev-install  - Install development dependencies"
	@echo "  test         - Run tests"
	@echo "  lint         - Run ruff linter"
	@echo "  format       - Format code with ruff"
	@echo "  type-check   - Run mypy type checking"
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
	pytest

lint:
	ruff check .

format:
	ruff format .

type-check:
	mypy .

clean:
	find . -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null || true
	find . -type f -name "*.pyc" -delete
	find . -type d -name "*.egg-info" -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name ".pytest_cache" -exec rm -rf {} + 2>/dev/null || true

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