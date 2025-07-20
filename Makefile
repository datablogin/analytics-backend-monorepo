.PHONY: help install dev-install test lint format type-check clean run-api setup-hooks ci

help:
	@echo "Available commands:"
	@echo "  install      - Install production dependencies"
	@echo "  dev-install  - Install development dependencies"
	@echo "  setup-hooks  - Install pre-commit hooks"
	@echo "  test         - Run tests"
	@echo "  lint         - Run ruff linter"
	@echo "  format       - Format code with ruff"
	@echo "  type-check   - Run mypy type checking"
	@echo "  ci           - Run full CI pipeline locally"
	@echo "  clean        - Clean build artifacts"
	@echo "  run-api      - Run analytics API service"

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