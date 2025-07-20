# Analytics Backend Monorepo

A modern Python monorepo for analytics backend services with shared libraries and microservices architecture.

## 🏗️ Architecture

This monorepo provides a scalable foundation for analytics applications with:

- **5 Shared Libraries** for common functionality
- **5 Microservices** for independent deployment
- **Modern Python Tooling** (uv, ruff, mypy)
- **Container Support** with Docker Compose
- **Developer Experience** with automated workflows

## 📁 Project Structure

```
/
├── libs/                          # Shared libraries
│   ├── analytics_core/            # Core analytics utilities
│   ├── data_processing/           # ETL and data transformation
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

## 🚀 Quick Start

### Prerequisites

- Python 3.10+ (tested on 3.10, 3.11, 3.12)
- [uv](https://github.com/astral-sh/uv) for fast package management
- [GitHub CLI](https://cli.github.com/) (optional, for repo management)

### Setup

1. **Clone the repository:**
   ```bash
   git clone https://github.com/datablogin/analytics-backend-monorepo.git
   cd analytics-backend-monorepo
   ```

2. **Install dependencies:**
   ```bash
   make dev-install
   ```

3. **Run the analytics API:**
   ```bash
   make run-api
   ```

4. **Test the setup:**
   ```bash
   curl http://localhost:8000/health
   ```

## 🛠️ Development

### Available Commands

```bash
make help                # Show available commands
make install            # Install production dependencies
make dev-install        # Install development dependencies
make test               # Run tests
make lint               # Run ruff linter
make format             # Format code with ruff
make type-check         # Run mypy type checking
make clean              # Clean build artifacts
make run-api            # Start analytics API service

# CI/CD commands
uv run pre-commit install       # Install pre-commit hooks
uv run pre-commit run --all-files  # Run pre-commit on all files
```

### GitHub Integration (Optional)

To enable GitHub CLI integration for repository management:

1. **Create a GitHub Personal Access Token:**
   - Go to GitHub → Settings → Developer settings → Personal access tokens
   - Generate a classic token with `repo` permissions

2. **Create `.env` file:**
   ```bash
   # ===== GitHub Configuration =====
   # GitHub Personal Access Token for API access
   GITHUB_TOKEN=your_token_here
   ```

3. **Authenticate:**
   ```bash
   source .env && gh auth login --with-token <<< "$GITHUB_TOKEN"
   ```

## 🐳 Docker Development

Start all services with Docker Compose:

```bash
cd docker
docker-compose up -d
```

Services will be available at:
- Analytics API: http://localhost:8000
- ML Inference: http://localhost:8001
- PostgreSQL: localhost:5432
- Redis: localhost:6379

## 🧪 Testing

Run the test suite:

```bash
make test
```

Run with coverage:

```bash
pytest --cov=libs --cov=services
```

## 📝 Code Style

This project follows strict code quality standards:

- **Formatting**: Ruff with 88 character line length
- **Linting**: Ruff with comprehensive rule set
- **Type Checking**: MyPy in strict mode
- **Testing**: Pytest with async support

## 🔧 Configuration

Each service uses Pydantic Settings for configuration management:

- Environment variables override defaults
- Configuration validation at startup
- Type-safe settings throughout the application

## 📚 Documentation

- [Developer Guide](CLAUDE.md) - Comprehensive development instructions
- [Architecture Decisions](docs/) - Design decisions and patterns
- [API Documentation](docs/api/) - Service-specific API docs

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Run `make format lint type-check test`
5. Create a pull request

## 📄 License

This project is licensed under the MIT License - see the LICENSE file for details.

## 🚀 CI/CD

This repository includes automated testing and quality checks:

- **GitHub Actions**: Automated CI/CD pipeline
- **Multi-Python Testing**: Tests on Python 3.10, 3.11, 3.12
- **Code Quality**: Ruff linting, formatting, and MyPy type checking
- **Test Coverage**: Pytest with coverage reporting
- **Security**: Safety vulnerability scanning
- **Docker**: Container build verification
- **Pre-commit Hooks**: Local development quality gates

[![CI](https://github.com/datablogin/analytics-backend-monorepo/workflows/CI/badge.svg)](https://github.com/datablogin/analytics-backend-monorepo/actions)

## 🎯 Roadmap

- [x] Add comprehensive CI/CD pipeline
- [x] Multi-Python version testing (3.10, 3.11, 3.12)
- [ ] Add monitoring and observability
- [ ] Create deployment guides
- [ ] Add example analytics workflows