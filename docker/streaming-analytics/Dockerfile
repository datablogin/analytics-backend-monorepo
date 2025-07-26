# Multi-stage production Dockerfile for Streaming Analytics
FROM python:3.11-slim as builder

# Install system dependencies for building
RUN apt-get update && apt-get install -y \
    build-essential \
    curl \
    git \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

# Install uv for fast dependency management
COPY --from=ghcr.io/astral-sh/uv:latest /uv /bin/uv

# Set working directory
WORKDIR /build

# Copy dependency files
COPY pyproject.toml uv.lock ./
COPY libs/ ./libs/
COPY services/ ./services/

# Install dependencies
RUN uv sync --frozen --no-dev

# Production stage
FROM python:3.11-slim as production

# Install runtime dependencies
RUN apt-get update && apt-get install -y \
    libpq5 \
    curl \
    && rm -rf /var/lib/apt/lists/* \
    && apt-get clean

# Create non-root user
RUN groupadd -r streaming && useradd -r -g streaming streaming \
    && mkdir -p /home/streaming \
    && chown -R streaming:streaming /home/streaming

# Set working directory
WORKDIR /home/streaming

# Copy virtual environment from builder
COPY --from=builder --chown=streaming:streaming /build/.venv .venv
ENV PATH="/home/streaming/.venv/bin:$PATH"

# Copy application code
COPY --chown=streaming:streaming libs/ ./libs/
COPY --chown=streaming:streaming services/ ./services/

# Create required directories
RUN mkdir -p /tmp /home/streaming/.cache /home/streaming/logs \
    && chown -R streaming:streaming /home/streaming \
    && chmod 755 /home/streaming

# Switch to non-root user
USER streaming

# Add health check script and entrypoint
COPY --chown=streaming:streaming docker/streaming-analytics/healthcheck.py ./healthcheck.py
COPY --chown=streaming:streaming docker/streaming-analytics/entrypoint.sh ./entrypoint.sh

# Make entrypoint executable
USER root
RUN chmod +x /home/streaming/entrypoint.sh
USER streaming

# Health check
HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
    CMD python healthcheck.py

# Expose ports
EXPOSE 8765 8080 8081

# Add labels
LABEL maintainer="streaming-analytics-team" \
      version="1.0.0" \
      description="Streaming Analytics Platform" \
      component="streaming-analytics"

# Use entrypoint script for flexible service configuration
ENTRYPOINT ["./entrypoint.sh"]