"""Celery application for async report generation."""

import os

from celery import Celery

from libs.config.base import Environment, get_environment

# Create Celery instance
celery_app = Celery("reporting_engine")

def init_celery():
    """Initialize Celery with configuration."""
    # Configuration based on environment
    env = get_environment()

    if env == Environment.DEVELOPMENT:
        broker_url = os.getenv("CELERY_BROKER_URL", "redis://localhost:6379/0")
        result_backend = os.getenv("CELERY_RESULT_BACKEND", "redis://localhost:6379/0")
    else:
        broker_url = os.getenv("CELERY_BROKER_URL", "redis://redis:6379/0")
        result_backend = os.getenv("CELERY_RESULT_BACKEND", "redis://redis:6379/0")

    celery_app.conf.update(
        broker_url=broker_url,
        result_backend=result_backend,
        task_serializer="json",
        accept_content=["json"],
        result_serializer="json",
        timezone="UTC",
        enable_utc=True,
        task_routes={
            "reporting_engine.tasks.generate_report": {"queue": "reports"},
            "reporting_engine.tasks.export_report": {"queue": "exports"},
        },
        task_annotations={
            "*": {"rate_limit": "10/m"},  # 10 tasks per minute
        },
        worker_prefetch_multiplier=4,
        task_acks_late=True,
        worker_max_tasks_per_child=1000,
    )

    # Import tasks to register them

    return celery_app


# Auto-discover tasks
celery_app.autodiscover_tasks(['services.reporting_engine.tasks'])
