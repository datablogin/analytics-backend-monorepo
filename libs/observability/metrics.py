"""Metrics collection and business intelligence tracking."""

import time
from datetime import datetime
from typing import Any

import structlog
from prometheus_client import Counter, Gauge, Histogram, Info

from .config import MetricsConfig

logger = structlog.get_logger(__name__)


class SystemMetrics:
    """System-level metrics tracking."""

    def __init__(self, prefix: str = "system"):
        self.prefix = prefix

        # HTTP metrics
        self.http_requests_total = Counter(
            f"{prefix}_http_requests_total",
            "Total HTTP requests",
            ["method", "endpoint", "status_code"],
        )

        self.http_request_duration = Histogram(
            f"{prefix}_http_request_duration_seconds",
            "HTTP request duration in seconds",
            ["method", "endpoint"],
        )

        # Database metrics
        self.db_connections_active = Gauge(
            f"{prefix}_db_connections_active", "Active database connections"
        )

        self.db_queries_total = Counter(
            f"{prefix}_db_queries_total",
            "Total database queries",
            ["operation", "table"],
        )

        self.db_query_duration = Histogram(
            f"{prefix}_db_query_duration_seconds",
            "Database query duration in seconds",
            ["operation", "table"],
        )

        # Application metrics
        self.app_errors_total = Counter(
            f"{prefix}_errors_total",
            "Total application errors",
            ["error_type", "component"],
        )

        self.app_uptime = Gauge(
            f"{prefix}_uptime_seconds", "Application uptime in seconds"
        )

        # Memory and CPU metrics
        self.memory_usage = Gauge(
            f"{prefix}_memory_usage_bytes", "Memory usage in bytes"
        )

        self.cpu_usage = Gauge(f"{prefix}_cpu_usage_percent", "CPU usage percentage")

        # Cache metrics
        self.cache_hits_total = Counter(
            f"{prefix}_cache_hits_total", "Total cache hits", ["cache_name"]
        )

        self.cache_misses_total = Counter(
            f"{prefix}_cache_misses_total", "Total cache misses", ["cache_name"]
        )


class BusinessMetrics:
    """Business-level metrics tracking."""

    def __init__(self, prefix: str = "business"):
        self.prefix = prefix

        # User metrics
        self.user_logins_total = Counter(
            f"{prefix}_user_logins_total", "Total user logins", ["status"]
        )

        self.active_users = Gauge(f"{prefix}_active_users", "Current active users")

        self.user_sessions_duration = Histogram(
            f"{prefix}_user_session_duration_seconds",
            "User session duration in seconds",
        )

        # Data processing metrics
        self.data_records_processed = Counter(
            f"{prefix}_data_records_processed_total",
            "Total data records processed",
            ["dataset", "status"],
        )

        self.data_quality_score = Gauge(
            f"{prefix}_data_quality_score", "Data quality score (0-100)", ["dataset"]
        )

        self.data_validation_failures = Counter(
            f"{prefix}_data_validation_failures_total",
            "Total data validation failures",
            ["dataset", "validation_type"],
        )

        # API usage metrics
        self.api_calls_total = Counter(
            f"{prefix}_api_calls_total",
            "Total API calls",
            ["endpoint", "user_type", "status"],
        )

        self.api_rate_limit_hits = Counter(
            f"{prefix}_api_rate_limit_hits_total",
            "Total API rate limit hits",
            ["endpoint", "user_id"],
        )

        # Revenue/business value metrics
        self.reports_generated = Counter(
            f"{prefix}_reports_generated_total",
            "Total reports generated",
            ["report_type", "user_type"],
        )

        self.export_operations = Counter(
            f"{prefix}_export_operations_total",
            "Total export operations",
            ["format", "size_category"],
        )


class MetricsCollector:
    """Central metrics collector and manager."""

    def __init__(self, config: MetricsConfig):
        self.config = config
        self.system = SystemMetrics()
        self.business = BusinessMetrics()
        self._start_time = time.time()

        # Application info
        self.app_info = Info("app_info", "Application information")

        logger.info("Metrics collector initialized")

    def set_app_info(self, **info: str) -> None:
        """Set application information metrics."""
        self.app_info.info(info)

    def record_http_request(
        self, method: str, endpoint: str, status_code: int, duration: float
    ) -> None:
        """Record HTTP request metrics."""
        self.system.http_requests_total.labels(
            method=method, endpoint=endpoint, status_code=str(status_code)
        ).inc()

        self.system.http_request_duration.labels(
            method=method, endpoint=endpoint
        ).observe(duration)

    def record_db_query(self, operation: str, table: str, duration: float) -> None:
        """Record database query metrics."""
        self.system.db_queries_total.labels(operation=operation, table=table).inc()

        self.system.db_query_duration.labels(operation=operation, table=table).observe(
            duration
        )

    def record_error(self, error_type: str, component: str) -> None:
        """Record application error metrics."""
        self.system.app_errors_total.labels(
            error_type=error_type, component=component
        ).inc()

    def update_uptime(self) -> None:
        """Update application uptime metric."""
        uptime = time.time() - self._start_time
        self.system.app_uptime.set(uptime)

    def record_user_login(self, status: str) -> None:
        """Record user login metrics."""
        self.business.user_logins_total.labels(status=status).inc()

    def update_active_users(self, count: int) -> None:
        """Update active users count."""
        self.business.active_users.set(count)

    def record_data_processing(
        self, dataset: str, records_count: int, status: str = "success"
    ) -> None:
        """Record data processing metrics."""
        self.business.data_records_processed.labels(dataset=dataset, status=status).inc(
            records_count
        )

    def update_data_quality_score(self, dataset: str, score: float) -> None:
        """Update data quality score for a dataset."""
        self.business.data_quality_score.labels(dataset=dataset).set(score)

    def record_validation_failure(self, dataset: str, validation_type: str) -> None:
        """Record data validation failure."""
        self.business.data_validation_failures.labels(
            dataset=dataset, validation_type=validation_type
        ).inc()

    def record_api_call(self, endpoint: str, user_type: str, status: str) -> None:
        """Record API call metrics."""
        self.business.api_calls_total.labels(
            endpoint=endpoint, user_type=user_type, status=status
        ).inc()

    def record_rate_limit_hit(self, endpoint: str, user_id: str) -> None:
        """Record API rate limit hit."""
        self.business.api_rate_limit_hits.labels(
            endpoint=endpoint, user_id=user_id
        ).inc()

    def record_report_generation(self, report_type: str, user_type: str) -> None:
        """Record report generation metrics."""
        self.business.reports_generated.labels(
            report_type=report_type, user_type=user_type
        ).inc()

    def record_export_operation(self, format: str, size_category: str) -> None:
        """Record export operation metrics."""
        self.business.export_operations.labels(
            format=format, size_category=size_category
        ).inc()

    def get_metrics_summary(self) -> dict[str, Any]:
        """Get a summary of current metrics."""
        return {
            "uptime_seconds": time.time() - self._start_time,
            "timestamp": datetime.utcnow().isoformat(),
            "collector_status": "active",
        }

    def shutdown(self) -> None:
        """Shutdown the metrics collector."""
        logger.info("Metrics collector shutdown")


def create_metrics_collector(config: MetricsConfig) -> MetricsCollector:
    """Create and configure a metrics collector."""
    collector = MetricsCollector(config)

    # Set basic app info
    collector.set_app_info(
        version="1.0.0", environment="development", collector_version="0.1.0"
    )

    return collector
