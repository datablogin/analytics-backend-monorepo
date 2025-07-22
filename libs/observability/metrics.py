"""Metrics collection and business intelligence tracking."""

import time
from datetime import datetime
from typing import Any

import structlog
from prometheus_client import CollectorRegistry, Counter, Gauge, Histogram, Info

from .config import MetricsConfig

logger = structlog.get_logger(__name__)


class MLOpsMetrics:
    """MLOps-specific metrics tracking."""

    def __init__(
        self, prefix: str = "mlops", registry: CollectorRegistry | None = None
    ):
        self.prefix = prefix

        # Model registry metrics
        self.model_registrations_total = Counter(
            f"{prefix}_model_registrations_total",
            "Total model registrations",
            ["model_name", "version", "stage"],
            registry=registry,
        )

        self.model_registration_duration = Histogram(
            f"{prefix}_model_registration_duration_seconds",
            "Model registration duration in seconds",
            ["model_name"],
            registry=registry,
        )

        self.model_registration_errors = Counter(
            f"{prefix}_model_registration_errors_total",
            "Model registration errors",
            ["model_name", "error_type"],
            registry=registry,
        )

        # Model serving metrics
        self.model_inferences_total = Counter(
            f"{prefix}_model_inferences_total",
            "Total model inferences",
            ["model_name", "version", "status"],
            registry=registry,
        )

        self.model_inference_duration = Histogram(
            f"{prefix}_model_inference_duration_seconds",
            "Model inference duration in seconds",
            ["model_name", "version"],
            registry=registry,
        )

        self.model_batch_size = Histogram(
            f"{prefix}_model_batch_size",
            "Model inference batch size",
            ["model_name", "version"],
            registry=registry,
        )

        self.models_loaded = Gauge(
            f"{prefix}_models_loaded",
            "Number of loaded models",
            ["model_name"],
            registry=registry,
        )

        self.model_cache_hits_total = Counter(
            f"{prefix}_model_cache_hits_total",
            "Model cache hits",
            ["model_name", "cache_type"],
            registry=registry,
        )

        # Experiment tracking metrics
        self.experiments_created_total = Counter(
            f"{prefix}_experiments_created_total",
            "Total experiments created",
            ["experiment_name", "created_by"],
            registry=registry,
        )

        self.experiment_runs_total = Counter(
            f"{prefix}_experiment_runs_total",
            "Total experiment runs",
            ["experiment_name", "run_type", "status"],
            registry=registry,
        )

        self.experiment_run_duration = Histogram(
            f"{prefix}_experiment_run_duration_seconds",
            "Experiment run duration in seconds",
            ["experiment_name"],
            registry=registry,
        )

        # Model monitoring metrics
        self.drift_detections_total = Counter(
            f"{prefix}_drift_detections_total",
            "Total drift detections",
            ["model_name", "feature_name", "drift_detected"],
            registry=registry,
        )

        self.drift_magnitude = Gauge(
            f"{prefix}_drift_magnitude",
            "Drift magnitude score",
            ["model_name", "feature_name"],
            registry=registry,
        )

        self.model_performance_score = Gauge(
            f"{prefix}_model_performance_score",
            "Model performance score",
            ["model_name", "metric_name"],
            registry=registry,
        )

        self.data_quality_score = Gauge(
            f"{prefix}_data_quality_score",
            "Data quality score",
            ["model_name"],
            registry=registry,
        )

        # Feature store metrics
        self.feature_reads_total = Counter(
            f"{prefix}_feature_reads_total",
            "Total feature reads",
            ["feature_name", "cache_hit"],
            registry=registry,
        )

        self.feature_writes_total = Counter(
            f"{prefix}_feature_writes_total",
            "Total feature writes",
            ["feature_name", "batch_size"],
            registry=registry,
        )

        self.feature_cache_size = Gauge(
            f"{prefix}_feature_cache_size",
            "Feature cache size",
            registry=registry,
        )

        self.features_total = Gauge(
            f"{prefix}_features_total",
            "Total number of features",
            ["feature_type"],
            registry=registry,
        )

    def record_model_registration(
        self, model_name: str, version: str, stage: str, duration_seconds: float
    ) -> None:
        """Record model registration metrics."""
        self.model_registrations_total.labels(
            model_name=model_name, version=version, stage=stage
        ).inc()
        self.model_registration_duration.labels(model_name=model_name).observe(
            duration_seconds
        )

    def record_model_registration_error(
        self, model_name: str, error_type: str, duration_seconds: float
    ) -> None:
        """Record model registration error."""
        self.model_registration_errors.labels(
            model_name=model_name, error_type=error_type
        ).inc()
        self.model_registration_duration.labels(model_name=model_name).observe(
            duration_seconds
        )

    def record_model_inference(
        self,
        model_name: str,
        model_version: str,
        inference_time_ms: float,
        total_time_ms: float,
        batch_size: int,
        success: bool,
        error_type: str | None = None,
    ) -> None:
        """Record model inference metrics."""
        status = "success" if success else "error"

        self.model_inferences_total.labels(
            model_name=model_name, version=model_version, status=status
        ).inc()

        if success:
            self.model_inference_duration.labels(
                model_name=model_name, version=model_version
            ).observe(total_time_ms / 1000.0)

            self.model_batch_size.labels(
                model_name=model_name, version=model_version
            ).observe(batch_size)

    def record_model_load(
        self,
        model_name: str,
        model_version: str,
        load_time_seconds: float,
        success: bool,
        error_type: str | None = None,
    ) -> None:
        """Record model loading metrics."""
        if success:
            self.models_loaded.labels(model_name=model_name).inc()

    def record_model_stage_transition(
        self, model_name: str, version: str, from_stage: str, to_stage: str
    ) -> None:
        """Record model stage transition."""
        # Custom metric for stage transitions could be added here
        logger.info(
            "Model stage transition recorded",
            model_name=model_name,
            version=version,
            from_stage=from_stage,
            to_stage=to_stage,
        )

    def record_experiment_creation(
        self, experiment_name: str, experiment_id: str, created_by: str | None
    ) -> None:
        """Record experiment creation."""
        self.experiments_created_total.labels(
            experiment_name=experiment_name, created_by=created_by or "unknown"
        ).inc()

    def record_experiment_run(
        self,
        experiment_name: str,
        run_type: str,
        status: str,
        duration_seconds: float | None = None,
    ) -> None:
        """Record experiment run."""
        self.experiment_runs_total.labels(
            experiment_name=experiment_name, run_type=run_type, status=status
        ).inc()

        if duration_seconds is not None:
            self.experiment_run_duration.labels(
                experiment_name=experiment_name
            ).observe(duration_seconds)

    def record_drift_detection(
        self,
        model_name: str,
        feature_name: str,
        drift_detected: bool,
        drift_magnitude: float,
        p_value: float,
    ) -> None:
        """Record drift detection results."""
        self.drift_detections_total.labels(
            model_name=model_name,
            feature_name=feature_name,
            drift_detected=str(drift_detected).lower(),
        ).inc()

        self.drift_magnitude.labels(
            model_name=model_name, feature_name=feature_name
        ).set(drift_magnitude)

    def record_performance_monitoring(
        self,
        model_name: str,
        metric_name: str,
        current_value: float,
        baseline_value: float,
        change_percentage: float,
        degraded: bool,
    ) -> None:
        """Record performance monitoring results."""
        self.model_performance_score.labels(
            model_name=model_name, metric_name=metric_name
        ).set(current_value)

    def record_data_quality_check(
        self,
        model_name: str,
        quality_score: float,
        missing_value_pct: float,
        outlier_pct: float,
        issues_count: int,
    ) -> None:
        """Record data quality check results."""
        self.data_quality_score.labels(model_name=model_name).set(quality_score)

    def record_feature_read(
        self,
        feature_count: int,
        entity_count: int,
        result_count: int,
        cache_hit: bool,
        success: bool,
        error_type: str | None = None,
    ) -> None:
        """Record feature read operation."""
        cache_hit_str = "hit" if cache_hit else "miss"
        self.feature_reads_total.labels(
            feature_name="batch", cache_hit=cache_hit_str
        ).inc(feature_count)

    def record_feature_write(
        self,
        feature_count: int,
        batch_count: int,
        success: bool,
        error_type: str | None = None,
    ) -> None:
        """Record feature write operation."""
        if success:
            avg_batch_size = feature_count / max(batch_count, 1)
            self.feature_writes_total.labels(
                feature_name="batch", batch_size=str(int(avg_batch_size))
            ).inc(feature_count)

    def record_feature_creation(
        self, feature_name: str, feature_type: str, owner: str | None
    ) -> None:
        """Record feature creation."""
        self.features_total.labels(feature_type=feature_type).inc()

    def update_feature_cache_size(self, cache_size: int) -> None:
        """Update feature cache size metric."""
        self.feature_cache_size.set(cache_size)


class SystemMetrics:
    """System-level metrics tracking."""

    def __init__(
        self, prefix: str = "system", registry: CollectorRegistry | None = None
    ):
        self.prefix = prefix

        # HTTP metrics
        self.http_requests_total = Counter(
            f"{prefix}_http_requests_total",
            "Total HTTP requests",
            ["method", "endpoint", "status_code"],
            registry=registry,
        )

        self.http_request_duration = Histogram(
            f"{prefix}_http_request_duration_seconds",
            "HTTP request duration in seconds",
            ["method", "endpoint"],
            registry=registry,
        )

        # Database metrics
        self.db_connections_active = Gauge(
            f"{prefix}_db_connections_active",
            "Active database connections",
            registry=registry,
        )

        self.db_queries_total = Counter(
            f"{prefix}_db_queries_total",
            "Total database queries",
            ["operation", "table"],
            registry=registry,
        )

        self.db_query_duration = Histogram(
            f"{prefix}_db_query_duration_seconds",
            "Database query duration in seconds",
            ["operation", "table"],
            registry=registry,
        )

        # Application metrics
        self.app_errors_total = Counter(
            f"{prefix}_errors_total",
            "Total application errors",
            ["error_type", "component"],
            registry=registry,
        )

        self.app_uptime = Gauge(
            f"{prefix}_uptime_seconds",
            "Application uptime in seconds",
            registry=registry,
        )

        # Memory and CPU metrics
        self.memory_usage = Gauge(
            f"{prefix}_memory_usage_bytes",
            "Memory usage in bytes",
            registry=registry,
        )

        self.cpu_usage = Gauge(
            f"{prefix}_cpu_usage_percent", "CPU usage percentage", registry=registry
        )

        # Cache metrics
        self.cache_hits_total = Counter(
            f"{prefix}_cache_hits_total",
            "Total cache hits",
            ["cache_name"],
            registry=registry,
        )

        self.cache_misses_total = Counter(
            f"{prefix}_cache_misses_total",
            "Total cache misses",
            ["cache_name"],
            registry=registry,
        )


class BusinessMetrics:
    """Business-level metrics tracking."""

    def __init__(
        self, prefix: str = "business", registry: CollectorRegistry | None = None
    ):
        self.prefix = prefix

        # User metrics
        self.user_logins_total = Counter(
            f"{prefix}_user_logins_total",
            "Total user logins",
            ["status"],
            registry=registry,
        )

        self.active_users = Gauge(
            f"{prefix}_active_users", "Current active users", registry=registry
        )

        self.user_sessions_duration = Histogram(
            f"{prefix}_user_session_duration_seconds",
            "User session duration in seconds",
            registry=registry,
        )

        # Data processing metrics
        self.data_records_processed = Counter(
            f"{prefix}_data_records_processed_total",
            "Total data records processed",
            ["dataset", "status"],
            registry=registry,
        )

        self.data_quality_score = Gauge(
            f"{prefix}_data_quality_score",
            "Data quality score (0-100)",
            ["dataset"],
            registry=registry,
        )

        self.data_validation_failures = Counter(
            f"{prefix}_data_validation_failures_total",
            "Total data validation failures",
            ["dataset", "validation_type"],
            registry=registry,
        )

        # API usage metrics
        self.api_calls_total = Counter(
            f"{prefix}_api_calls_total",
            "Total API calls",
            ["endpoint", "user_type", "status"],
            registry=registry,
        )

        self.api_rate_limit_hits = Counter(
            f"{prefix}_api_rate_limit_hits_total",
            "Total API rate limit hits",
            ["endpoint", "user_id"],
            registry=registry,
        )

        # Revenue/business value metrics
        self.reports_generated = Counter(
            f"{prefix}_reports_generated_total",
            "Total reports generated",
            ["report_type", "user_type"],
            registry=registry,
        )

        self.export_operations = Counter(
            f"{prefix}_export_operations_total",
            "Total export operations",
            ["format", "size_category"],
            registry=registry,
        )


class MetricsCollector:
    """Central metrics collector and manager."""

    def __init__(
        self, config: MetricsConfig, registry: CollectorRegistry | None = None
    ):
        self.config = config
        self.system = SystemMetrics(registry=registry)
        self.business = BusinessMetrics(registry=registry)
        self._start_time = time.time()

        # Application info
        self.app_info = Info("app_info", "Application information", registry=registry)

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


def create_metrics_collector(
    config: MetricsConfig, registry: CollectorRegistry | None = None
) -> MetricsCollector:
    """Create and configure a metrics collector."""
    collector = MetricsCollector(config, registry=registry)

    # Set basic app info
    collector.set_app_info(
        version="1.0.0", environment="development", collector_version="0.1.0"
    )

    return collector
