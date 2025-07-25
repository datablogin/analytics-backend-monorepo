"""Monitoring, alerting and auto-scaling for streaming analytics."""

import asyncio
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any

import structlog
from prometheus_client import CollectorRegistry, Counter, Gauge, Histogram

from .config import MonitoringConfig, get_streaming_config

logger = structlog.get_logger(__name__)


class AlertSeverity(str, Enum):
    """Alert severity levels."""

    INFO = "info"
    WARNING = "warning"
    CRITICAL = "critical"
    FATAL = "fatal"


class MetricType(str, Enum):
    """Types of streaming metrics."""

    COUNTER = "counter"
    GAUGE = "gauge"
    HISTOGRAM = "histogram"
    RATE = "rate"


@dataclass
class StreamingAlert:
    """Streaming analytics alert."""

    id: str
    title: str
    description: str
    severity: AlertSeverity
    metric_name: str
    threshold: float
    current_value: float
    condition: str  # e.g., "greater_than", "less_than"
    component: str
    triggered_at: datetime = field(default_factory=datetime.utcnow)
    resolved_at: datetime | None = None
    metadata: dict[str, Any] = field(default_factory=dict)

    @property
    def is_resolved(self) -> bool:
        """Check if alert is resolved."""
        return self.resolved_at is not None

    @property
    def duration_seconds(self) -> float:
        """Get alert duration in seconds."""
        end_time = self.resolved_at or datetime.utcnow()
        return (end_time - self.triggered_at).total_seconds()

    def resolve(self, resolved_at: datetime | None = None) -> None:
        """Mark alert as resolved."""
        self.resolved_at = resolved_at or datetime.utcnow()

    def to_dict(self) -> dict[str, Any]:
        """Convert alert to dictionary."""
        return {
            "id": self.id,
            "title": self.title,
            "description": self.description,
            "severity": self.severity.value,
            "metric_name": self.metric_name,
            "threshold": self.threshold,
            "current_value": self.current_value,
            "condition": self.condition,
            "component": self.component,
            "triggered_at": self.triggered_at.isoformat(),
            "resolved_at": self.resolved_at.isoformat() if self.resolved_at else None,
            "duration_seconds": self.duration_seconds,
            "metadata": self.metadata,
        }


class StreamingMetrics:
    """Prometheus metrics for streaming analytics."""

    def __init__(
        self,
        prefix: str = "streaming_analytics",
        registry: CollectorRegistry | None = None,
    ):
        self.prefix = prefix
        self.registry = registry

        # Event processing metrics
        self.events_processed_total = Counter(
            f"{prefix}_events_processed_total",
            "Total events processed",
            ["event_type", "source_service", "status"],
            registry=registry,
        )

        self.event_processing_duration = Histogram(
            f"{prefix}_event_processing_duration_seconds",
            "Event processing duration",
            ["event_type", "processor"],
            registry=registry,
        )

        self.events_per_second = Gauge(
            f"{prefix}_events_per_second",
            "Events processed per second",
            ["event_type"],
            registry=registry,
        )

        # Stream processing metrics
        self.windows_created_total = Counter(
            f"{prefix}_windows_created_total",
            "Total windows created",
            ["window_type", "key_field"],
            registry=registry,
        )

        self.windows_closed_total = Counter(
            f"{prefix}_windows_closed_total",
            "Total windows closed",
            ["window_type", "key_field"],
            registry=registry,
        )

        self.active_windows = Gauge(
            f"{prefix}_active_windows",
            "Number of active windows",
            ["window_type"],
            registry=registry,
        )

        self.window_event_count = Histogram(
            f"{prefix}_window_event_count",
            "Number of events per window",
            ["window_type"],
            registry=registry,
        )

        # Aggregation metrics
        self.aggregations_computed_total = Counter(
            f"{prefix}_aggregations_computed_total",
            "Total aggregations computed",
            ["aggregation_type", "field_name"],
            registry=registry,
        )

        self.aggregation_computation_duration = Histogram(
            f"{prefix}_aggregation_computation_duration_seconds",
            "Aggregation computation duration",
            ["aggregation_type"],
            registry=registry,
        )

        # Kafka metrics
        self.kafka_messages_sent_total = Counter(
            f"{prefix}_kafka_messages_sent_total",
            "Total Kafka messages sent",
            ["topic", "status"],
            registry=registry,
        )

        self.kafka_messages_received_total = Counter(
            f"{prefix}_kafka_messages_received_total",
            "Total Kafka messages received",
            ["topic", "consumer_group"],
            registry=registry,
        )

        self.kafka_producer_errors_total = Counter(
            f"{prefix}_kafka_producer_errors_total",
            "Total Kafka producer errors",
            ["topic", "error_type"],
            registry=registry,
        )

        self.kafka_consumer_lag = Gauge(
            f"{prefix}_kafka_consumer_lag",
            "Kafka consumer lag",
            ["topic", "partition", "consumer_group"],
            registry=registry,
        )

        # ML inference metrics
        self.ml_predictions_total = Counter(
            f"{prefix}_ml_predictions_total",
            "Total ML predictions",
            ["model_name", "status"],
            registry=registry,
        )

        self.ml_inference_duration = Histogram(
            f"{prefix}_ml_inference_duration_seconds",
            "ML inference duration",
            ["model_name"],
            registry=registry,
        )

        self.ml_batch_size = Histogram(
            f"{prefix}_ml_batch_size",
            "ML inference batch size",
            ["model_name"],
            registry=registry,
        )

        self.ml_model_cache_hits_total = Counter(
            f"{prefix}_ml_model_cache_hits_total",
            "ML model cache hits",
            ["model_name", "hit_type"],
            registry=registry,
        )

        # WebSocket metrics
        self.websocket_connections_active = Gauge(
            f"{prefix}_websocket_connections_active",
            "Active WebSocket connections",
            registry=registry,
        )

        self.websocket_messages_sent_total = Counter(
            f"{prefix}_websocket_messages_sent_total",
            "Total WebSocket messages sent",
            ["message_type"],
            registry=registry,
        )

        self.websocket_subscriptions_active = Gauge(
            f"{prefix}_websocket_subscriptions_active",
            "Active WebSocket subscriptions",
            ["subscription_type"],
            registry=registry,
        )

        # System resource metrics
        self.cpu_usage_percent = Gauge(
            f"{prefix}_cpu_usage_percent",
            "CPU usage percentage",
            ["component"],
            registry=registry,
        )

        self.memory_usage_bytes = Gauge(
            f"{prefix}_memory_usage_bytes",
            "Memory usage in bytes",
            ["component"],
            registry=registry,
        )

        self.disk_usage_bytes = Gauge(
            f"{prefix}_disk_usage_bytes",
            "Disk usage in bytes",
            ["mount_point"],
            registry=registry,
        )

        # Latency and throughput metrics
        self.end_to_end_latency = Histogram(
            f"{prefix}_end_to_end_latency_seconds",
            "End-to-end processing latency",
            ["pipeline"],
            registry=registry,
        )

        self.throughput_events_per_second = Gauge(
            f"{prefix}_throughput_events_per_second",
            "Throughput in events per second",
            ["component"],
            registry=registry,
        )

        # Error metrics
        self.errors_total = Counter(
            f"{prefix}_errors_total",
            "Total errors",
            ["component", "error_type"],
            registry=registry,
        )

        self.error_rate = Gauge(
            f"{prefix}_error_rate",
            "Error rate (errors per second)",
            ["component"],
            registry=registry,
        )


class AlertRule:
    """Rule for triggering alerts based on metrics."""

    def __init__(
        self,
        name: str,
        metric_name: str,
        condition: str,
        threshold: float,
        severity: AlertSeverity = AlertSeverity.WARNING,
        component: str = "streaming",
        description: str | None = None,
    ):
        self.name = name
        self.metric_name = metric_name
        self.condition = condition
        self.threshold = threshold
        self.severity = severity
        self.component = component
        self.description = description or f"{metric_name} {condition} {threshold}"

        # Alert state
        self.active_alert: StreamingAlert | None = None
        self.last_check: datetime | None = None
        self.check_count = 0

    def check_condition(self, current_value: float) -> bool:
        """Check if alert condition is met."""
        self.last_check = datetime.utcnow()
        self.check_count += 1

        if self.condition == "greater_than":
            return current_value > self.threshold
        elif self.condition == "less_than":
            return current_value < self.threshold
        elif self.condition == "equals":
            return abs(current_value - self.threshold) < 0.001
        elif self.condition == "not_equals":
            return abs(current_value - self.threshold) >= 0.001
        else:
            raise ValueError(f"Unknown condition: {self.condition}")

    def trigger_alert(self, current_value: float) -> StreamingAlert:
        """Trigger a new alert."""
        alert_id = f"{self.name}_{int(time.time())}"

        self.active_alert = StreamingAlert(
            id=alert_id,
            title=f"Alert: {self.name}",
            description=self.description,
            severity=self.severity,
            metric_name=self.metric_name,
            threshold=self.threshold,
            current_value=current_value,
            condition=self.condition,
            component=self.component,
        )

        return self.active_alert

    def resolve_alert(self) -> StreamingAlert | None:
        """Resolve the active alert."""
        if self.active_alert and not self.active_alert.is_resolved:
            self.active_alert.resolve()
            resolved_alert = self.active_alert
            self.active_alert = None
            return resolved_alert
        return None


class AlertManager:
    """Manages streaming analytics alerts."""

    def __init__(self, config: MonitoringConfig):
        self.config = config
        self.logger = logger.bind(component="alert_manager")

        # Alert rules and state
        self.rules: dict[str, AlertRule] = {}
        self.active_alerts: dict[str, StreamingAlert] = {}
        self.alert_history: list[StreamingAlert] = []

        # Alert handlers
        self.alert_handlers: list[Callable[[StreamingAlert], None]] = []

        # Statistics
        self.total_alerts_triggered = 0
        self.total_alerts_resolved = 0

    def add_rule(self, rule: AlertRule) -> None:
        """Add an alert rule."""
        self.rules[rule.name] = rule
        self.logger.info(
            "Alert rule added",
            rule_name=rule.name,
            metric_name=rule.metric_name,
            condition=rule.condition,
            threshold=rule.threshold,
        )

    async def remove_rule(self, rule_name: str) -> bool:
        """Remove an alert rule."""
        if rule_name in self.rules:
            rule = self.rules.pop(rule_name)

            # Resolve any active alert
            if rule.active_alert:
                resolved_alert = rule.resolve_alert()
                if resolved_alert:
                    await self._handle_alert_resolved(resolved_alert)

            self.logger.info("Alert rule removed", rule_name=rule_name)
            return True

        return False

    def add_alert_handler(self, handler: Callable[[StreamingAlert], None]) -> None:
        """Add handler for alert notifications."""
        self.alert_handlers.append(handler)
        self.logger.info("Alert handler added", handler_count=len(self.alert_handlers))

    async def check_metrics(self, metrics: dict[str, float]) -> list[StreamingAlert]:
        """Check metrics against alert rules."""
        triggered_alerts = []
        resolved_alerts = []

        for rule_name, rule in self.rules.items():
            try:
                current_value = metrics.get(rule.metric_name)
                if current_value is None:
                    continue

                condition_met = rule.check_condition(current_value)

                if condition_met and not rule.active_alert:
                    # Trigger new alert
                    alert = rule.trigger_alert(current_value)
                    self.active_alerts[alert.id] = alert
                    self.alert_history.append(alert)
                    triggered_alerts.append(alert)
                    self.total_alerts_triggered += 1

                    await self._handle_alert_triggered(alert)

                elif not condition_met and rule.active_alert:
                    # Resolve existing alert
                    resolved_alert = rule.resolve_alert()
                    if resolved_alert:
                        self.active_alerts.pop(resolved_alert.id, None)
                        resolved_alerts.append(resolved_alert)
                        self.total_alerts_resolved += 1

                        await self._handle_alert_resolved(resolved_alert)

                elif condition_met and rule.active_alert:
                    # Update existing alert with new value
                    rule.active_alert.current_value = current_value

            except Exception as e:
                self.logger.error(
                    "Error checking alert rule", rule_name=rule_name, error=str(e)
                )

        return triggered_alerts + resolved_alerts

    async def _handle_alert_triggered(self, alert: StreamingAlert) -> None:
        """Handle newly triggered alert."""
        self.logger.warning(
            "Alert triggered",
            alert_id=alert.id,
            title=alert.title,
            severity=alert.severity.value,
            metric_name=alert.metric_name,
            current_value=alert.current_value,
            threshold=alert.threshold,
        )

        # Call alert handlers
        for handler in self.alert_handlers:
            try:
                if asyncio.iscoroutinefunction(handler):
                    result_coro = handler(alert)
                    if result_coro is not None:
                        await result_coro
                else:
                    await asyncio.get_event_loop().run_in_executor(None, handler, alert)
            except Exception as e:
                self.logger.error(
                    "Error in alert handler",
                    alert_id=alert.id,
                    handler=str(handler),
                    error=str(e),
                )

    async def _handle_alert_resolved(self, alert: StreamingAlert) -> None:
        """Handle resolved alert."""
        self.logger.info(
            "Alert resolved",
            alert_id=alert.id,
            title=alert.title,
            duration_seconds=alert.duration_seconds,
        )

    def get_active_alerts(
        self, severity: AlertSeverity | None = None, component: str | None = None
    ) -> list[StreamingAlert]:
        """Get active alerts with optional filters."""
        alerts = list(self.active_alerts.values())

        if severity:
            alerts = [a for a in alerts if a.severity == severity]

        if component:
            alerts = [a for a in alerts if a.component == component]

        return alerts

    def get_alert_history(
        self, limit: int = 100, severity: AlertSeverity | None = None
    ) -> list[StreamingAlert]:
        """Get alert history with optional filters."""
        alerts = self.alert_history[-limit:] if limit else self.alert_history

        if severity:
            alerts = [a for a in alerts if a.severity == severity]

        return sorted(alerts, key=lambda a: a.triggered_at, reverse=True)

    def get_stats(self) -> dict[str, Any]:
        """Get alert manager statistics."""
        active_by_severity = {}
        for severity in AlertSeverity:
            count = len(
                [a for a in self.active_alerts.values() if a.severity == severity]
            )
            active_by_severity[severity.value] = count

        return {
            "total_rules": len(self.rules),
            "active_alerts": len(self.active_alerts),
            "total_alerts_triggered": self.total_alerts_triggered,
            "total_alerts_resolved": self.total_alerts_resolved,
            "active_by_severity": active_by_severity,
            "alert_handlers": len(self.alert_handlers),
        }


class AutoScaler:
    """Auto-scaling based on streaming metrics."""

    def __init__(self, config: MonitoringConfig):
        self.config = config
        self.logger = logger.bind(component="auto_scaler")

        # Scaling state
        self.current_instances = config.min_instances
        self.last_scale_action: datetime | None = None
        self.scale_actions: list[dict[str, Any]] = []

        # Scaling handlers
        self.scale_handlers: list[Callable[[str, int, int], None]] = []

    def add_scale_handler(self, handler: Callable[[str, int, int], None]) -> None:
        """Add handler for scaling actions."""
        self.scale_handlers.append(handler)
        self.logger.info("Scale handler added", handler_count=len(self.scale_handlers))

    async def check_scaling(self, metrics: dict[str, float]) -> dict[str, Any] | None:
        """Check if scaling is needed based on metrics."""
        if not self.config.enable_auto_scaling:
            return None

        current_time = datetime.utcnow()

        # Check cooldown period
        if (
            self.last_scale_action
            and (current_time - self.last_scale_action).total_seconds()
            < self.config.cooldown_period_seconds
        ):
            return None

        # Get comprehensive metrics for scaling decisions
        throughput = metrics.get("throughput_events_per_second", 0)
        cpu_usage = metrics.get("cpu_usage_percent", 0) / 100  # Convert to 0-1
        memory_usage = metrics.get("memory_usage_bytes", 0) / (
            8 * 1024**3
        )  # Normalize to 8GB
        error_rate = metrics.get("error_rate", 0)
        kafka_lag = metrics.get("kafka_consumer_lag", 0)
        queue_depth = metrics.get("queue_depth", 0)

        # Advanced utilization score with multiple factors
        base_utilization = max(cpu_usage, memory_usage)
        throughput_pressure = min(
            throughput / 10000, 1.0
        )  # Normalize to 10K events/sec
        lag_pressure = min(kafka_lag / 1000, 1.0)  # Normalize to 1K message lag
        queue_pressure = min(queue_depth / 100, 1.0)  # Normalize to 100 queued items

        # Weighted composite score
        utilization_score = (
            base_utilization * 0.4  # Resource usage (40%)
            + throughput_pressure * 0.3  # Throughput pressure (30%)
            + lag_pressure * 0.2  # Kafka lag (20%)
            + queue_pressure * 0.1  # Queue depth (10%)
        )

        # Calculate scaling velocity based on urgency
        urgency_multiplier = 1.0
        if error_rate > 0.05:  # 5% error rate
            urgency_multiplier = 2.0
        elif utilization_score > 0.9:  # Very high utilization
            urgency_multiplier = 1.5

        scale_action = None

        # Check scale up conditions with improved logic
        if (
            utilization_score > self.config.scale_up_threshold
            and self.current_instances < self.config.max_instances
        ):
            # Aggressive scaling for high urgency
            scale_amount = max(1, int(urgency_multiplier))
            new_instances = min(
                self.current_instances + scale_amount, self.config.max_instances
            )

            scale_action = {
                "action": "scale_up",
                "from_instances": self.current_instances,
                "to_instances": new_instances,
                "reason": f"High utilization: {utilization_score:.2f}",
                "metrics": {
                    "utilization_score": utilization_score,
                    "throughput": throughput,
                    "cpu_usage": cpu_usage,
                    "error_rate": error_rate,
                },
            }

        # Check scale down conditions
        elif (
            utilization_score < self.config.scale_down_threshold
            and self.current_instances > self.config.min_instances
            and error_rate < 0.01
        ):  # Only scale down if error rate is low
            new_instances = max(self.current_instances - 1, self.config.min_instances)

            scale_action = {
                "action": "scale_down",
                "from_instances": self.current_instances,
                "to_instances": new_instances,
                "reason": f"Low utilization: {utilization_score:.2f}",
                "metrics": {
                    "utilization_score": utilization_score,
                    "throughput": throughput,
                    "cpu_usage": cpu_usage,
                    "error_rate": error_rate,
                },
            }

        if scale_action:
            await self._execute_scaling_action(scale_action)
            return scale_action

        return None

    async def _execute_scaling_action(self, action: dict[str, Any]) -> None:
        """Execute a scaling action."""
        try:
            old_instances = self.current_instances
            new_instances = action["to_instances"]

            self.logger.info(
                "Executing scaling action",
                action=action["action"],
                from_instances=old_instances,
                to_instances=new_instances,
                reason=action["reason"],
            )

            # Update state
            self.current_instances = new_instances
            self.last_scale_action = datetime.utcnow()

            # Add to history
            action["timestamp"] = self.last_scale_action
            action["success"] = True
            self.scale_actions.append(action)

            # Call scale handlers
            for handler in self.scale_handlers:
                try:
                    if asyncio.iscoroutinefunction(handler):
                        result_coro = handler(action["action"], old_instances, new_instances)
                        if result_coro is not None:
                            await result_coro
                    else:
                        await asyncio.get_event_loop().run_in_executor(
                            None,
                            handler,
                            action["action"],
                            old_instances,
                            new_instances,
                        )
                except Exception as e:
                    self.logger.error(
                        "Error in scale handler", handler=str(handler), error=str(e)
                    )

        except Exception as e:
            self.logger.error(
                "Failed to execute scaling action",
                action=action["action"],
                error=str(e),
            )

            # Mark action as failed
            action["success"] = False
            action["error"] = str(e)
            self.scale_actions.append(action)

    def get_scaling_history(self, limit: int = 50) -> list[dict[str, Any]]:
        """Get scaling action history."""
        return self.scale_actions[-limit:] if limit else self.scale_actions

    def get_stats(self) -> dict[str, Any]:
        """Get auto-scaler statistics."""
        successful_actions = [a for a in self.scale_actions if a.get("success", False)]
        failed_actions = [a for a in self.scale_actions if not a.get("success", False)]

        return {
            "enabled": self.config.enable_auto_scaling,
            "current_instances": self.current_instances,
            "min_instances": self.config.min_instances,
            "max_instances": self.config.max_instances,
            "last_scale_action": self.last_scale_action.isoformat()
            if self.last_scale_action
            else None,
            "total_scale_actions": len(self.scale_actions),
            "successful_actions": len(successful_actions),
            "failed_actions": len(failed_actions),
            "scale_handlers": len(self.scale_handlers),
        }


class StreamingMonitor:
    """Main monitoring orchestrator for streaming analytics."""

    def __init__(self, config: MonitoringConfig | None = None):
        self.config = config or get_streaming_config().monitoring
        self.metrics = StreamingMetrics()
        self.alert_manager = AlertManager(self.config)
        self.auto_scaler = AutoScaler(self.config)
        self.logger = logger.bind(component="streaming_monitor")

        # Monitoring state
        self.is_running = False
        self.monitor_task: asyncio.Task | None = None
        self.metrics_cache: dict[str, float] = {}

        # Setup default alert rules
        self._setup_default_alert_rules()

    def _setup_default_alert_rules(self) -> None:
        """Setup default alert rules."""
        # High latency alert
        self.alert_manager.add_rule(
            AlertRule(
                name="high_latency",
                metric_name="end_to_end_latency_seconds",
                condition="greater_than",
                threshold=self.config.latency_critical_ms / 1000,
                severity=AlertSeverity.CRITICAL,
                description="End-to-end latency is too high",
            )
        )

        # High error rate alert
        self.alert_manager.add_rule(
            AlertRule(
                name="high_error_rate",
                metric_name="error_rate",
                condition="greater_than",
                threshold=self.config.error_rate_critical_threshold,
                severity=AlertSeverity.CRITICAL,
                description="Error rate is too high",
            )
        )

        # Low throughput warning
        self.alert_manager.add_rule(
            AlertRule(
                name="low_throughput",
                metric_name="throughput_events_per_second",
                condition="less_than",
                threshold=self.config.throughput_warning_threshold * 1000,
                severity=AlertSeverity.WARNING,
                description="Throughput is below expected level",
            )
        )

    async def start(self) -> None:
        """Start the monitoring system."""
        self.is_running = True

        # Start monitoring loop
        self.monitor_task = asyncio.create_task(self._monitoring_loop())

        self.logger.info(
            "Streaming monitor started",
            metrics_interval=self.config.metrics_interval_seconds,
            detailed_metrics=self.config.detailed_metrics,
        )

    async def stop(self) -> None:
        """Stop the monitoring system."""
        self.is_running = False

        if self.monitor_task:
            self.monitor_task.cancel()
            try:
                await self.monitor_task
            except asyncio.CancelledError:
                pass

        self.logger.info("Streaming monitor stopped")

    async def _monitoring_loop(self) -> None:
        """Main monitoring loop."""
        while self.is_running:
            try:
                # Collect current metrics
                current_metrics = await self._collect_metrics()

                # Update metrics cache
                self.metrics_cache.update(current_metrics)

                # Check alerts
                alert_changes = await self.alert_manager.check_metrics(current_metrics)

                # Check auto-scaling
                scale_action = await self.auto_scaler.check_scaling(current_metrics)

                if alert_changes:
                    self.logger.debug(
                        "Alert changes detected", count=len(alert_changes)
                    )

                if scale_action:
                    self.logger.info(
                        "Scaling action executed", action=scale_action["action"]
                    )

                # Sleep until next monitoring interval
                await asyncio.sleep(self.config.metrics_interval_seconds)

            except asyncio.CancelledError:
                break
            except Exception as e:
                self.logger.error("Error in monitoring loop", error=str(e))
                await asyncio.sleep(5)  # Backoff on error

    async def _collect_metrics(self) -> dict[str, float]:
        """Collect current metrics from all components."""
        metrics = {}

        try:
            # In a real implementation, you would collect metrics from:
            # - Prometheus metrics registry
            # - Kafka managers
            # - Stream processors
            # - ML inference engines
            # - WebSocket servers
            # - System resources

            # For now, we'll simulate some basic metrics
            current_time = time.time()

            # Simulate throughput metric
            metrics["throughput_events_per_second"] = 850.0  # Below warning threshold

            # Simulate latency metric
            metrics["end_to_end_latency_seconds"] = 0.05  # 50ms

            # Simulate error rate
            metrics["error_rate"] = 0.002  # 0.2%

            # Simulate CPU usage
            metrics["cpu_usage_percent"] = 65.0

            # Simulate memory usage
            metrics["memory_usage_bytes"] = 2_000_000_000  # 2GB

            # Add timestamp
            metrics["collection_timestamp"] = current_time

        except Exception as e:
            self.logger.error("Error collecting metrics", error=str(e))

        return metrics

    def get_current_metrics(self) -> dict[str, float]:
        """Get current cached metrics."""
        return self.metrics_cache.copy()

    def get_active_alerts(self) -> list[StreamingAlert]:
        """Get active alerts."""
        return self.alert_manager.get_active_alerts()

    def get_alert_history(self, limit: int = 100) -> list[StreamingAlert]:
        """Get alert history."""
        return self.alert_manager.get_alert_history(limit)

    def get_scaling_history(self, limit: int = 50) -> list[dict[str, Any]]:
        """Get scaling history."""
        return self.auto_scaler.get_scaling_history(limit)

    def get_comprehensive_stats(self) -> dict[str, Any]:
        """Get comprehensive monitoring statistics."""
        return {
            "is_running": self.is_running,
            "config": {
                "metrics_interval_seconds": self.config.metrics_interval_seconds,
                "detailed_metrics": self.config.detailed_metrics,
                "enable_auto_scaling": self.config.enable_auto_scaling,
            },
            "current_metrics": self.get_current_metrics(),
            "alerts": self.alert_manager.get_stats(),
            "auto_scaling": self.auto_scaler.get_stats(),
            "thresholds": {
                "latency_warning_ms": self.config.latency_warning_ms,
                "latency_critical_ms": self.config.latency_critical_ms,
                "error_rate_warning": self.config.error_rate_warning_threshold,
                "error_rate_critical": self.config.error_rate_critical_threshold,
                "throughput_warning": self.config.throughput_warning_threshold,
            },
        }


# Global monitor instance
_streaming_monitor: StreamingMonitor | None = None


async def get_streaming_monitor() -> StreamingMonitor:
    """Get the global streaming monitor instance."""
    global _streaming_monitor

    if _streaming_monitor is None:
        _streaming_monitor = StreamingMonitor()
        await _streaming_monitor.start()

    return _streaming_monitor


async def shutdown_streaming_monitor() -> None:
    """Shutdown the global streaming monitor."""
    global _streaming_monitor

    if _streaming_monitor:
        await _streaming_monitor.stop()
        _streaming_monitor = None
