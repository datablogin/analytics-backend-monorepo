"""Workflow monitoring and metrics collection."""

from datetime import UTC, datetime, timedelta  # type: ignore[attr-defined]
from typing import Any

import structlog
from pydantic import BaseModel, Field

logger = structlog.get_logger(__name__)


class ExecutionMetrics(BaseModel):
    """Metrics for workflow execution."""

    # Execution counts
    total_executions: int = Field(default=0, description="Total workflow executions")
    successful_executions: int = Field(default=0, description="Successful executions")
    failed_executions: int = Field(default=0, description="Failed executions")
    cancelled_executions: int = Field(default=0, description="Cancelled executions")

    # Success rates
    success_rate: float = Field(default=0.0, description="Success rate percentage")

    # Timing metrics
    avg_duration_seconds: float = Field(
        default=0.0, description="Average execution duration"
    )
    min_duration_seconds: float = Field(
        default=0.0, description="Minimum execution duration"
    )
    max_duration_seconds: float = Field(
        default=0.0, description="Maximum execution duration"
    )

    # Task metrics
    total_tasks_executed: int = Field(default=0, description="Total tasks executed")
    avg_tasks_per_workflow: float = Field(
        default=0.0, description="Average tasks per workflow"
    )
    task_success_rate: float = Field(
        default=0.0, description="Task success rate percentage"
    )

    # Resource metrics
    avg_cpu_utilization: float = Field(
        default=0.0, description="Average CPU utilization"
    )
    avg_memory_utilization: float = Field(
        default=0.0, description="Average memory utilization"
    )
    peak_concurrent_executions: int = Field(
        default=0, description="Peak concurrent executions"
    )

    # Error analysis
    common_errors: list[dict[str, Any]] = Field(
        default_factory=list, description="Most common errors"
    )
    error_rate_by_task: dict[str, float] = Field(
        default_factory=dict, description="Error rates by task type"
    )

    # Performance trends
    hourly_execution_counts: list[int] = Field(
        default_factory=list, description="Executions per hour (24h)"
    )
    daily_success_rates: list[float] = Field(
        default_factory=list, description="Success rates per day (7d)"
    )

    # Metadata
    metrics_period_start: datetime = Field(description="Start of metrics period")
    metrics_period_end: datetime = Field(description="End of metrics period")
    last_updated: datetime = Field(default_factory=lambda: datetime.now(UTC))


class WorkflowHealthCheck(BaseModel):
    """Health check status for a workflow."""

    workflow_name: str = Field(description="Workflow name")
    workflow_version: str = Field(description="Workflow version")

    # Overall health
    health_status: str = Field(
        description="Overall health: healthy, degraded, unhealthy"
    )
    health_score: float = Field(ge=0.0, le=100.0, description="Health score 0-100")

    # Recent performance
    recent_success_rate: float = Field(
        ge=0.0, le=100.0, description="Success rate last 24h"
    )
    recent_avg_duration: float = Field(description="Average duration last 24h")
    recent_execution_count: int = Field(description="Executions last 24h")

    # Issues
    active_alerts: list[str] = Field(default_factory=list, description="Active alerts")
    recurring_failures: list[str] = Field(
        default_factory=list, description="Recurring failure patterns"
    )
    performance_issues: list[str] = Field(
        default_factory=list, description="Performance issues"
    )

    # Recommendations
    recommendations: list[str] = Field(
        default_factory=list, description="Improvement recommendations"
    )

    # Check metadata
    last_check: datetime = Field(default_factory=lambda: datetime.now(UTC))
    next_check: datetime | None = Field(
        default=None, description="Next scheduled check"
    )


class AlertRule(BaseModel):
    """Configuration for monitoring alerts."""

    name: str = Field(description="Alert rule name")
    workflow_name: str | None = Field(
        default=None, description="Target workflow (None = all)"
    )

    # Conditions
    success_rate_threshold: float | None = Field(
        default=None, description="Min success rate %"
    )
    duration_threshold_seconds: float | None = Field(
        default=None, description="Max duration seconds"
    )
    error_count_threshold: int | None = Field(
        default=None, description="Max errors in period"
    )
    failure_streak_threshold: int | None = Field(
        default=None, description="Max consecutive failures"
    )

    # Evaluation period
    evaluation_period_minutes: int = Field(
        default=60, description="Evaluation period in minutes"
    )

    # Alert settings
    severity: str = Field(
        default="warning", description="Alert severity: info, warning, error, critical"
    )
    enabled: bool = Field(default=True, description="Whether alert is enabled")
    cooldown_minutes: int = Field(default=30, description="Cooldown between alerts")

    # Notification channels
    notification_channels: list[str] = Field(
        default_factory=list, description="Notification channels"
    )

    # Metadata
    created_at: datetime = Field(default_factory=lambda: datetime.now(UTC))
    last_triggered: datetime | None = Field(
        default=None, description="Last time alert triggered"
    )
    trigger_count: int = Field(default=0, description="Number of times triggered")


class AlertInstance(BaseModel):
    """Instance of a triggered alert."""

    alert_id: str = Field(description="Unique alert instance ID")
    rule_name: str = Field(description="Alert rule name")
    workflow_name: str | None = Field(default=None, description="Affected workflow")

    # Alert details
    severity: str = Field(description="Alert severity")
    message: str = Field(description="Alert message")
    description: str = Field(description="Detailed description")

    # Triggering conditions
    triggered_conditions: dict[str, Any] = Field(
        description="Conditions that triggered alert"
    )
    metric_values: dict[str, float] = Field(description="Metric values at trigger time")

    # Timing
    triggered_at: datetime = Field(default_factory=lambda: datetime.now(UTC))
    acknowledged_at: datetime | None = Field(
        default=None, description="When alert was acknowledged"
    )
    resolved_at: datetime | None = Field(
        default=None, description="When alert was resolved"
    )

    # Status
    status: str = Field(
        default="active", description="Alert status: active, acknowledged, resolved"
    )
    acknowledged_by: str | None = Field(
        default=None, description="User who acknowledged"
    )
    resolution_notes: str | None = Field(
        default=None, description="Resolution notes"
    )


class WorkflowMonitor:
    """Monitors workflow execution and generates metrics."""

    def __init__(self, workflow_state):
        self.workflow_state = workflow_state
        self.alert_rules: dict[str, AlertRule] = {}
        self.active_alerts: dict[str, AlertInstance] = {}
        self.metrics_cache: dict[str, ExecutionMetrics] = {}
        self.health_checks: dict[str, WorkflowHealthCheck] = {}

        logger.info("Workflow monitor initialized")

    def add_alert_rule(self, rule: AlertRule) -> None:
        """Add monitoring alert rule."""
        self.alert_rules[rule.name] = rule
        logger.info(
            "Alert rule added", rule_name=rule.name, workflow_name=rule.workflow_name
        )

    def remove_alert_rule(self, rule_name: str) -> bool:
        """Remove alert rule."""
        if rule_name in self.alert_rules:
            del self.alert_rules[rule_name]
            logger.info("Alert rule removed", rule_name=rule_name)
            return True
        return False

    def get_workflow_metrics(
        self, workflow_name: str, period_hours: int = 24, force_refresh: bool = False
    ) -> ExecutionMetrics:
        """Get comprehensive metrics for a workflow."""
        cache_key = f"{workflow_name}:{period_hours}"

        # Check cache
        if not force_refresh and cache_key in self.metrics_cache:
            cached_metrics = self.metrics_cache[cache_key]
            # Use cache if less than 5 minutes old
            if (
                datetime.now(UTC) - cached_metrics.last_updated
            ).total_seconds() < 300:
                return cached_metrics

        # Calculate metrics
        end_time = datetime.now(UTC)
        start_time = end_time - timedelta(hours=period_hours)

        # Get executions for the period
        executions = self.workflow_state.list_executions(
            workflow_name=workflow_name,
            limit=1000,  # Adjust based on expected volume
        )

        # Filter by time period
        period_executions = [
            exec_summary
            for exec_summary in executions
            if exec_summary.get("start_time")
            and datetime.fromisoformat(
                exec_summary["start_time"].replace("Z", "+00:00")
            )
            >= start_time
        ]

        metrics = self._calculate_metrics(period_executions, start_time, end_time)

        # Cache the result
        self.metrics_cache[cache_key] = metrics

        return metrics

    def _calculate_metrics(
        self, executions: list[dict[str, Any]], start_time: datetime, end_time: datetime
    ) -> ExecutionMetrics:
        """Calculate execution metrics from execution data."""
        total_executions = len(executions)

        if total_executions == 0:
            return ExecutionMetrics(
                metrics_period_start=start_time, metrics_period_end=end_time
            )

        # Count by status
        successful_executions = sum(
            1 for e in executions if e.get("status") == "success"
        )
        failed_executions = sum(1 for e in executions if e.get("status") == "failed")
        cancelled_executions = sum(
            1 for e in executions if e.get("status") == "cancelled"
        )

        # Calculate success rate
        success_rate = (
            (successful_executions / total_executions) * 100
            if total_executions > 0
            else 0
        )

        # Duration metrics
        durations = [
            e.get("duration_seconds", 0)
            for e in executions
            if e.get("duration_seconds")
        ]
        avg_duration = sum(durations) / len(durations) if durations else 0
        min_duration = min(durations) if durations else 0
        max_duration = max(durations) if durations else 0

        # Task metrics
        total_tasks = sum(e.get("total_tasks", 0) for e in executions)
        avg_tasks_per_workflow = (
            total_tasks / total_executions if total_executions > 0 else 0
        )

        # Calculate task success rate
        completed_tasks = sum(e.get("completed_tasks", 0) for e in executions)
        failed_tasks = sum(e.get("failed_tasks", 0) for e in executions)
        task_success_rate = (
            (completed_tasks / (completed_tasks + failed_tasks)) * 100
            if (completed_tasks + failed_tasks) > 0
            else 0
        )

        # Error analysis
        common_errors = self._analyze_common_errors(executions)

        # Generate hourly execution counts (last 24 hours)
        hourly_counts = self._calculate_hourly_counts(executions, end_time)

        # Generate daily success rates (last 7 days)
        daily_success_rates = self._calculate_daily_success_rates(executions, end_time)

        return ExecutionMetrics(
            total_executions=total_executions,
            successful_executions=successful_executions,
            failed_executions=failed_executions,
            cancelled_executions=cancelled_executions,
            success_rate=success_rate,
            avg_duration_seconds=avg_duration,
            min_duration_seconds=min_duration,
            max_duration_seconds=max_duration,
            total_tasks_executed=total_tasks,
            avg_tasks_per_workflow=avg_tasks_per_workflow,
            task_success_rate=task_success_rate,
            common_errors=common_errors,
            hourly_execution_counts=hourly_counts,
            daily_success_rates=daily_success_rates,
            metrics_period_start=start_time,
            metrics_period_end=end_time,
        )

    def _analyze_common_errors(
        self, executions: list[dict[str, Any]]
    ) -> list[dict[str, Any]]:
        """Analyze and return most common errors."""
        error_counts: dict[str, int] = {}

        for execution in executions:
            if execution.get("status") == "failed" and execution.get("error_message"):
                error_msg = execution["error_message"]
                # Simplify error message for grouping
                simplified_error = (
                    error_msg.split("\n")[0][:100] if error_msg else "Unknown error"
                )
                error_counts[simplified_error] = (
                    error_counts.get(simplified_error, 0) + 1
                )

        # Sort by count and return top 5
        sorted_errors = sorted(error_counts.items(), key=lambda x: x[1], reverse=True)[
            :5
        ]
        return [{"error": error, "count": count} for error, count in sorted_errors]

    def _calculate_hourly_counts(
        self, executions: list[dict[str, Any]], end_time: datetime
    ) -> list[int]:
        """Calculate executions per hour for the last 24 hours."""
        hourly_counts = [0] * 24

        for execution in executions:
            if execution.get("start_time"):
                exec_time = datetime.fromisoformat(
                    execution["start_time"].replace("Z", "+00:00")
                )
                hours_ago = int((end_time - exec_time).total_seconds() // 3600)
                if 0 <= hours_ago < 24:
                    hourly_counts[23 - hours_ago] += 1

        return hourly_counts

    def _calculate_daily_success_rates(
        self, executions: list[dict[str, Any]], end_time: datetime
    ) -> list[float]:
        """Calculate success rates per day for the last 7 days."""
        daily_success_rates = []

        for day_offset in range(7):
            day_start = (end_time - timedelta(days=day_offset + 1)).replace(
                hour=0, minute=0, second=0, microsecond=0
            )
            day_end = day_start + timedelta(days=1)

            day_executions = [
                e
                for e in executions
                if e.get("start_time")
                and day_start
                <= datetime.fromisoformat(e["start_time"].replace("Z", "+00:00"))
                < day_end
            ]

            if day_executions:
                successful = sum(
                    1 for e in day_executions if e.get("status") == "success"
                )
                success_rate = (successful / len(day_executions)) * 100
                daily_success_rates.append(success_rate)
            else:
                daily_success_rates.append(0.0)

        return list(reversed(daily_success_rates))  # Return in chronological order

    def perform_health_check(
        self, workflow_name: str, workflow_version: str
    ) -> WorkflowHealthCheck:
        """Perform comprehensive health check for a workflow."""
        # Get recent metrics (last 24 hours)
        metrics = self.get_workflow_metrics(workflow_name, period_hours=24)

        # Calculate health score based on multiple factors
        health_score = self._calculate_health_score(metrics)

        # Determine health status
        if health_score >= 90:
            health_status = "healthy"
        elif health_score >= 70:
            health_status = "degraded"
        else:
            health_status = "unhealthy"

        # Identify issues and recommendations
        active_alerts = self._get_active_alerts_for_workflow(workflow_name)
        recurring_failures = self._identify_recurring_failures(workflow_name)
        performance_issues = self._identify_performance_issues(metrics)
        recommendations = self._generate_recommendations(
            metrics, recurring_failures, performance_issues
        )

        health_check = WorkflowHealthCheck(
            workflow_name=workflow_name,
            workflow_version=workflow_version,
            health_status=health_status,
            health_score=health_score,
            recent_success_rate=metrics.success_rate,
            recent_avg_duration=metrics.avg_duration_seconds,
            recent_execution_count=metrics.total_executions,
            active_alerts=active_alerts,
            recurring_failures=recurring_failures,
            performance_issues=performance_issues,
            recommendations=recommendations,
            next_check=datetime.now(UTC) + timedelta(minutes=30),
        )

        # Cache the health check
        self.health_checks[f"{workflow_name}:{workflow_version}"] = health_check

        return health_check

    def _calculate_health_score(self, metrics: ExecutionMetrics) -> float:
        """Calculate overall health score from metrics."""
        score = 100.0

        # Success rate impact (40% of score)
        if metrics.success_rate < 95:
            score -= (95 - metrics.success_rate) * 0.4

        # Error rate impact (20% of score)
        if metrics.total_executions > 0:
            error_rate = (metrics.failed_executions / metrics.total_executions) * 100
            if error_rate > 5:
                score -= (error_rate - 5) * 0.2

        # Performance impact (20% of score)
        if metrics.avg_duration_seconds > 3600:  # 1 hour baseline
            score -= min(20, (metrics.avg_duration_seconds - 3600) / 3600 * 20)

        # Execution frequency impact (10% of score)
        if metrics.total_executions == 0:
            score -= 10  # No recent executions is concerning

        # Task success rate impact (10% of score)
        if metrics.task_success_rate < 95:
            score -= (95 - metrics.task_success_rate) * 0.1

        return max(0.0, min(100.0, score))

    def _get_active_alerts_for_workflow(self, workflow_name: str) -> list[str]:
        """Get active alerts for a workflow."""
        active_alerts = []
        for alert in self.active_alerts.values():
            if (
                alert.workflow_name == workflow_name or alert.workflow_name is None
            ) and alert.status == "active":
                active_alerts.append(f"{alert.severity}: {alert.message}")
        return active_alerts

    def _identify_recurring_failures(self, workflow_name: str) -> list[str]:
        """Identify recurring failure patterns."""
        # Get recent executions
        executions = self.workflow_state.list_executions(
            workflow_name=workflow_name, limit=50
        )

        recurring_failures = []
        error_patterns: dict[str, int] = {}

        # Look for error patterns
        for execution in executions:
            if execution.get("status") == "failed" and execution.get("error_message"):
                # Extract key error patterns
                error_msg = execution["error_message"]
                # Simple pattern extraction - could be more sophisticated
                key_phrases = [
                    phrase for phrase in error_msg.split() if len(phrase) > 5
                ][:3]
                pattern = " ".join(key_phrases)

                error_patterns[pattern] = error_patterns.get(pattern, 0) + 1

        # Identify patterns that occur multiple times
        for pattern, count in error_patterns.items():
            if count >= 3:  # Appears 3+ times
                recurring_failures.append(f"Recurring error: {pattern} ({count} times)")

        return recurring_failures

    def _identify_performance_issues(self, metrics: ExecutionMetrics) -> list[str]:
        """Identify performance issues from metrics."""
        issues = []

        # Long average duration
        if metrics.avg_duration_seconds > 3600:
            issues.append(f"High average duration: {metrics.avg_duration_seconds:.1f}s")

        # High variation in duration
        if metrics.max_duration_seconds > metrics.avg_duration_seconds * 3:
            issues.append("High duration variability detected")

        # Low task success rate
        if metrics.task_success_rate < 90:
            issues.append(f"Low task success rate: {metrics.task_success_rate:.1f}%")

        # Declining success rate trend
        if len(metrics.daily_success_rates) >= 3:
            recent_rates = metrics.daily_success_rates[-3:]
            if all(
                recent_rates[i] > recent_rates[i + 1]
                for i in range(len(recent_rates) - 1)
            ):
                issues.append("Declining success rate trend")

        return issues

    def _generate_recommendations(
        self,
        metrics: ExecutionMetrics,
        recurring_failures: list[str],
        performance_issues: list[str],
    ) -> list[str]:
        """Generate improvement recommendations."""
        recommendations = []

        # Success rate recommendations
        if metrics.success_rate < 95:
            recommendations.append("Review and fix recurring error patterns")

        # Performance recommendations
        if metrics.avg_duration_seconds > 3600:
            recommendations.append(
                "Consider optimizing long-running tasks or adding parallelization"
            )

        # Reliability recommendations
        if recurring_failures:
            recommendations.append("Implement better error handling and retry policies")

        # Resource recommendations
        if metrics.avg_tasks_per_workflow > 20:
            recommendations.append(
                "Consider breaking down complex workflows into smaller ones"
            )

        # Monitoring recommendations
        if not self._has_adequate_monitoring(metrics.total_executions):
            recommendations.append(
                "Increase monitoring frequency and add more detailed alerts"
            )

        return recommendations

    def _has_adequate_monitoring(self, execution_count: int) -> bool:
        """Check if workflow has adequate monitoring coverage."""
        # Simple heuristic - workflows with more executions need more monitoring
        expected_alerts = max(1, execution_count // 100)  # 1 alert per 100 executions
        actual_alerts = len([r for r in self.alert_rules.values() if r.enabled])
        return actual_alerts >= expected_alerts

    def check_alert_conditions(self) -> list[AlertInstance]:
        """Check all alert rules and trigger alerts if conditions are met."""
        triggered_alerts = []
        current_time = datetime.now(UTC)

        for rule in self.alert_rules.values():
            if not rule.enabled:
                continue

            # Check cooldown period
            if rule.last_triggered:
                time_since_last = (
                    current_time - rule.last_triggered
                ).total_seconds() / 60
                if time_since_last < rule.cooldown_minutes:
                    continue

            # Evaluate alert conditions
            alert = self._evaluate_alert_rule(rule, current_time)
            if alert:
                triggered_alerts.append(alert)
                self.active_alerts[alert.alert_id] = alert

                # Update rule trigger info
                rule.last_triggered = current_time
                rule.trigger_count += 1

        return triggered_alerts

    def _evaluate_alert_rule(
        self, rule: AlertRule, current_time: datetime
    ) -> AlertInstance | None:
        """Evaluate a single alert rule."""
        # Get metrics for evaluation period
        period_start = current_time - timedelta(minutes=rule.evaluation_period_minutes)

        # Get executions for the period
        if rule.workflow_name:
            executions = self.workflow_state.list_executions(
                workflow_name=rule.workflow_name, limit=1000
            )
        else:
            executions = self.workflow_state.list_executions(limit=1000)

        # Filter by evaluation period
        period_executions = [
            e
            for e in executions
            if e.get("start_time")
            and datetime.fromisoformat(e["start_time"].replace("Z", "+00:00"))
            >= period_start
        ]

        if not period_executions:
            return None  # No executions to evaluate

        # Check conditions
        triggered_conditions = {}
        metric_values = {}

        # Success rate check
        if rule.success_rate_threshold is not None:
            successful = sum(
                1 for e in period_executions if e.get("status") == "success"
            )
            success_rate = (successful / len(period_executions)) * 100
            metric_values["success_rate"] = success_rate

            if success_rate < rule.success_rate_threshold:
                triggered_conditions["success_rate"] = (
                    f"Success rate {success_rate:.1f}% below threshold {rule.success_rate_threshold}%"
                )

        # Duration check
        if rule.duration_threshold_seconds is not None:
            durations = [
                e.get("duration_seconds", 0)
                for e in period_executions
                if e.get("duration_seconds")
            ]
            if durations:
                avg_duration = sum(durations) / len(durations)
                metric_values["avg_duration"] = avg_duration

                if avg_duration > rule.duration_threshold_seconds:
                    triggered_conditions["duration"] = (
                        f"Average duration {avg_duration:.1f}s exceeds threshold {rule.duration_threshold_seconds}s"
                    )

        # Error count check
        if rule.error_count_threshold is not None:
            error_count = sum(
                1 for e in period_executions if e.get("status") == "failed"
            )
            metric_values["error_count"] = error_count

            if error_count > rule.error_count_threshold:
                triggered_conditions["error_count"] = (
                    f"Error count {error_count} exceeds threshold {rule.error_count_threshold}"
                )

        # Failure streak check
        if rule.failure_streak_threshold is not None:
            # Check recent executions for consecutive failures
            recent_executions = sorted(
                period_executions, key=lambda x: x.get("start_time", ""), reverse=True
            )[: rule.failure_streak_threshold]

            consecutive_failures = 0
            for execution in recent_executions:
                if execution.get("status") == "failed":
                    consecutive_failures += 1
                else:
                    break

            metric_values["failure_streak"] = consecutive_failures

            if consecutive_failures >= rule.failure_streak_threshold:
                triggered_conditions["failure_streak"] = (
                    f"Consecutive failures {consecutive_failures} meets threshold {rule.failure_streak_threshold}"
                )

        # If any conditions triggered, create alert
        if triggered_conditions:
            alert_id = f"{rule.name}:{current_time.isoformat()}"

            message = f"Alert '{rule.name}' triggered"
            if rule.workflow_name:
                message += f" for workflow '{rule.workflow_name}'"

            description = "; ".join(triggered_conditions.values())

            return AlertInstance(
                alert_id=alert_id,
                rule_name=rule.name,
                workflow_name=rule.workflow_name,
                severity=rule.severity,
                message=message,
                description=description,
                triggered_conditions=triggered_conditions,
                metric_values=metric_values,
            )

        return None

    def acknowledge_alert(self, alert_id: str, acknowledged_by: str) -> bool:
        """Acknowledge an active alert."""
        if alert_id in self.active_alerts:
            alert = self.active_alerts[alert_id]
            alert.status = "acknowledged"
            alert.acknowledged_at = datetime.now(UTC)
            alert.acknowledged_by = acknowledged_by

            logger.info(
                "Alert acknowledged", alert_id=alert_id, acknowledged_by=acknowledged_by
            )
            return True
        return False

    def resolve_alert(
        self, alert_id: str, resolution_notes: str | None = None
    ) -> bool:
        """Resolve an active alert."""
        if alert_id in self.active_alerts:
            alert = self.active_alerts[alert_id]
            alert.status = "resolved"
            alert.resolved_at = datetime.now(UTC)
            alert.resolution_notes = resolution_notes

            logger.info(
                "Alert resolved", alert_id=alert_id, resolution_notes=resolution_notes
            )
            return True
        return False

    def get_active_alerts(
        self, workflow_name: str | None = None
    ) -> list[AlertInstance]:
        """Get all active alerts, optionally filtered by workflow."""
        alerts = [
            alert for alert in self.active_alerts.values() if alert.status == "active"
        ]

        if workflow_name:
            alerts = [alert for alert in alerts if alert.workflow_name == workflow_name]

        return sorted(alerts, key=lambda x: x.triggered_at, reverse=True)

    def get_system_overview(self) -> dict[str, Any]:
        """Get high-level system overview metrics."""
        # Get overall system metrics
        all_executions = self.workflow_state.list_executions(limit=1000)

        # Calculate system-wide metrics
        total_workflows = len(
            set(
                e.get("workflow_name") for e in all_executions if e.get("workflow_name")
            )
        )
        active_alerts_count = len(
            [a for a in self.active_alerts.values() if a.status == "active"]
        )

        # Recent activity (last 24 hours)
        recent_time = datetime.now(UTC) - timedelta(hours=24)
        recent_executions = [
            e
            for e in all_executions
            if e.get("start_time")
            and datetime.fromisoformat(e["start_time"].replace("Z", "+00:00"))
            >= recent_time
        ]

        recent_success_rate = 0.0
        if recent_executions:
            successful_recent = sum(
                1 for e in recent_executions if e.get("status") == "success"
            )
            recent_success_rate = (successful_recent / len(recent_executions)) * 100

        return {
            "total_workflows": total_workflows,
            "total_executions_24h": len(recent_executions),
            "system_success_rate_24h": recent_success_rate,
            "active_alerts": active_alerts_count,
            "alert_rules": len(self.alert_rules),
            "last_updated": datetime.now(UTC).isoformat(),
        }
