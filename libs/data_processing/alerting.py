"""Data quality alerting and notification system."""

import json
import smtplib
from datetime import datetime
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from enum import Enum
from typing import Any

import structlog
from pydantic import BaseModel, Field

logger = structlog.get_logger(__name__)


class AlertSeverity(str, Enum):
    """Alert severity levels."""

    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"
    CRITICAL = "critical"


class AlertChannel(str, Enum):
    """Alert delivery channels."""

    EMAIL = "email"
    SLACK = "slack"
    WEBHOOK = "webhook"
    LOG = "log"


class AlertRule(BaseModel):
    """Alert rule configuration."""

    id: str
    name: str
    description: str
    severity: AlertSeverity
    channels: list[AlertChannel]
    conditions: dict[str, Any]
    enabled: bool = True
    cooldown_minutes: int = 60
    metadata: dict[str, Any] = Field(default_factory=dict)


class Alert(BaseModel):
    """Data quality alert."""

    id: str = Field(
        default_factory=lambda: f"alert_{int(datetime.utcnow().timestamp())}"
    )
    rule_id: str
    severity: AlertSeverity
    title: str
    message: str
    dataset_name: str
    timestamp: datetime = Field(default_factory=datetime.utcnow)
    details: dict[str, Any] = Field(default_factory=dict)
    status: str = "active"  # active, acknowledged, resolved


class AlertingConfig(BaseModel):
    """Alerting system configuration."""

    enabled: bool = True
    default_channels: list[AlertChannel] = [AlertChannel.LOG]
    email_config: dict[str, str] | None = None
    slack_config: dict[str, str] | None = None
    webhook_config: dict[str, str] | None = None


class DataQualityAlerting:
    """Data quality alerting system."""

    def __init__(self, config: AlertingConfig):
        self.config = config
        self.rules: dict[str, AlertRule] = {}
        self.alert_history: list[Alert] = []
        self.logger = structlog.get_logger(__name__)

    def add_rule(self, rule: AlertRule) -> None:
        """Add an alert rule."""
        self.rules[rule.id] = rule
        self.logger.info("Alert rule added", rule_id=rule.id, name=rule.name)

    def remove_rule(self, rule_id: str) -> None:
        """Remove an alert rule."""
        if rule_id in self.rules:
            del self.rules[rule_id]
            self.logger.info("Alert rule removed", rule_id=rule_id)

    def evaluate_validation_result(
        self, validation_result: dict[str, Any]
    ) -> list[Alert]:
        """Evaluate validation result against alert rules."""
        alerts = []

        for rule in self.rules.values():
            if not rule.enabled:
                continue

            if self._should_alert(validation_result, rule):
                alert = self._create_alert_from_validation(validation_result, rule)
                if self._check_cooldown(alert):
                    alerts.append(alert)
                    self.alert_history.append(alert)

        return alerts

    def _should_alert(self, validation_result: dict[str, Any], rule: AlertRule) -> bool:
        """Check if validation result should trigger an alert."""
        conditions = rule.conditions

        # Check success rate threshold
        if "min_success_rate" in conditions:
            success_rate = validation_result.get("success_rate", 100)
            if success_rate < conditions["min_success_rate"]:
                return True

        # Check failure count threshold
        if "max_failures" in conditions:
            failure_count = validation_result.get("failure_count", 0)
            if failure_count > conditions["max_failures"]:
                return True

        # Check specific expectation failures
        if "expectation_types" in conditions:
            expected_types = set(conditions["expectation_types"])
            failures = validation_result.get("failures", [])
            failed_types = {f.get("expectation_type") for f in failures}
            if expected_types.intersection(failed_types):
                return True

        return False

    def _create_alert_from_validation(
        self, validation_result: dict[str, Any], rule: AlertRule
    ) -> Alert:
        """Create alert from validation result."""
        dataset_name = validation_result.get("dataset_name", "unknown")
        success_rate = validation_result.get("success_rate", 0)
        failure_count = validation_result.get("failure_count", 0)

        title = f"Data Quality Alert: {rule.name}"
        message = (
            f"Data quality validation failed for dataset '{dataset_name}'. "
            f"Success rate: {success_rate:.1f}%, Failures: {failure_count}"
        )

        return Alert(
            rule_id=rule.id,
            severity=rule.severity,
            title=title,
            message=message,
            dataset_name=dataset_name,
            details={
                "validation_result": validation_result,
                "rule_conditions": rule.conditions,
            },
        )

    def _check_cooldown(self, alert: Alert) -> bool:
        """Check if alert is within cooldown period."""
        rule = self.rules.get(alert.rule_id)
        if not rule:
            return True

        cooldown_minutes = rule.cooldown_minutes
        if cooldown_minutes <= 0:
            return True

        # Check recent alerts for same rule and dataset
        recent_threshold = datetime.utcnow().timestamp() - (cooldown_minutes * 60)

        for historic_alert in reversed(self.alert_history):
            if (
                historic_alert.rule_id == alert.rule_id
                and historic_alert.dataset_name == alert.dataset_name
                and historic_alert.timestamp.timestamp() > recent_threshold
            ):
                return False

        return True

    def send_alerts(self, alerts: list[Alert]) -> None:
        """Send alerts through configured channels."""
        if not self.config.enabled:
            return

        for alert in alerts:
            rule = self.rules.get(alert.rule_id)
            if not rule:
                continue

            channels = rule.channels or self.config.default_channels

            for channel in channels:
                try:
                    if channel == AlertChannel.EMAIL:
                        self._send_email_alert(alert)
                    elif channel == AlertChannel.SLACK:
                        self._send_slack_alert(alert)
                    elif channel == AlertChannel.WEBHOOK:
                        self._send_webhook_alert(alert)
                    elif channel == AlertChannel.LOG:
                        self._send_log_alert(alert)

                except Exception as e:
                    self.logger.error(
                        "Failed to send alert",
                        alert_id=alert.id,
                        channel=channel,
                        error=str(e),
                    )

    def _send_email_alert(self, alert: Alert) -> None:
        """Send alert via email."""
        if not self.config.email_config:
            raise ValueError("Email configuration not provided")

        config = self.config.email_config

        msg = MIMEMultipart()
        msg["From"] = config["sender"]
        msg["To"] = config["recipients"]
        msg["Subject"] = f"[{alert.severity.upper()}] {alert.title}"

        body = f"""
        Data Quality Alert
        
        Severity: {alert.severity.upper()}
        Dataset: {alert.dataset_name}
        Time: {alert.timestamp.isoformat()}
        
        {alert.message}
        
        Details:
        {json.dumps(alert.details, indent=2)}
        """

        msg.attach(MIMEText(body, "plain"))

        server = smtplib.SMTP(config["smtp_server"], config.get("smtp_port", 587))
        if config.get("use_tls", True):
            server.starttls()
        if config.get("username") and config.get("password"):
            server.login(config["username"], config["password"])

        server.sendmail(
            config["sender"], config["recipients"].split(","), msg.as_string()
        )
        server.quit()

        self.logger.info("Email alert sent", alert_id=alert.id)

    def _send_slack_alert(self, alert: Alert) -> None:
        """Send alert via Slack webhook."""
        if not self.config.slack_config:
            raise ValueError("Slack configuration not provided")

        import httpx

        webhook_url = self.config.slack_config["webhook_url"]

        # Create Slack message
        color = {
            AlertSeverity.LOW: "good",
            AlertSeverity.MEDIUM: "warning",
            AlertSeverity.HIGH: "danger",
            AlertSeverity.CRITICAL: "danger",
        }.get(alert.severity, "warning")

        payload = {
            "attachments": [
                {
                    "color": color,
                    "title": alert.title,
                    "text": alert.message,
                    "fields": [
                        {
                            "title": "Dataset",
                            "value": alert.dataset_name,
                            "short": True,
                        },
                        {
                            "title": "Severity",
                            "value": alert.severity.upper(),
                            "short": True,
                        },
                        {
                            "title": "Timestamp",
                            "value": alert.timestamp.isoformat(),
                            "short": False,
                        },
                    ],
                }
            ]
        }

        with httpx.Client() as client:
            response = client.post(webhook_url, json=payload)
            response.raise_for_status()

        self.logger.info("Slack alert sent", alert_id=alert.id)

    def _send_webhook_alert(self, alert: Alert) -> None:
        """Send alert via generic webhook."""
        if not self.config.webhook_config:
            raise ValueError("Webhook configuration not provided")

        import httpx

        webhook_url = self.config.webhook_config["url"]
        headers = self.config.webhook_config.get("headers", {})

        payload = {
            "alert_id": alert.id,
            "rule_id": alert.rule_id,
            "severity": alert.severity,
            "title": alert.title,
            "message": alert.message,
            "dataset_name": alert.dataset_name,
            "timestamp": alert.timestamp.isoformat(),
            "details": alert.details,
        }

        with httpx.Client() as client:
            response = client.post(webhook_url, json=payload, headers=headers)
            response.raise_for_status()

        self.logger.info("Webhook alert sent", alert_id=alert.id)

    def _send_log_alert(self, alert: Alert) -> None:
        """Send alert via structured logging."""
        self.logger.warning(
            "Data quality alert",
            alert_id=alert.id,
            rule_id=alert.rule_id,
            severity=alert.severity,
            title=alert.title,
            message=alert.message,
            dataset_name=alert.dataset_name,
            timestamp=alert.timestamp.isoformat(),
            details=alert.details,
        )

    def get_active_alerts(self, dataset_name: str | None = None) -> list[Alert]:
        """Get currently active alerts."""
        active_alerts = [
            alert for alert in self.alert_history if alert.status == "active"
        ]

        if dataset_name:
            active_alerts = [
                alert for alert in active_alerts if alert.dataset_name == dataset_name
            ]

        return active_alerts

    def acknowledge_alert(self, alert_id: str) -> bool:
        """Acknowledge an alert."""
        for alert in self.alert_history:
            if alert.id == alert_id:
                alert.status = "acknowledged"
                self.logger.info("Alert acknowledged", alert_id=alert_id)
                return True
        return False

    def resolve_alert(self, alert_id: str) -> bool:
        """Resolve an alert."""
        for alert in self.alert_history:
            if alert.id == alert_id:
                alert.status = "resolved"
                self.logger.info("Alert resolved", alert_id=alert_id)
                return True
        return False


def create_default_alert_rules() -> list[AlertRule]:
    """Create default data quality alert rules."""
    return [
        AlertRule(
            id="low_success_rate",
            name="Low Success Rate",
            description="Alert when validation success rate falls below 95%",
            severity=AlertSeverity.HIGH,
            channels=[AlertChannel.LOG, AlertChannel.SLACK],
            conditions={"min_success_rate": 95.0},
        ),
        AlertRule(
            id="multiple_failures",
            name="Multiple Validation Failures",
            description="Alert when more than 5 expectations fail",
            severity=AlertSeverity.MEDIUM,
            channels=[AlertChannel.LOG],
            conditions={"max_failures": 5},
        ),
        AlertRule(
            id="critical_expectations",
            name="Critical Expectation Failures",
            description="Alert on failures of critical data expectations",
            severity=AlertSeverity.CRITICAL,
            channels=[AlertChannel.EMAIL, AlertChannel.SLACK],
            conditions={
                "expectation_types": [
                    "expect_column_values_to_not_be_null",
                    "expect_column_values_to_be_unique",
                    "expect_table_row_count_to_be_between",
                ]
            },
            cooldown_minutes=30,
        ),
    ]
