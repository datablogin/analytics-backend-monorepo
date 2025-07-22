"""Workflow scheduling and trigger system."""

import asyncio
import re
from abc import ABC, abstractmethod
from datetime import timezone, datetime, timedelta
from enum import Enum
from typing import Any

import structlog
from croniter import croniter
from pydantic import BaseModel, Field, validator

logger = structlog.get_logger(__name__)


class TriggerType(str, Enum):
    """Types of workflow triggers."""

    MANUAL = "manual"
    CRON = "cron"
    INTERVAL = "interval"
    EVENT = "event"
    WEBHOOK = "webhook"
    FILE_WATCH = "file_watch"
    DATA_ARRIVAL = "data_arrival"


class TriggerStatus(str, Enum):
    """Trigger execution status."""

    ACTIVE = "active"
    INACTIVE = "inactive"
    ERROR = "error"
    PAUSED = "paused"


class BaseTrigger(ABC):
    """Abstract base class for workflow triggers."""

    def __init__(self, name: str, workflow_name: str, workflow_version: str):
        self.name = name
        self.workflow_name = workflow_name
        self.workflow_version = workflow_version
        self.status = TriggerStatus.INACTIVE
        self.created_at = datetime.now(timezone.utc)
        self.last_triggered: datetime | None = None
        self.trigger_count = 0
        self.error_count = 0
        self.last_error: str | None = None

    @abstractmethod
    async def should_trigger(self, current_time: datetime) -> bool:
        """Check if trigger should fire at current time."""
        pass

    @abstractmethod
    def get_next_run_time(self) -> datetime | None:
        """Get next scheduled run time."""
        pass

    def mark_triggered(self) -> None:
        """Mark trigger as fired."""
        self.last_triggered = datetime.now(timezone.utc)
        self.trigger_count += 1
        logger.info(
            "Trigger fired",
            trigger_name=self.name,
            workflow_name=self.workflow_name,
            trigger_count=self.trigger_count,
        )

    def mark_error(self, error_message: str) -> None:
        """Mark trigger error."""
        self.error_count += 1
        self.last_error = error_message
        self.status = TriggerStatus.ERROR
        logger.error(
            "Trigger error",
            trigger_name=self.name,
            workflow_name=self.workflow_name,
            error=error_message,
            error_count=self.error_count,
        )

    def activate(self) -> None:
        """Activate trigger."""
        self.status = TriggerStatus.ACTIVE
        logger.info("Trigger activated", trigger_name=self.name)

    def deactivate(self) -> None:
        """Deactivate trigger."""
        self.status = TriggerStatus.INACTIVE
        logger.info("Trigger deactivated", trigger_name=self.name)

    def pause(self) -> None:
        """Pause trigger."""
        self.status = TriggerStatus.PAUSED
        logger.info("Trigger paused", trigger_name=self.name)

    def to_dict(self) -> dict[str, Any]:
        """Convert trigger to dictionary."""
        return {
            "name": self.name,
            "workflow_name": self.workflow_name,
            "workflow_version": self.workflow_version,
            "status": self.status.value,
            "created_at": self.created_at.isoformat(),
            "last_triggered": self.last_triggered.isoformat()
            if self.last_triggered
            else None,
            "trigger_count": self.trigger_count,
            "error_count": self.error_count,
            "last_error": self.last_error,
            "next_run_time": self.get_next_run_time().isoformat()
            if self.get_next_run_time()
            else None,
        }


class CronTrigger(BaseTrigger):
    """Cron-based scheduling trigger."""

    def __init__(
        self,
        name: str,
        workflow_name: str,
        workflow_version: str,
        cron_expression: str,
        timezone_name: str = "UTC",
    ):
        super().__init__(name, workflow_name, workflow_version)
        self.cron_expression = cron_expression
        self.timezone_name = timezone_name

        # Validate cron expression
        try:
            self.cron_iter = croniter(cron_expression, datetime.now())
        except ValueError as e:
            raise ValueError(f"Invalid cron expression '{cron_expression}': {e}")

        logger.info(
            "Cron trigger created",
            trigger_name=name,
            cron_expression=cron_expression,
            next_run=self.get_next_run_time(),
        )

    async def should_trigger(self, current_time: datetime) -> bool:
        """Check if cron trigger should fire."""
        if self.status != TriggerStatus.ACTIVE:
            return False

        # Get next scheduled time
        next_time = self.get_next_run_time()
        if not next_time:
            return False

        # Check if current time is past the next scheduled time
        # Allow 60 second window to account for scheduling delays
        return current_time >= next_time and current_time <= next_time + timedelta(
            seconds=60
        )

    def get_next_run_time(self) -> datetime | None:
        """Get next cron scheduled time."""
        try:
            # Reset iterator to current time
            self.cron_iter = croniter(self.cron_expression, datetime.now(timezone.utc))
            return self.cron_iter.get_next(datetime)
        except Exception as e:
            logger.error("Error calculating next cron time", error=str(e))
            return None


class IntervalTrigger(BaseTrigger):
    """Interval-based scheduling trigger."""

    def __init__(
        self,
        name: str,
        workflow_name: str,
        workflow_version: str,
        interval_seconds: int,
        start_time: datetime | None = None,
    ):
        super().__init__(name, workflow_name, workflow_version)
        self.interval_seconds = interval_seconds
        self.start_time = start_time or datetime.now(timezone.utc)

        logger.info(
            "Interval trigger created",
            trigger_name=name,
            interval_seconds=interval_seconds,
            start_time=self.start_time.isoformat(),
        )

    async def should_trigger(self, current_time: datetime) -> bool:
        """Check if interval trigger should fire."""
        if self.status != TriggerStatus.ACTIVE:
            return False

        # Calculate time since last trigger or start time
        last_time = self.last_triggered or self.start_time
        time_since_last = (current_time - last_time).total_seconds()

        return time_since_last >= self.interval_seconds

    def get_next_run_time(self) -> datetime | None:
        """Get next interval scheduled time."""
        last_time = self.last_triggered or self.start_time
        return last_time + timedelta(seconds=self.interval_seconds)


class EventTrigger(BaseTrigger):
    """Event-driven trigger."""

    def __init__(
        self,
        name: str,
        workflow_name: str,
        workflow_version: str,
        event_type: str,
        event_pattern: str | None = None,
        conditions: dict[str, Any] | None = None,
    ):
        super().__init__(name, workflow_name, workflow_version)
        self.event_type = event_type
        self.event_pattern = event_pattern
        self.conditions = conditions or {}
        self.pending_events: list[dict[str, Any]] = []

        # Compile regex pattern if provided
        self.compiled_pattern = re.compile(event_pattern) if event_pattern else None

        logger.info(
            "Event trigger created",
            trigger_name=name,
            event_type=event_type,
            event_pattern=event_pattern,
        )

    async def should_trigger(self, current_time: datetime) -> bool:
        """Check if event trigger should fire."""
        return self.status == TriggerStatus.ACTIVE and len(self.pending_events) > 0

    def get_next_run_time(self) -> datetime | None:
        """Event triggers don't have scheduled times."""
        return None

    def add_event(self, event_data: dict[str, Any]) -> bool:
        """Add event and check if it matches trigger conditions."""
        if self.status != TriggerStatus.ACTIVE:
            return False

        # Check event type
        if event_data.get("type") != self.event_type:
            return False

        # Check pattern matching
        if self.compiled_pattern:
            event_message = event_data.get("message", "")
            if not self.compiled_pattern.match(event_message):
                return False

        # Check additional conditions
        for key, expected_value in self.conditions.items():
            if event_data.get(key) != expected_value:
                return False

        # Event matches - add to pending
        self.pending_events.append(event_data)
        logger.info(
            "Event added to trigger",
            trigger_name=self.name,
            event_type=self.event_type,
            pending_events=len(self.pending_events),
        )

        return True

    def consume_events(self) -> list[dict[str, Any]]:
        """Consume and return all pending events."""
        events = self.pending_events.copy()
        self.pending_events.clear()
        return events


class WebhookTrigger(BaseTrigger):
    """Webhook-based trigger."""

    def __init__(
        self,
        name: str,
        workflow_name: str,
        workflow_version: str,
        webhook_path: str,
        http_method: str = "POST",
        authentication: dict[str, str] | None = None,
    ):
        super().__init__(name, workflow_name, workflow_version)
        self.webhook_path = webhook_path
        self.http_method = http_method.upper()
        self.authentication = authentication or {}
        self.pending_requests: list[dict[str, Any]] = []

        logger.info(
            "Webhook trigger created",
            trigger_name=name,
            webhook_path=webhook_path,
            http_method=self.http_method,
        )

    async def should_trigger(self, current_time: datetime) -> bool:
        """Check if webhook trigger should fire."""
        return self.status == TriggerStatus.ACTIVE and len(self.pending_requests) > 0

    def get_next_run_time(self) -> datetime | None:
        """Webhook triggers don't have scheduled times."""
        return None

    def add_webhook_request(self, request_data: dict[str, Any]) -> bool:
        """Add webhook request."""
        if self.status != TriggerStatus.ACTIVE:
            return False

        # Validate HTTP method
        if request_data.get("method") != self.http_method:
            return False

        # TODO: Implement authentication validation

        self.pending_requests.append(request_data)
        logger.info(
            "Webhook request added",
            trigger_name=self.name,
            method=self.http_method,
            pending_requests=len(self.pending_requests),
        )

        return True

    def consume_requests(self) -> list[dict[str, Any]]:
        """Consume and return all pending requests."""
        requests = self.pending_requests.copy()
        self.pending_requests.clear()
        return requests


class TriggerConfig(BaseModel):
    """Configuration for trigger creation."""

    name: str = Field(description="Trigger name")
    trigger_type: TriggerType = Field(description="Type of trigger")
    workflow_name: str = Field(description="Target workflow name")
    workflow_version: str = Field(description="Target workflow version")

    # Cron trigger specific
    cron_expression: str | None = Field(default=None, description="Cron expression")
    timezone_name: str = Field(default="UTC", description="Timezone for cron")

    # Interval trigger specific
    interval_seconds: int | None = Field(
        default=None, description="Interval in seconds"
    )
    start_time: datetime | None = Field(
        default=None, description="Start time for interval"
    )

    # Event trigger specific
    event_type: str | None = Field(
        default=None, description="Event type to listen for"
    )
    event_pattern: str | None = Field(
        default=None, description="Event pattern regex"
    )
    event_conditions: dict[str, Any] | None = Field(
        default=None, description="Event conditions"
    )

    # Webhook trigger specific
    webhook_path: str | None = Field(default=None, description="Webhook URL path")
    http_method: str = Field(default="POST", description="HTTP method")
    webhook_auth: dict[str, str] | None = Field(
        default=None, description="Authentication config"
    )

    # General settings
    enabled: bool = Field(default=True, description="Whether trigger is enabled")
    parameters: dict[str, Any] = Field(
        default_factory=dict, description="Workflow parameters"
    )

    @validator("cron_expression")
    def validate_cron(cls, v: str | None, values: dict[str, Any]) -> str | None:
        """Validate cron expression if trigger type is cron."""
        if values.get("trigger_type") == TriggerType.CRON:
            if not v:
                raise ValueError("Cron expression required for cron trigger")
            try:
                croniter(v, datetime.now())
            except ValueError as e:
                raise ValueError(f"Invalid cron expression: {e}")
        return v

    @validator("interval_seconds")
    def validate_interval(
        cls, v: int | None, values: dict[str, Any]
    ) -> int | None:
        """Validate interval for interval trigger."""
        if values.get("trigger_type") == TriggerType.INTERVAL:
            if not v or v <= 0:
                raise ValueError(
                    "Positive interval_seconds required for interval trigger"
                )
            if v < 60:
                raise ValueError("Minimum interval is 60 seconds")
        return v

    @validator("event_type")
    def validate_event_type(
        cls, v: str | None, values: dict[str, Any]
    ) -> str | None:
        """Validate event type for event trigger."""
        if values.get("trigger_type") == TriggerType.EVENT:
            if not v:
                raise ValueError("Event type required for event trigger")
        return v

    @validator("webhook_path")
    def validate_webhook_path(
        cls, v: str | None, values: dict[str, Any]
    ) -> str | None:
        """Validate webhook path for webhook trigger."""
        if values.get("trigger_type") == TriggerType.WEBHOOK:
            if not v:
                raise ValueError("Webhook path required for webhook trigger")
            if not v.startswith("/"):
                raise ValueError("Webhook path must start with '/'")
        return v


class WorkflowScheduler:
    """Main workflow scheduler managing all triggers."""

    def __init__(self, workflow_engine):
        self.workflow_engine = workflow_engine
        self.triggers: dict[str, BaseTrigger] = {}
        self.running = False
        self.scheduler_task: asyncio.Task | None = None
        self.check_interval = 30  # Check triggers every 30 seconds

        logger.info("Workflow scheduler initialized")

    def create_trigger(self, config: TriggerConfig) -> BaseTrigger:
        """Create trigger from configuration."""
        if config.name in self.triggers:
            raise ValueError(f"Trigger '{config.name}' already exists")

        # Create appropriate trigger type
        if config.trigger_type == TriggerType.CRON:
            trigger = CronTrigger(
                name=config.name,
                workflow_name=config.workflow_name,
                workflow_version=config.workflow_version,
                cron_expression=config.cron_expression,
                timezone_name=config.timezone_name,
            )
        elif config.trigger_type == TriggerType.INTERVAL:
            trigger = IntervalTrigger(
                name=config.name,
                workflow_name=config.workflow_name,
                workflow_version=config.workflow_version,
                interval_seconds=config.interval_seconds,
                start_time=config.start_time,
            )
        elif config.trigger_type == TriggerType.EVENT:
            trigger = EventTrigger(
                name=config.name,
                workflow_name=config.workflow_name,
                workflow_version=config.workflow_version,
                event_type=config.event_type,
                event_pattern=config.event_pattern,
                conditions=config.event_conditions,
            )
        elif config.trigger_type == TriggerType.WEBHOOK:
            trigger = WebhookTrigger(
                name=config.name,
                workflow_name=config.workflow_name,
                workflow_version=config.workflow_version,
                webhook_path=config.webhook_path,
                http_method=config.http_method,
                authentication=config.webhook_auth,
            )
        else:
            raise ValueError(f"Unsupported trigger type: {config.trigger_type}")

        # Add to registry and activate if enabled
        self.triggers[config.name] = trigger
        if config.enabled:
            trigger.activate()

        logger.info(
            "Trigger created",
            trigger_name=config.name,
            trigger_type=config.trigger_type.value,
            workflow_name=config.workflow_name,
        )

        return trigger

    def remove_trigger(self, trigger_name: str) -> bool:
        """Remove trigger."""
        if trigger_name in self.triggers:
            trigger = self.triggers[trigger_name]
            trigger.deactivate()
            del self.triggers[trigger_name]
            logger.info("Trigger removed", trigger_name=trigger_name)
            return True
        return False

    def get_trigger(self, trigger_name: str) -> BaseTrigger | None:
        """Get trigger by name."""
        return self.triggers.get(trigger_name)

    def list_triggers(
        self, workflow_name: str | None = None
    ) -> list[dict[str, Any]]:
        """List all triggers, optionally filtered by workflow."""
        triggers = list(self.triggers.values())
        if workflow_name:
            triggers = [t for t in triggers if t.workflow_name == workflow_name]

        return [trigger.to_dict() for trigger in triggers]

    def add_event(self, event_data: dict[str, Any]) -> list[str]:
        """Add event to matching event triggers."""
        triggered_names = []
        for trigger in self.triggers.values():
            if isinstance(trigger, EventTrigger):
                if trigger.add_event(event_data):
                    triggered_names.append(trigger.name)
        return triggered_names

    def add_webhook_request(
        self, webhook_path: str, request_data: dict[str, Any]
    ) -> list[str]:
        """Add webhook request to matching webhook triggers."""
        triggered_names = []
        for trigger in self.triggers.values():
            if isinstance(trigger, WebhookTrigger):
                if trigger.webhook_path == webhook_path:
                    if trigger.add_webhook_request(request_data):
                        triggered_names.append(trigger.name)
        return triggered_names

    async def start(self) -> None:
        """Start scheduler loop."""
        if self.running:
            return

        self.running = True
        self.scheduler_task = asyncio.create_task(self._scheduler_loop())
        logger.info("Workflow scheduler started")

    async def stop(self) -> None:
        """Stop scheduler loop."""
        self.running = False
        if self.scheduler_task:
            self.scheduler_task.cancel()
            try:
                await self.scheduler_task
            except asyncio.CancelledError:
                pass
        logger.info("Workflow scheduler stopped")

    async def _scheduler_loop(self) -> None:
        """Main scheduler loop."""
        while self.running:
            try:
                current_time = datetime.now(timezone.utc)

                # Check all triggers
                for trigger in list(self.triggers.values()):
                    try:
                        if await trigger.should_trigger(current_time):
                            await self._execute_triggered_workflow(trigger)
                    except Exception as e:
                        trigger.mark_error(str(e))
                        logger.error(
                            "Trigger evaluation error",
                            trigger_name=trigger.name,
                            error=str(e),
                        )

                # Sleep until next check
                await asyncio.sleep(self.check_interval)

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error("Scheduler loop error", error=str(e))
                await asyncio.sleep(self.check_interval)

    async def _execute_triggered_workflow(self, trigger: BaseTrigger) -> None:
        """Execute workflow for triggered trigger."""
        try:
            # Prepare parameters
            parameters = {}

            # Add trigger-specific data
            if isinstance(trigger, EventTrigger):
                events = trigger.consume_events()
                parameters["trigger_events"] = events
            elif isinstance(trigger, WebhookTrigger):
                requests = trigger.consume_requests()
                parameters["webhook_requests"] = requests

            # Execute workflow
            execution_id = await self.workflow_engine.execute_workflow(
                workflow_name=trigger.workflow_name,
                workflow_version=trigger.workflow_version,
                parameters=parameters,
                trigger_type=f"trigger:{trigger.name}",
            )

            trigger.mark_triggered()

            logger.info(
                "Workflow triggered successfully",
                trigger_name=trigger.name,
                workflow_name=trigger.workflow_name,
                execution_id=execution_id,
            )

        except Exception as e:
            trigger.mark_error(str(e))
            logger.error(
                "Failed to execute triggered workflow",
                trigger_name=trigger.name,
                workflow_name=trigger.workflow_name,
                error=str(e),
            )
