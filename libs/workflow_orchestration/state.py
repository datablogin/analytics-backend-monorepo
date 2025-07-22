"""Workflow state management and execution context."""

from datetime import UTC, datetime
from enum import Enum
from typing import Any

import structlog
from pydantic import BaseModel, Field

logger = structlog.get_logger(__name__)


class WorkflowStatus(str, Enum):
    """Workflow execution status."""

    PENDING = "pending"
    RUNNING = "running"
    SUCCESS = "success"
    FAILED = "failed"
    CANCELLED = "cancelled"
    PAUSED = "paused"


class TaskResult(BaseModel):
    """Result from task execution."""

    task_name: str = Field(description="Task name")
    status: str = Field(description="Task status")
    result_data: dict[str, Any] | None = Field(
        default=None, description="Task output data"
    )
    error_message: str | None = Field(
        default=None, description="Error message if failed"
    )
    start_time: datetime = Field(description="Task start time")
    end_time: datetime | None = Field(default=None, description="Task end time")
    duration_seconds: float | None = Field(default=None, description="Task duration")
    retry_count: int = Field(default=0, description="Number of retries")
    resource_usage: dict[str, Any] | None = Field(
        default=None, description="Resource utilization"
    )
    metadata: dict[str, Any] = Field(
        default_factory=dict, description="Additional metadata"
    )

    def calculate_duration(self) -> None:
        """Calculate and set duration if end_time is available."""
        if self.end_time and self.start_time:
            self.duration_seconds = (self.end_time - self.start_time).total_seconds()


class ExecutionContext(BaseModel):
    """Execution context for workflow and tasks."""

    workflow_id: str = Field(description="Workflow execution ID")
    workflow_name: str = Field(description="Workflow name")
    execution_date: datetime = Field(description="Workflow execution date")
    parameters: dict[str, Any] = Field(
        default_factory=dict, description="Workflow parameters"
    )
    user_id: str | None = Field(
        default=None, description="User who triggered execution"
    )
    trigger_type: str = Field(
        default="manual", description="What triggered the execution"
    )
    environment: str = Field(default="production", description="Execution environment")
    run_id: str | None = Field(default=None, description="Unique run identifier")
    parent_execution_id: str | None = Field(
        default=None, description="Parent execution if sub-workflow"
    )

    # Task-specific context
    task_results: dict[str, TaskResult] = Field(
        default_factory=dict, description="Results from completed tasks"
    )
    shared_data: dict[str, Any] = Field(
        default_factory=dict, description="Data shared between tasks"
    )

    def get_task_result(self, task_name: str) -> TaskResult | None:
        """Get result from a specific task."""
        return self.task_results.get(task_name)

    def add_task_result(self, result: TaskResult) -> None:
        """Add task result to context."""
        self.task_results[result.task_name] = result
        logger.info(
            "Task result added to context",
            task_name=result.task_name,
            status=result.status,
        )

    def get_upstream_results(self, upstream_tasks: list[str]) -> dict[str, TaskResult]:
        """Get results from specified upstream tasks."""
        return {
            task_name: result
            for task_name, result in self.task_results.items()
            if task_name in upstream_tasks
        }

    def set_shared_data(self, key: str, value: Any) -> None:
        """Set shared data accessible by all tasks."""
        self.shared_data[key] = value

    def get_shared_data(self, key: str, default: Any = None) -> Any:
        """Get shared data."""
        return self.shared_data.get(key, default)


class WorkflowExecution(BaseModel):
    """Complete workflow execution state."""

    execution_id: str = Field(description="Unique execution ID")
    workflow_name: str = Field(description="Workflow name")
    workflow_version: str = Field(description="Workflow version")
    status: WorkflowStatus = Field(
        default=WorkflowStatus.PENDING, description="Execution status"
    )

    # Timing
    scheduled_time: datetime | None = Field(
        default=None, description="When execution was scheduled"
    )
    start_time: datetime | None = Field(
        default=None, description="When execution started"
    )
    end_time: datetime | None = Field(
        default=None, description="When execution ended"
    )
    duration_seconds: float | None = Field(
        default=None, description="Execution duration"
    )

    # Context and results
    context: ExecutionContext = Field(description="Execution context")
    task_states: dict[str, str] = Field(
        default_factory=dict, description="Current task states"
    )

    # Metrics
    total_tasks: int = Field(default=0, description="Total number of tasks")
    completed_tasks: int = Field(default=0, description="Number of completed tasks")
    failed_tasks: int = Field(default=0, description="Number of failed tasks")
    success_rate: float = Field(default=0.0, description="Success rate percentage")

    # Error handling
    error_message: str | None = Field(
        default=None, description="Error message if failed"
    )
    failed_task: str | None = Field(
        default=None, description="Task that caused failure"
    )

    def update_metrics(self) -> None:
        """Update execution metrics based on task results."""
        if not self.context.task_results:
            return

        total_results = len(self.context.task_results)
        successful_results = sum(
            1
            for result in self.context.task_results.values()
            if result.status == "success"
        )
        failed_results = sum(
            1
            for result in self.context.task_results.values()
            if result.status == "failed"
        )

        self.completed_tasks = successful_results + failed_results
        self.failed_tasks = failed_results

        if total_results > 0:
            self.success_rate = (successful_results / total_results) * 100

    def calculate_duration(self) -> None:
        """Calculate execution duration if both start and end times are set."""
        if self.start_time and self.end_time:
            self.duration_seconds = (self.end_time - self.start_time).total_seconds()

    def mark_started(self) -> None:
        """Mark execution as started."""
        self.start_time = datetime.now(UTC)
        self.status = WorkflowStatus.RUNNING

    def mark_completed(
        self, success: bool, error_message: str | None = None
    ) -> None:
        """Mark execution as completed."""
        self.end_time = datetime.now(UTC)
        self.calculate_duration()
        self.update_metrics()

        if success:
            self.status = WorkflowStatus.SUCCESS
        else:
            self.status = WorkflowStatus.FAILED
            self.error_message = error_message

    def mark_cancelled(self) -> None:
        """Mark execution as cancelled."""
        self.end_time = datetime.now(UTC)
        self.calculate_duration()
        self.status = WorkflowStatus.CANCELLED


class WorkflowState:
    """Manages workflow execution state and provides state transitions."""

    def __init__(self):
        self.executions: dict[str, WorkflowExecution] = {}
        self.active_executions: dict[str, str] = {}  # workflow_name -> execution_id

    def create_execution(
        self,
        execution_id: str,
        workflow_name: str,
        workflow_version: str,
        context: ExecutionContext,
        scheduled_time: datetime | None = None,
    ) -> WorkflowExecution:
        """Create new workflow execution."""
        execution = WorkflowExecution(
            execution_id=execution_id,
            workflow_name=workflow_name,
            workflow_version=workflow_version,
            context=context,
            scheduled_time=scheduled_time or datetime.now(UTC),
        )

        self.executions[execution_id] = execution
        self.active_executions[workflow_name] = execution_id

        logger.info(
            "Workflow execution created",
            execution_id=execution_id,
            workflow_name=workflow_name,
            version=workflow_version,
        )

        return execution

    def get_execution(self, execution_id: str) -> WorkflowExecution | None:
        """Get workflow execution by ID."""
        return self.executions.get(execution_id)

    def get_active_execution(self, workflow_name: str) -> WorkflowExecution | None:
        """Get currently active execution for workflow."""
        execution_id = self.active_executions.get(workflow_name)
        return self.executions.get(execution_id) if execution_id else None

    def update_task_state(self, execution_id: str, task_name: str, status: str) -> None:
        """Update task state within execution."""
        execution = self.get_execution(execution_id)
        if execution:
            execution.task_states[task_name] = status

    def add_task_result(self, execution_id: str, result: TaskResult) -> None:
        """Add task result to execution."""
        execution = self.get_execution(execution_id)
        if execution:
            execution.context.add_task_result(result)
            execution.update_metrics()

    def complete_execution(
        self, execution_id: str, success: bool, error_message: str | None = None
    ) -> None:
        """Complete workflow execution."""
        execution = self.get_execution(execution_id)
        if execution:
            execution.mark_completed(success, error_message)

            # Remove from active executions
            if execution.workflow_name in self.active_executions:
                del self.active_executions[execution.workflow_name]

            logger.info(
                "Workflow execution completed",
                execution_id=execution_id,
                status=execution.status.value,
                duration=execution.duration_seconds,
            )

    def cancel_execution(self, execution_id: str) -> None:
        """Cancel workflow execution."""
        execution = self.get_execution(execution_id)
        if execution:
            execution.mark_cancelled()

            # Remove from active executions
            if execution.workflow_name in self.active_executions:
                del self.active_executions[execution.workflow_name]

            logger.info("Workflow execution cancelled", execution_id=execution_id)

    def get_execution_summary(self, execution_id: str) -> dict[str, Any] | None:
        """Get execution summary."""
        execution = self.get_execution(execution_id)
        if not execution:
            return None

        return {
            "execution_id": execution.execution_id,
            "workflow_name": execution.workflow_name,
            "workflow_version": execution.workflow_version,
            "status": execution.status.value,
            "start_time": execution.start_time.isoformat()
            if execution.start_time
            else None,
            "end_time": execution.end_time.isoformat() if execution.end_time else None,
            "duration_seconds": execution.duration_seconds,
            "total_tasks": execution.total_tasks,
            "completed_tasks": execution.completed_tasks,
            "failed_tasks": execution.failed_tasks,
            "success_rate": execution.success_rate,
            "error_message": execution.error_message,
        }

    def list_executions(
        self,
        workflow_name: str | None = None,
        status: WorkflowStatus | None = None,
        limit: int = 100,
    ) -> list[dict[str, Any]]:
        """List workflow executions with optional filtering."""
        executions = list(self.executions.values())

        # Apply filters
        if workflow_name:
            executions = [e for e in executions if e.workflow_name == workflow_name]
        if status:
            executions = [e for e in executions if e.status == status]

        # Sort by start time (newest first)
        executions.sort(
            key=lambda e: e.start_time or datetime.min.replace(tzinfo=UTC),
            reverse=True,
        )

        # Apply limit
        executions = executions[:limit]

        return [
            self.get_execution_summary(execution.execution_id)
            for execution in executions
        ]
