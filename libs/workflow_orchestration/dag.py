"""Directed Acyclic Graph (DAG) workflow definition system."""

import uuid
from collections.abc import Callable
from datetime import UTC, datetime
from enum import Enum
from typing import Any

import structlog
from pydantic import BaseModel, Field, validator

logger = structlog.get_logger(__name__)


class TaskStatus(str, Enum):
    """Task execution status."""

    PENDING = "pending"
    RUNNING = "running"
    SUCCESS = "success"
    FAILED = "failed"
    SKIPPED = "skipped"
    RETRY = "retry"
    CANCELLED = "cancelled"


class TaskType(str, Enum):
    """Task type classification."""

    DATA_INGESTION = "data_ingestion"
    DATA_VALIDATION = "data_validation"
    DATA_TRANSFORMATION = "data_transformation"
    DATA_QUALITY = "data_quality"
    MACHINE_LEARNING = "machine_learning"
    REPORTING = "reporting"
    NOTIFICATION = "notification"
    CUSTOM = "custom"


class ResourceRequirement(BaseModel):
    """Resource requirements for task execution."""

    cpu: float = Field(default=1.0, ge=0.1, le=16.0, description="CPU cores required")
    memory_mb: int = Field(default=512, ge=128, le=32768, description="Memory in MB")
    disk_gb: int | None = Field(default=None, ge=1, description="Disk space in GB")
    gpu_count: int = Field(default=0, ge=0, le=8, description="Number of GPUs")
    timeout_seconds: int = Field(default=3600, ge=60, description="Task timeout")
    priority: int = Field(
        default=5, ge=1, le=10, description="Task priority (1=highest)"
    )


class TaskConfig(BaseModel):
    """Task configuration and metadata."""

    name: str = Field(description="Task name (must be unique within DAG)")
    description: str | None = Field(default=None, description="Task description")
    task_type: TaskType = Field(default=TaskType.CUSTOM, description="Task type")
    function_name: str = Field(description="Function to execute")
    parameters: dict[str, Any] = Field(
        default_factory=dict, description="Task parameters"
    )
    resources: ResourceRequirement = Field(default_factory=ResourceRequirement)
    tags: list[str] = Field(default_factory=list, description="Task tags")
    owner: str | None = Field(default=None, description="Task owner")
    created_at: datetime = Field(default_factory=lambda: datetime.now(UTC))

    @validator("name")
    def validate_name(cls, v: str) -> str:  # noqa: N805
        """Validate task name format."""
        if not v or not v.strip():
            raise ValueError("Task name cannot be empty")
        if len(v) > 100:
            raise ValueError("Task name cannot exceed 100 characters")
        if not v.replace("_", "").replace("-", "").isalnum():
            raise ValueError(
                "Task name can only contain alphanumeric characters, hyphens, and underscores"
            )
        return v.strip()


class Task:
    """Individual task within a workflow DAG."""

    def __init__(
        self,
        config: TaskConfig,
        upstream_tasks: list[str] | None = None,
        downstream_tasks: list[str] | None = None,
        condition: Callable[[dict[str, Any]], bool] | None = None,
    ):
        self.id = str(uuid.uuid4())
        self.config = config
        self.upstream_tasks: set[str] = set(upstream_tasks or [])
        self.downstream_tasks: set[str] = set(downstream_tasks or [])
        self.condition = condition  # Conditional execution logic
        self.status = TaskStatus.PENDING
        self.start_time: datetime | None = None
        self.end_time: datetime | None = None
        self.error_message: str | None = None
        self.retry_count = 0
        self.execution_context: dict[str, Any] = {}

    def add_upstream(self, task_name: str) -> None:
        """Add upstream dependency."""
        self.upstream_tasks.add(task_name)

    def add_downstream(self, task_name: str) -> None:
        """Add downstream dependency."""
        self.downstream_tasks.add(task_name)

    def remove_upstream(self, task_name: str) -> None:
        """Remove upstream dependency."""
        self.upstream_tasks.discard(task_name)

    def remove_downstream(self, task_name: str) -> None:
        """Remove downstream dependency."""
        self.downstream_tasks.discard(task_name)

    def should_execute(self, context: dict[str, Any]) -> bool:
        """Check if task should execute based on conditions."""
        if self.condition is None:
            return True
        try:
            return self.condition(context)
        except Exception as e:
            logger.warning(
                "Task condition evaluation failed",
                task_name=self.config.name,
                error=str(e),
            )
            return False

    def to_dict(self) -> dict[str, Any]:
        """Convert task to dictionary representation."""
        return {
            "id": self.id,
            "name": self.config.name,
            "description": self.config.description,
            "task_type": self.config.task_type.value,
            "function_name": self.config.function_name,
            "parameters": self.config.parameters,
            "resources": self.config.resources.model_dump(),
            "upstream_tasks": list(self.upstream_tasks),
            "downstream_tasks": list(self.downstream_tasks),
            "status": self.status.value,
            "start_time": self.start_time.isoformat() if self.start_time else None,
            "end_time": self.end_time.isoformat() if self.end_time else None,
            "error_message": self.error_message,
            "retry_count": self.retry_count,
            "tags": self.config.tags,
            "owner": self.config.owner,
        }


class WorkflowDefinition(BaseModel):
    """Complete workflow definition with metadata."""

    name: str = Field(description="Workflow name")
    version: str = Field(description="Workflow version")
    description: str | None = Field(default=None, description="Workflow description")
    owner: str = Field(description="Workflow owner")
    tags: list[str] = Field(default_factory=list, description="Workflow tags")
    default_resources: ResourceRequirement = Field(default_factory=ResourceRequirement)
    max_parallel_tasks: int = Field(
        default=10, ge=1, le=100, description="Max parallel tasks"
    )
    timeout_seconds: int = Field(default=7200, ge=300, description="Workflow timeout")
    retry_policy: dict[str, Any] | None = Field(
        default=None, description="Default retry policy"
    )
    notifications: list[str] = Field(
        default_factory=list, description="Notification channels"
    )
    created_at: datetime = Field(default_factory=lambda: datetime.now(UTC))
    updated_at: datetime = Field(default_factory=lambda: datetime.now(UTC))

    @validator("name")
    def validate_name(cls, v: str) -> str:  # noqa: N805
        """Validate workflow name format."""
        if not v or not v.strip():
            raise ValueError("Workflow name cannot be empty")
        if len(v) > 100:
            raise ValueError("Workflow name cannot exceed 100 characters")
        return v.strip()


class DAG:
    """Directed Acyclic Graph for workflow definition."""

    def __init__(self, definition: WorkflowDefinition):
        self.definition = definition
        self.tasks: dict[str, Task] = {}
        self.execution_id: str | None = None
        self.created_at = datetime.now(UTC)

    def add_task(self, task: Task) -> None:
        """Add task to DAG."""
        if task.config.name in self.tasks:
            raise ValueError(f"Task '{task.config.name}' already exists in DAG")
        self.tasks[task.config.name] = task
        logger.info(
            "Task added to DAG",
            task_name=task.config.name,
            dag_name=self.definition.name,
        )

    def remove_task(self, task_name: str) -> None:
        """Remove task from DAG."""
        if task_name not in self.tasks:
            raise ValueError(f"Task '{task_name}' not found in DAG")

        # Remove dependencies
        task = self.tasks[task_name]
        for upstream in task.upstream_tasks:
            if upstream in self.tasks:
                self.tasks[upstream].remove_downstream(task_name)
        for downstream in task.downstream_tasks:
            if downstream in self.tasks:
                self.tasks[downstream].remove_upstream(task_name)

        del self.tasks[task_name]
        logger.info(
            "Task removed from DAG", task_name=task_name, dag_name=self.definition.name
        )

    def set_dependency(self, upstream_task: str, downstream_task: str) -> None:
        """Set dependency between tasks."""
        if upstream_task not in self.tasks:
            raise ValueError(f"Upstream task '{upstream_task}' not found")
        if downstream_task not in self.tasks:
            raise ValueError(f"Downstream task '{downstream_task}' not found")
        if upstream_task == downstream_task:
            raise ValueError("Task cannot depend on itself")

        self.tasks[upstream_task].add_downstream(downstream_task)
        self.tasks[downstream_task].add_upstream(upstream_task)

        # Check for cycles
        if self.has_cycle():
            # Revert the dependency
            self.tasks[upstream_task].remove_downstream(downstream_task)
            self.tasks[downstream_task].remove_upstream(upstream_task)
            raise ValueError(
                f"Adding dependency would create cycle: {upstream_task} -> {downstream_task}"
            )

        logger.info(
            "Dependency set", upstream=upstream_task, downstream=downstream_task
        )

    def has_cycle(self) -> bool:
        """Check if DAG has cycles using DFS."""
        visited = set()
        rec_stack = set()

        def dfs(task_name: str) -> bool:
            visited.add(task_name)
            rec_stack.add(task_name)

            for downstream in self.tasks[task_name].downstream_tasks:
                if downstream not in visited:
                    if dfs(downstream):
                        return True
                elif downstream in rec_stack:
                    return True

            rec_stack.remove(task_name)
            return False

        for task_name in self.tasks:
            if task_name not in visited:
                if dfs(task_name):
                    return True
        return False

    def get_ready_tasks(self) -> list[str]:
        """Get tasks ready for execution (all dependencies completed)."""
        ready_tasks = []
        for task_name, task in self.tasks.items():
            if task.status == TaskStatus.PENDING:
                # Check if all upstream tasks are completed successfully
                upstream_completed = all(
                    self.tasks[upstream].status == TaskStatus.SUCCESS
                    for upstream in task.upstream_tasks
                )
                if upstream_completed:
                    ready_tasks.append(task_name)
        return ready_tasks

    def get_root_tasks(self) -> list[str]:
        """Get tasks with no upstream dependencies."""
        return [
            task_name
            for task_name, task in self.tasks.items()
            if not task.upstream_tasks
        ]

    def get_leaf_tasks(self) -> list[str]:
        """Get tasks with no downstream dependencies."""
        return [
            task_name
            for task_name, task in self.tasks.items()
            if not task.downstream_tasks
        ]

    def topological_sort(self) -> list[str]:
        """Return topologically sorted task order."""
        in_degree = {
            task_name: len(task.upstream_tasks)
            for task_name, task in self.tasks.items()
        }
        queue = [task_name for task_name, degree in in_degree.items() if degree == 0]
        result = []

        while queue:
            task_name = queue.pop(0)
            result.append(task_name)

            for downstream in self.tasks[task_name].downstream_tasks:
                in_degree[downstream] -= 1
                if in_degree[downstream] == 0:
                    queue.append(downstream)

        if len(result) != len(self.tasks):
            raise ValueError("DAG contains cycles - cannot perform topological sort")

        return result

    def validate(self) -> list[str]:
        """Validate DAG structure and return list of issues."""
        issues = []

        # Check for cycles
        if self.has_cycle():
            issues.append("DAG contains cycles")

        # Check for orphaned tasks
        for task_name, task in self.tasks.items():
            # Check upstream references exist
            for upstream in task.upstream_tasks:
                if upstream not in self.tasks:
                    issues.append(
                        f"Task '{task_name}' references non-existent upstream task '{upstream}'"
                    )
            # Check downstream references exist
            for downstream in task.downstream_tasks:
                if downstream not in self.tasks:
                    issues.append(
                        f"Task '{task_name}' references non-existent downstream task '{downstream}'"
                    )

        # Check resource requirements
        for task_name, task in self.tasks.items():
            if task.config.resources.cpu <= 0:
                issues.append(f"Task '{task_name}' has invalid CPU requirement")
            if task.config.resources.memory_mb <= 0:
                issues.append(f"Task '{task_name}' has invalid memory requirement")

        return issues

    def to_dict(self) -> dict[str, Any]:
        """Convert DAG to dictionary representation."""
        return {
            "definition": self.definition.model_dump(),
            "tasks": {name: task.to_dict() for name, task in self.tasks.items()},
            "execution_id": self.execution_id,
            "created_at": self.created_at.isoformat(),
            "task_count": len(self.tasks),
            "root_tasks": self.get_root_tasks(),
            "leaf_tasks": self.get_leaf_tasks(),
        }
