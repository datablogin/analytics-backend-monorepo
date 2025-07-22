"""Advanced Pipeline Orchestration & Workflow Management Library.

This module provides comprehensive workflow orchestration capabilities including:
- DAG-based workflow definition and execution
- Intelligent retry mechanisms with backoff strategies
- Resource pools and task concurrency management
- Event-driven and scheduled execution triggers
- Workflow versioning and rollback capabilities
- Real-time monitoring and alerting
"""

from .dag import (
    DAG,
    ResourceRequirement,
    Task,
    TaskConfig,
    TaskStatus,
    TaskType,
    WorkflowDefinition,
)
from .engine import WorkflowEngine, WorkflowExecutor
from .monitoring import (
    AlertRule,
    ExecutionMetrics,
    WorkflowHealthCheck,
    WorkflowMonitor,
)
from .retry import (
    ExponentialBackoffStrategy,
    RetryExecutor,
    RetryPolicy,
    RetryStrategy,
    create_default_retry_policy,
    create_aggressive_retry_policy,
)
from .scheduler import (
    CronTrigger,
    EventTrigger,
    IntervalTrigger,
    TriggerConfig,
    TriggerType,
    WorkflowScheduler,
)
from .state import ExecutionContext, TaskResult, WorkflowState
from .storage import (
    ExecutionArtifact,
    ExecutionHistory,
    WorkflowStorage,
    WorkflowVersion,
)

__version__ = "1.0.0"
__all__ = [
    # Core workflow components
    "DAG",
    "Task",
    "TaskConfig",
    "TaskType",
    "TaskStatus",
    "ResourceRequirement",
    "WorkflowDefinition",
    # Execution engine
    "WorkflowEngine",
    "WorkflowExecutor",
    # Monitoring and metrics
    "WorkflowMonitor",
    "ExecutionMetrics",
    "AlertRule",
    "WorkflowHealthCheck",
    # Retry mechanisms
    "RetryPolicy",
    "RetryStrategy",
    "RetryExecutor",
    "ExponentialBackoffStrategy",
    "create_default_retry_policy",
    "create_aggressive_retry_policy",
    # Scheduling
    "WorkflowScheduler",
    "TriggerConfig",
    "TriggerType",
    "CronTrigger",
    "IntervalTrigger",
    "EventTrigger",
    # State management
    "WorkflowState",
    "ExecutionContext",
    "TaskResult",
    # Storage and history
    "WorkflowStorage",
    "ExecutionHistory",
    "WorkflowVersion",
    "ExecutionArtifact",
]
