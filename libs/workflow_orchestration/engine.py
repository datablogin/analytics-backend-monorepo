"""Workflow execution engine and orchestrator."""

import asyncio
import uuid
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime
from typing import Any

import structlog
from celery import Celery

from .dag import DAG, Task, TaskStatus
from .retry import RetryExecutor, RetryPolicy, create_default_retry_policy
from .state import (
    ExecutionContext,
    TaskResult,
    WorkflowExecution,
    WorkflowState,
    WorkflowStatus,
)

logger = structlog.get_logger(__name__)


class ResourcePool:
    """Manages resource allocation for task execution."""

    def __init__(
        self, max_cpu: float = 10.0, max_memory_mb: int = 8192, max_concurrent: int = 20
    ):
        self.max_cpu = max_cpu
        self.max_memory_mb = max_memory_mb
        self.max_concurrent = max_concurrent

        self.allocated_cpu = 0.0
        self.allocated_memory_mb = 0
        self.running_tasks = 0
        self.task_allocations: dict[str, dict[str, float]] = {}
        self._lock = asyncio.Lock()

    async def can_allocate(self, task_name: str, cpu: float, memory_mb: int) -> bool:
        """Check if resources can be allocated for task."""
        async with self._lock:
            return (
                self.running_tasks < self.max_concurrent
                and self.allocated_cpu + cpu <= self.max_cpu
                and self.allocated_memory_mb + memory_mb <= self.max_memory_mb
            )

    async def allocate(self, task_name: str, cpu: float, memory_mb: int) -> bool:
        """Allocate resources for task."""
        async with self._lock:
            if await self.can_allocate(task_name, cpu, memory_mb):
                self.allocated_cpu += cpu
                self.allocated_memory_mb += memory_mb
                self.running_tasks += 1
                self.task_allocations[task_name] = {"cpu": cpu, "memory_mb": memory_mb}

                logger.info(
                    "Resources allocated",
                    task_name=task_name,
                    cpu=cpu,
                    memory_mb=memory_mb,
                    total_cpu=self.allocated_cpu,
                    total_memory=self.allocated_memory_mb,
                    running_tasks=self.running_tasks,
                )
                return True
            return False

    async def deallocate(self, task_name: str) -> None:
        """Deallocate resources for task."""
        async with self._lock:
            if task_name in self.task_allocations:
                allocation = self.task_allocations[task_name]
                self.allocated_cpu -= allocation["cpu"]
                self.allocated_memory_mb -= allocation["memory_mb"]
                self.running_tasks -= 1
                del self.task_allocations[task_name]

                logger.info(
                    "Resources deallocated",
                    task_name=task_name,
                    remaining_cpu=self.allocated_cpu,
                    remaining_memory=self.allocated_memory_mb,
                    running_tasks=self.running_tasks,
                )

    def get_utilization(self) -> dict[str, float]:
        """Get current resource utilization."""
        return {
            "cpu_utilization": (self.allocated_cpu / self.max_cpu) * 100
            if self.max_cpu > 0
            else 0,
            "memory_utilization": (self.allocated_memory_mb / self.max_memory_mb) * 100
            if self.max_memory_mb > 0
            else 0,
            "task_utilization": (self.running_tasks / self.max_concurrent) * 100
            if self.max_concurrent > 0
            else 0,
        }


class WorkflowExecutor:
    """Executes individual workflow tasks."""

    def __init__(self, celery_app: Celery | None = None):
        self.celery_app = celery_app
        self.function_registry: dict[str, Callable] = {}
        self.thread_pool = ThreadPoolExecutor(max_workers=10)

    def register_function(self, name: str, func: Callable) -> None:
        """Register function for task execution."""
        self.function_registry[name] = func
        logger.info("Function registered", function_name=name)

    def unregister_function(self, name: str) -> None:
        """Unregister function."""
        if name in self.function_registry:
            del self.function_registry[name]
            logger.info("Function unregistered", function_name=name)

    async def execute_task(
        self,
        task: Task,
        context: ExecutionContext,
        retry_policy: RetryPolicy | None = None,
    ) -> TaskResult:
        """Execute single task with retry logic."""
        task_start = datetime.now(UTC)

        logger.info(
            "Starting task execution",
            task_name=task.config.name,
            function_name=task.config.function_name,
            workflow_id=context.workflow_id,
        )

        # Create retry executor
        retry_executor = RetryExecutor(retry_policy or create_default_retry_policy())

        try:
            # Update task status
            task.status = TaskStatus.RUNNING
            task.start_time = task_start

            # Get function to execute
            func = self.function_registry.get(task.config.function_name)
            if not func:
                raise ValueError(
                    f"Function '{task.config.function_name}' not found in registry"
                )

            # Prepare function parameters
            func_kwargs = {
                **task.config.parameters,
                "context": context.model_dump(),
                "task_config": task.config.model_dump(),
                "upstream_results": context.get_upstream_results(
                    list(task.upstream_tasks)
                ),
            }

            # Execute with retry logic
            result_data = await retry_executor.execute_with_retry(
                func,
                task_name=task.config.name,
                context=context.model_dump(),
                **func_kwargs,
            )

            # Mark task as successful
            task.status = TaskStatus.SUCCESS
            task.end_time = datetime.now(UTC)

            # Create task result
            task_result = TaskResult(
                task_name=task.config.name,
                status=TaskStatus.SUCCESS.value,
                result_data=result_data
                if isinstance(result_data, dict)
                else {"result": result_data},
                start_time=task_start,
                end_time=task.end_time,
                retry_count=0,  # TODO: Get from retry executor
                metadata={
                    "task_type": task.config.task_type.value,
                    "function_name": task.config.function_name,
                    "resources": task.config.resources.model_dump(),
                },
            )
            task_result.calculate_duration()

            logger.info(
                "Task executed successfully",
                task_name=task.config.name,
                duration=task_result.duration_seconds,
                workflow_id=context.workflow_id,
            )

            return task_result

        except Exception as error:
            task.status = TaskStatus.FAILED
            task.end_time = datetime.now(UTC)
            task.error_message = str(error)

            # Create failed task result
            task_result = TaskResult(
                task_name=task.config.name,
                status=TaskStatus.FAILED.value,
                error_message=str(error),
                start_time=task_start,
                end_time=task.end_time,
                retry_count=0,  # TODO: Get from retry executor
                metadata={
                    "task_type": task.config.task_type.value,
                    "function_name": task.config.function_name,
                    "error_type": type(error).__name__,
                },
            )
            task_result.calculate_duration()

            logger.error(
                "Task execution failed",
                task_name=task.config.name,
                error=str(error),
                duration=task_result.duration_seconds,
                workflow_id=context.workflow_id,
            )

            return task_result

    def shutdown(self) -> None:
        """Shutdown executor and cleanup resources."""
        self.thread_pool.shutdown(wait=True)
        logger.info("Workflow executor shutdown completed")


class WorkflowEngine:
    """Main workflow orchestration engine."""

    def __init__(
        self,
        celery_app: Celery | None = None,
        max_cpu: float = 10.0,
        max_memory_mb: int = 8192,
        max_concurrent_tasks: int = 20,
    ):
        self.celery_app = celery_app
        self.executor = WorkflowExecutor(celery_app)
        self.resource_pool = ResourcePool(max_cpu, max_memory_mb, max_concurrent_tasks)
        self.workflow_state = WorkflowState()

        # Running workflow management
        self.running_workflows: dict[str, asyncio.Task] = {}
        self.workflow_dags: dict[str, DAG] = {}

        logger.info(
            "Workflow engine initialized",
            max_cpu=max_cpu,
            max_memory_mb=max_memory_mb,
            max_concurrent_tasks=max_concurrent_tasks,
        )

    def register_workflow(self, dag: DAG) -> None:
        """Register workflow DAG."""
        # Validate DAG
        issues = dag.validate()
        if issues:
            raise ValueError(f"DAG validation failed: {', '.join(issues)}")

        workflow_key = f"{dag.definition.name}:{dag.definition.version}"
        self.workflow_dags[workflow_key] = dag

        logger.info(
            "Workflow registered",
            workflow_name=dag.definition.name,
            version=dag.definition.version,
            task_count=len(dag.tasks),
        )

    def register_task_function(self, name: str, func: Callable) -> None:
        """Register function for task execution."""
        self.executor.register_function(name, func)

    async def execute_workflow(
        self,
        workflow_name: str,
        workflow_version: str,
        parameters: dict[str, Any] | None = None,
        user_id: str | None = None,
        trigger_type: str = "manual",
    ) -> str:
        """Execute workflow asynchronously."""
        # Get workflow DAG
        workflow_key = f"{workflow_name}:{workflow_version}"
        dag = self.workflow_dags.get(workflow_key)
        if not dag:
            raise ValueError(f"Workflow '{workflow_key}' not found")

        # Create execution context
        execution_id = str(uuid.uuid4())
        context = ExecutionContext(
            workflow_id=execution_id,
            workflow_name=workflow_name,
            execution_date=datetime.now(UTC),
            parameters=parameters or {},
            user_id=user_id,
            trigger_type=trigger_type,
        )

        # Create execution state
        execution = self.workflow_state.create_execution(
            execution_id=execution_id,
            workflow_name=workflow_name,
            workflow_version=workflow_version,
            context=context,
        )

        # Set task count
        execution.total_tasks = len(dag.tasks)

        # Start workflow execution
        workflow_task = asyncio.create_task(self._execute_dag(dag, execution, context))
        self.running_workflows[execution_id] = workflow_task

        logger.info(
            "Workflow execution started",
            workflow_name=workflow_name,
            version=workflow_version,
            execution_id=execution_id,
            user_id=user_id,
            trigger_type=trigger_type,
        )

        return execution_id

    async def _execute_dag(
        self, dag: DAG, execution: WorkflowExecution, context: ExecutionContext
    ) -> None:
        """Execute DAG with dependency management."""
        execution.mark_started()
        completed_tasks: set[str] = set()
        failed_tasks: set[str] = set()
        running_tasks: dict[str, asyncio.Task] = {}

        try:
            while len(completed_tasks) + len(failed_tasks) < len(dag.tasks):
                # Get tasks ready for execution
                ready_tasks = []
                for task_name, task in dag.tasks.items():
                    if (
                        task_name not in completed_tasks
                        and task_name not in failed_tasks
                        and task_name not in running_tasks
                        and task.status == TaskStatus.PENDING
                    ):
                        # Check if all upstream tasks are completed
                        upstream_completed = all(
                            upstream in completed_tasks
                            for upstream in task.upstream_tasks
                        )

                        if upstream_completed:
                            # Check conditional execution
                            if task.should_execute(context.model_dump()):
                                ready_tasks.append(task_name)
                            else:
                                # Skip task due to condition
                                task.status = TaskStatus.SKIPPED
                                completed_tasks.add(task_name)
                                logger.info(
                                    "Task skipped due to condition", task_name=task_name
                                )

                # Start ready tasks (respecting resource limits)
                for task_name in ready_tasks:
                    task = dag.tasks[task_name]

                    # Check resource availability
                    can_allocate = await self.resource_pool.can_allocate(
                        task_name,
                        task.config.resources.cpu,
                        task.config.resources.memory_mb,
                    )

                    if can_allocate:
                        # Allocate resources
                        allocated = await self.resource_pool.allocate(
                            task_name,
                            task.config.resources.cpu,
                            task.config.resources.memory_mb,
                        )

                        if allocated:
                            # Start task execution
                            task_execution = asyncio.create_task(
                                self._execute_task_with_cleanup(
                                    task, context, task_name
                                )
                            )
                            running_tasks[task_name] = task_execution

                            self.workflow_state.update_task_state(
                                execution.execution_id,
                                task_name,
                                TaskStatus.RUNNING.value,
                            )

                # Wait for at least one task to complete
                if running_tasks:
                    done, pending = await asyncio.wait(
                        list(running_tasks.values()),
                        return_when=asyncio.FIRST_COMPLETED,
                    )

                    # Process completed tasks
                    for completed_task in done:
                        task_result = await completed_task
                        task_name = task_result.task_name

                        # Remove from running tasks
                        if task_name in running_tasks:
                            del running_tasks[task_name]

                        # Deallocate resources
                        await self.resource_pool.deallocate(task_name)

                        # Update execution state
                        self.workflow_state.add_task_result(
                            execution.execution_id, task_result
                        )

                        if task_result.status == TaskStatus.SUCCESS.value:
                            completed_tasks.add(task_name)
                            self.workflow_state.update_task_state(
                                execution.execution_id,
                                task_name,
                                TaskStatus.SUCCESS.value,
                            )
                        else:
                            failed_tasks.add(task_name)
                            self.workflow_state.update_task_state(
                                execution.execution_id,
                                task_name,
                                TaskStatus.FAILED.value,
                            )

                            # Check if workflow should fail fast
                            if (
                                dag.definition.retry_policy
                                and dag.definition.retry_policy.get("fail_fast", False)
                            ):
                                logger.warning(
                                    "Workflow failing fast due to task failure",
                                    failed_task=task_name,
                                    execution_id=execution.execution_id,
                                )
                                break

                # Small delay to prevent busy waiting
                if not running_tasks and not ready_tasks:
                    await asyncio.sleep(0.1)

            # Determine final workflow status
            workflow_success = len(failed_tasks) == 0

            # Complete workflow execution
            self.workflow_state.complete_execution(
                execution.execution_id,
                success=workflow_success,
                error_message=f"Failed tasks: {list(failed_tasks)}"
                if failed_tasks
                else None,
            )

            logger.info(
                "Workflow execution completed",
                execution_id=execution.execution_id,
                status="success" if workflow_success else "failed",
                completed_tasks=len(completed_tasks),
                failed_tasks=len(failed_tasks),
                duration=execution.duration_seconds,
            )

        except Exception as error:
            # Handle workflow execution error
            logger.error(
                "Workflow execution error",
                execution_id=execution.execution_id,
                error=str(error),
            )

            # Cancel running tasks
            for asyncio_task in running_tasks.values():
                asyncio_task.cancel()

            # Complete with failure
            self.workflow_state.complete_execution(
                execution.execution_id, success=False, error_message=str(error)
            )

        finally:
            # Clean up
            if execution.execution_id in self.running_workflows:
                del self.running_workflows[execution.execution_id]

    async def _execute_task_with_cleanup(
        self, task: Task, context: ExecutionContext, task_name: str
    ) -> TaskResult:
        """Execute task with resource cleanup."""
        try:
            # Execute task
            result = await self.executor.execute_task(task, context)
            return result
        except Exception as error:
            # Create failed result
            return TaskResult(
                task_name=task_name,
                status=TaskStatus.FAILED.value,
                error_message=str(error),
                start_time=datetime.now(UTC),
                end_time=datetime.now(UTC),
            )

    async def cancel_workflow(self, execution_id: str) -> bool:
        """Cancel running workflow."""
        if execution_id in self.running_workflows:
            workflow_task = self.running_workflows[execution_id]
            workflow_task.cancel()

            # Mark as cancelled in state
            self.workflow_state.cancel_execution(execution_id)

            logger.info("Workflow cancelled", execution_id=execution_id)
            return True
        return False

    def get_workflow_status(self, execution_id: str) -> dict[str, Any] | None:
        """Get workflow execution status."""
        return self.workflow_state.get_execution_summary(execution_id)

    def list_workflows(
        self,
        workflow_name: str | None = None,
        status: WorkflowStatus | None = None,
        limit: int = 100,
    ) -> list[dict[str, Any]]:
        """List workflow executions."""
        return self.workflow_state.list_executions(workflow_name, status, limit)

    def get_resource_utilization(self) -> dict[str, float]:
        """Get current resource utilization."""
        return self.resource_pool.get_utilization()

    async def shutdown(self) -> None:
        """Shutdown workflow engine."""
        # Cancel all running workflows
        for execution_id, workflow_task in self.running_workflows.items():
            workflow_task.cancel()
            logger.info("Cancelling workflow on shutdown", execution_id=execution_id)

        # Wait for cancellations to complete
        if self.running_workflows:
            await asyncio.gather(
                *self.running_workflows.values(), return_exceptions=True
            )

        # Shutdown executor
        self.executor.shutdown()

        logger.info("Workflow engine shutdown completed")
