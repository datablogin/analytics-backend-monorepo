"""Comprehensive tests for the workflow orchestration system."""

import asyncio
from datetime import UTC, datetime, timedelta
from unittest.mock import AsyncMock, Mock, patch

import pytest

from libs.workflow_orchestration import (
    DAG,
    AlertRule,
    CronTrigger,
    EventTrigger,
    ExecutionArtifact,
    ExecutionContext,
    ExecutionHistory,
    ExecutionMetrics,
    ExponentialBackoffStrategy,
    IntervalTrigger,
    ResourceRequirement,
    RetryExecutor,
    RetryPolicy,
    Task,
    TaskConfig,
    TaskResult,
    TaskStatus,
    TaskType,
    TriggerConfig,
    TriggerType,
    WorkflowDefinition,
    WorkflowEngine,
    WorkflowMonitor,
    WorkflowScheduler,
    WorkflowState,
    WorkflowStorage,
    WorkflowVersion,
)


class TestDAG:
    """Test DAG workflow definition functionality."""

    def test_dag_creation(self):
        """Test DAG creation and basic operations."""
        definition = WorkflowDefinition(
            name="test_workflow",
            version="1.0.0",
            description="Test workflow",
            owner="test_user",
            max_parallel_tasks=5,
        )

        dag = DAG(definition)
        assert dag.definition.name == "test_workflow"
        assert len(dag.tasks) == 0

    def test_task_addition(self):
        """Test adding tasks to DAG."""
        definition = WorkflowDefinition(
            name="test_workflow", version="1.0.0", owner="test_user"
        )
        dag = DAG(definition)

        task_config = TaskConfig(
            name="test_task",
            function_name="test_function",
            task_type=TaskType.DATA_INGESTION,
        )
        task = Task(task_config)

        dag.add_task(task)
        assert len(dag.tasks) == 1
        assert "test_task" in dag.tasks

    def test_task_dependencies(self):
        """Test setting task dependencies."""
        definition = WorkflowDefinition(
            name="test_workflow", version="1.0.0", owner="test_user"
        )
        dag = DAG(definition)

        # Create two tasks
        task1_config = TaskConfig(name="task1", function_name="func1")
        task2_config = TaskConfig(name="task2", function_name="func2")

        task1 = Task(task1_config)
        task2 = Task(task2_config)

        dag.add_task(task1)
        dag.add_task(task2)

        # Set dependency
        dag.set_dependency("task1", "task2")

        assert "task2" in dag.tasks["task1"].downstream_tasks
        assert "task1" in dag.tasks["task2"].upstream_tasks

    def test_cycle_detection(self):
        """Test cycle detection in DAG."""
        definition = WorkflowDefinition(
            name="test_workflow", version="1.0.0", owner="test_user"
        )
        dag = DAG(definition)

        # Create three tasks
        for i in range(3):
            task_config = TaskConfig(name=f"task{i}", function_name=f"func{i}")
            task = Task(task_config)
            dag.add_task(task)

        # Set up dependencies
        dag.set_dependency("task0", "task1")
        dag.set_dependency("task1", "task2")

        # Try to create a cycle - this should fail
        with pytest.raises(ValueError, match="cycle"):
            dag.set_dependency("task2", "task0")

    def test_topological_sort(self):
        """Test topological sorting of tasks."""
        definition = WorkflowDefinition(
            name="test_workflow", version="1.0.0", owner="test_user"
        )
        dag = DAG(definition)

        # Create tasks with dependencies: A -> B -> C, A -> C
        for name in ["A", "B", "C"]:
            task_config = TaskConfig(name=name, function_name=f"func_{name}")
            task = Task(task_config)
            dag.add_task(task)

        dag.set_dependency("A", "B")
        dag.set_dependency("B", "C")
        dag.set_dependency("A", "C")

        sorted_tasks = dag.topological_sort()

        # A should come before B and C, B should come before C
        a_index = sorted_tasks.index("A")
        b_index = sorted_tasks.index("B")
        c_index = sorted_tasks.index("C")

        assert a_index < b_index
        assert a_index < c_index
        assert b_index < c_index

    def test_ready_tasks(self):
        """Test getting tasks ready for execution."""
        definition = WorkflowDefinition(
            name="test_workflow", version="1.0.0", owner="test_user"
        )
        dag = DAG(definition)

        # Create tasks: A -> B, C (independent)
        for name in ["A", "B", "C"]:
            task_config = TaskConfig(name=name, function_name=f"func_{name}")
            task = Task(task_config)
            dag.add_task(task)

        dag.set_dependency("A", "B")

        # Initially, A and C should be ready (no dependencies)
        ready_tasks = dag.get_ready_tasks()
        assert set(ready_tasks) == {"A", "C"}

        # Complete A, then B should be ready
        dag.tasks["A"].status = TaskStatus.SUCCESS
        ready_tasks = dag.get_ready_tasks()
        assert "B" in ready_tasks

    def test_dag_validation(self):
        """Test DAG validation."""
        definition = WorkflowDefinition(
            name="test_workflow", version="1.0.0", owner="test_user"
        )
        dag = DAG(definition)

        # Add task with invalid upstream reference
        task_config = TaskConfig(name="test_task", function_name="test_func")
        task = Task(task_config, upstream_tasks=["nonexistent_task"])
        dag.add_task(task)

        issues = dag.validate()
        assert len(issues) > 0
        assert "nonexistent_task" in issues[0]


class TestWorkflowState:
    """Test workflow state management."""

    def test_execution_creation(self):
        """Test creating workflow execution."""
        state = WorkflowState()

        execution_id = "test_execution_123"
        context = ExecutionContext(
            workflow_id=execution_id,
            workflow_name="test_workflow",
            execution_date=datetime.now(UTC),
        )

        execution = state.create_execution(
            execution_id=execution_id,
            workflow_name="test_workflow",
            workflow_version="1.0.0",
            context=context,
        )

        assert execution.execution_id == execution_id
        assert execution.workflow_name == "test_workflow"
        assert execution.status.value == "pending"

    def test_task_result_addition(self):
        """Test adding task results to execution."""
        state = WorkflowState()

        execution_id = "test_execution_123"
        context = ExecutionContext(
            workflow_id=execution_id,
            workflow_name="test_workflow",
            execution_date=datetime.now(UTC),
        )

        state.create_execution(
            execution_id=execution_id,
            workflow_name="test_workflow",
            workflow_version="1.0.0",
            context=context,
        )

        task_result = TaskResult(
            task_name="test_task",
            status="success",
            start_time=datetime.now(UTC),
            result_data={"output": "test_output"},
        )

        state.add_task_result(execution_id, task_result)

        retrieved_execution = state.get_execution(execution_id)
        assert "test_task" in retrieved_execution.context.task_results
        assert (
            retrieved_execution.context.task_results["test_task"].result_data["output"]
            == "test_output"
        )


class TestRetryMechanism:
    """Test retry mechanisms and backoff strategies."""

    def test_exponential_backoff_strategy(self):
        """Test exponential backoff calculation."""
        strategy = ExponentialBackoffStrategy(
            multiplier=2.0, max_delay=60.0, jitter=False
        )

        # Test delay calculation for different attempts
        delay1 = strategy.calculate_delay(1, 1.0)
        delay2 = strategy.calculate_delay(2, 1.0)
        delay3 = strategy.calculate_delay(3, 1.0)

        assert delay1 == 1.0  # 1.0 * 2^0
        assert delay2 == 2.0  # 1.0 * 2^1
        assert delay3 == 4.0  # 1.0 * 2^2

        # Test max delay limit
        delay_large = strategy.calculate_delay(10, 1.0)
        assert delay_large <= 60.0

    def test_retry_policy_should_retry(self):
        """Test retry policy decision logic."""
        policy = RetryPolicy(
            max_attempts=3,
            retry_condition="transient_errors",
            retryable_errors=["ConnectionError", "TimeoutError"],
        )

        # Test with retryable error
        connection_error = ConnectionError("Connection failed")
        assert policy.should_retry(connection_error, 1)
        assert not policy.should_retry(connection_error, 3)  # Max attempts reached

        # Test with non-retryable error
        value_error = ValueError("Invalid value")
        assert not policy.should_retry(value_error, 1)

    @pytest.mark.asyncio
    async def test_retry_executor(self):
        """Test retry executor with mock function."""
        policy = RetryPolicy(max_attempts=3, base_delay_seconds=0.1)
        executor = RetryExecutor(policy)

        # Create mock function that fails twice then succeeds
        call_count = 0

        async def mock_function():
            nonlocal call_count
            call_count += 1
            if call_count <= 2:
                raise ConnectionError("Connection failed")
            return "success"

        result, retry_count = await executor.execute_with_retry(
            mock_function, task_name="test_task"
        )

        assert result == "success"
        assert retry_count == 2  # Failed twice, succeeded on third attempt
        assert call_count == 3  # Failed twice, succeeded on third attempt


class TestScheduling:
    """Test workflow scheduling and triggers."""

    def test_cron_trigger_creation(self):
        """Test creating cron-based trigger."""
        trigger = CronTrigger(
            name="daily_trigger",
            workflow_name="test_workflow",
            workflow_version="1.0.0",
            cron_expression="0 2 * * *",  # Daily at 2 AM
        )

        assert trigger.name == "daily_trigger"
        assert trigger.workflow_name == "test_workflow"
        assert trigger.cron_expression == "0 2 * * *"

        # Test next run time calculation
        next_run = trigger.get_next_run_time()
        assert next_run is not None
        assert isinstance(next_run, datetime)

    def test_interval_trigger(self):
        """Test interval-based trigger."""
        start_time = datetime.now(UTC)
        trigger = IntervalTrigger(
            name="interval_trigger",
            workflow_name="test_workflow",
            workflow_version="1.0.0",
            interval_seconds=3600,  # 1 hour
            start_time=start_time,
        )

        # Test next run time
        next_run = trigger.get_next_run_time()
        expected_next = start_time + timedelta(seconds=3600)

        assert abs((next_run - expected_next).total_seconds()) < 1

    def test_event_trigger(self):
        """Test event-driven trigger."""
        trigger = EventTrigger(
            name="event_trigger",
            workflow_name="test_workflow",
            workflow_version="1.0.0",
            event_type="data_arrival",
            conditions={"source": "database"},
        )

        trigger.activate()

        # Test matching event
        matching_event = {
            "type": "data_arrival",
            "source": "database",
            "timestamp": datetime.now(UTC).isoformat(),
        }

        assert trigger.add_event(matching_event)
        assert len(trigger.pending_events) == 1

        # Test non-matching event
        non_matching_event = {
            "type": "data_arrival",
            "source": "api",  # Different source
            "timestamp": datetime.now(UTC).isoformat(),
        }

        assert not trigger.add_event(non_matching_event)
        assert len(trigger.pending_events) == 1  # Still only one event

    @pytest.mark.asyncio
    async def test_scheduler_integration(self):
        """Test workflow scheduler integration."""
        # Mock workflow engine
        mock_engine = Mock()
        mock_engine.execute_workflow = AsyncMock(return_value="execution_123")

        scheduler = WorkflowScheduler(mock_engine)

        # Create and add a trigger
        trigger_config = TriggerConfig(
            name="test_trigger",
            trigger_type=TriggerType.INTERVAL,
            workflow_name="test_workflow",
            workflow_version="1.0.0",
            interval_seconds=60,  # 60 seconds minimum
            enabled=True,
        )

        trigger = scheduler.create_trigger(trigger_config)
        assert trigger.status.value == "active"


class TestMonitoring:
    """Test workflow monitoring and metrics."""

    def test_execution_metrics_calculation(self):
        """Test execution metrics calculation."""
        # Create mock workflow state
        mock_state = Mock()

        # Mock execution data
        mock_executions = [
            {
                "execution_id": "exec1",
                "workflow_name": "test_workflow",
                "status": "success",
                "start_time": datetime.now(UTC).isoformat(),
                "duration_seconds": 120.0,
                "total_tasks": 5,
                "completed_tasks": 5,
                "failed_tasks": 0,
            },
            {
                "execution_id": "exec2",
                "workflow_name": "test_workflow",
                "status": "failed",
                "start_time": datetime.now(UTC).isoformat(),
                "duration_seconds": 60.0,
                "total_tasks": 5,
                "completed_tasks": 3,
                "failed_tasks": 2,
                "error_message": "Task failed",
            },
        ]

        mock_state.list_executions.return_value = mock_executions

        monitor = WorkflowMonitor(mock_state)

        # Test metrics calculation
        with patch.object(monitor, "_calculate_metrics") as mock_calc:
            mock_metrics = ExecutionMetrics(
                total_executions=2,
                successful_executions=1,
                failed_executions=1,
                success_rate=50.0,
                avg_duration_seconds=90.0,
                metrics_period_start=datetime.now(UTC) - timedelta(hours=24),
                metrics_period_end=datetime.now(UTC),
            )
            mock_calc.return_value = mock_metrics

            metrics = monitor.get_workflow_metrics("test_workflow", period_hours=24)

            assert metrics.total_executions == 2
            assert metrics.success_rate == 50.0
            assert metrics.avg_duration_seconds == 90.0

    def test_health_check(self):
        """Test workflow health check."""
        # Create mock monitor with test data
        mock_state = Mock()
        mock_state.list_executions.return_value = []

        monitor = WorkflowMonitor(mock_state)

        # Create test metrics
        metrics = ExecutionMetrics(
            total_executions=10,
            successful_executions=9,
            failed_executions=1,
            success_rate=90.0,
            avg_duration_seconds=300.0,
            task_success_rate=95.0,
            metrics_period_start=datetime.now(UTC) - timedelta(hours=24),
            metrics_period_end=datetime.now(UTC),
        )

        # Mock the metrics method
        with patch.object(monitor, "get_workflow_metrics", return_value=metrics):
            health_check = monitor.perform_health_check("test_workflow", "1.0.0")

            assert health_check.workflow_name == "test_workflow"
            assert (
                health_check.health_status == "healthy"
            )  # 90% success rate is healthy
            assert health_check.recent_success_rate == 90.0

    def test_alert_rule_evaluation(self):
        """Test alert rule evaluation."""
        # Create mock state with failing executions
        mock_state = Mock()
        failing_executions = [
            {
                "execution_id": f"exec{i}",
                "workflow_name": "test_workflow",
                "status": "failed",
                "start_time": (datetime.now(UTC) - timedelta(minutes=10)).isoformat(),
                "error_message": "Connection error",
            }
            for i in range(5)  # 5 failures
        ]
        mock_state.list_executions.return_value = failing_executions

        monitor = WorkflowMonitor(mock_state)

        # Create alert rule for high error count
        alert_rule = AlertRule(
            name="high_error_rate",
            workflow_name="test_workflow",
            error_count_threshold=3,
            evaluation_period_minutes=30,
            severity="error",
        )

        # Test alert evaluation
        alert = monitor._evaluate_alert_rule(alert_rule, datetime.now(UTC))

        assert alert is not None
        assert alert.rule_name == "high_error_rate"
        assert alert.severity == "error"
        assert "error_count" in alert.triggered_conditions


class TestStorage:
    """Test workflow storage and versioning."""

    def test_workflow_version_storage(self):
        """Test storing and retrieving workflow versions."""
        storage = WorkflowStorage()

        # Create workflow version
        version = WorkflowVersion(
            name="test_workflow",
            version="1.0.0",
            definition={"tasks": [], "dependencies": []},
            created_by="test_user",
            changelog="Initial version",
        )

        # Store version
        version_key = storage.store_workflow_version(version)
        assert version_key == "test_workflow:1.0.0"

        # Retrieve version
        retrieved_version = storage.get_workflow_version("test_workflow", "1.0.0")
        assert retrieved_version is not None
        assert retrieved_version.name == "test_workflow"
        assert retrieved_version.version == "1.0.0"
        assert retrieved_version.created_by == "test_user"

    def test_execution_history_storage(self):
        """Test storing execution history."""
        storage = WorkflowStorage()

        history = ExecutionHistory(
            execution_id="exec_123",
            workflow_name="test_workflow",
            workflow_version="1.0.0",
            status="success",
            trigger_type="manual",
            input_parameters={"param1": "value1"},
            started_at=datetime.now(UTC),
            completed_at=datetime.now(UTC),
            duration_seconds=120.0,
        )

        # Store history
        stored_id = storage.store_execution_history(history)
        assert stored_id == "exec_123"

        # Retrieve history
        retrieved_history = storage.get_execution_history("exec_123")
        assert retrieved_history is not None
        assert retrieved_history.execution_id == "exec_123"
        assert retrieved_history.status == "success"

    def test_execution_artifact_storage(self):
        """Test storing execution artifacts."""
        storage = WorkflowStorage()

        artifact = ExecutionArtifact(
            artifact_id="artifact_123",
            execution_id="exec_123",
            task_name="test_task",
            artifact_type="data",
            name="output_data.csv",
            storage_path="/tmp/output_data.csv",
            size_bytes=1024,
            mime_type="text/csv",
        )

        # Store artifact
        stored_id = storage.store_execution_artifact(artifact)
        assert stored_id == "artifact_123"

        # Retrieve artifact
        retrieved_artifact = storage.get_execution_artifact("artifact_123")
        assert retrieved_artifact is not None
        assert retrieved_artifact.artifact_type == "data"
        assert retrieved_artifact.size_bytes == 1024

    def test_workflow_version_deprecation(self):
        """Test workflow version deprecation."""
        storage = WorkflowStorage()

        # Store a version
        version = WorkflowVersion(
            name="test_workflow",
            version="1.0.0",
            definition={"tasks": []},
            created_by="test_user",
        )
        storage.store_workflow_version(version)

        # Deprecate the version
        success = storage.deprecate_workflow_version("test_workflow", "1.0.0")
        assert success

        # Verify it's deprecated
        retrieved_version = storage.get_workflow_version("test_workflow", "1.0.0")
        assert retrieved_version.is_deprecated

    def test_storage_statistics(self):
        """Test storage statistics generation."""
        storage = WorkflowStorage()

        # Add some test data
        version = WorkflowVersion(
            name="test_workflow",
            version="1.0.0",
            definition={"tasks": []},
            created_by="test_user",
        )
        storage.store_workflow_version(version)

        history = ExecutionHistory(
            execution_id="exec_123",
            workflow_name="test_workflow",
            workflow_version="1.0.0",
            status="success",
            trigger_type="manual",
        )
        storage.store_execution_history(history)

        # Get statistics
        stats = storage.get_storage_statistics()

        assert stats["workflows"]["total_workflows"] == 1
        assert stats["workflows"]["total_versions"] == 1
        assert stats["executions"]["total_executions"] == 1
        assert "storage_backend" in stats
        assert "last_updated" in stats


@pytest.mark.asyncio
class TestWorkflowEngine:
    """Test complete workflow engine functionality."""

    async def test_workflow_registration(self):
        """Test workflow registration and validation."""
        engine = WorkflowEngine()

        # Create simple workflow
        definition = WorkflowDefinition(
            name="test_workflow", version="1.0.0", owner="test_user"
        )
        dag = DAG(definition)

        task_config = TaskConfig(
            name="test_task", function_name="test_function", task_type=TaskType.CUSTOM
        )
        task = Task(task_config)
        dag.add_task(task)

        # Register workflow
        engine.register_workflow(dag)

        # Verify registration
        workflow_key = f"{definition.name}:{definition.version}"
        assert workflow_key in engine.workflow_dags

    async def test_task_function_registration(self):
        """Test task function registration."""
        engine = WorkflowEngine()

        # Register a test function
        async def test_function(**kwargs):
            return {"result": "test_output"}

        engine.register_task_function("test_function", test_function)

        # Verify registration
        assert "test_function" in engine.executor.function_registry

    async def test_workflow_execution(self):
        """Test complete workflow execution."""
        engine = WorkflowEngine()

        # Register a test function
        async def test_function(**kwargs):
            return {"result": "success", "processed_items": 100}

        engine.register_task_function("test_function", test_function)

        # Create and register workflow
        definition = WorkflowDefinition(
            name="test_workflow", version="1.0.0", owner="test_user"
        )
        dag = DAG(definition)

        task_config = TaskConfig(
            name="test_task",
            function_name="test_function",
            task_type=TaskType.CUSTOM,
            resources=ResourceRequirement(cpu=0.1, memory_mb=128, timeout_seconds=60),
        )
        task = Task(task_config)
        dag.add_task(task)

        engine.register_workflow(dag)

        # Execute workflow
        execution_id = await engine.execute_workflow(
            workflow_name="test_workflow",
            workflow_version="1.0.0",
            parameters={"input_param": "test_value"},
        )

        assert execution_id is not None

        # Wait for completion (with timeout)
        max_wait_time = 30  # seconds
        wait_time = 0
        while wait_time < max_wait_time:
            status = engine.get_workflow_status(execution_id)
            if status and status["status"] in ["success", "failed"]:
                break
            await asyncio.sleep(0.5)
            wait_time += 0.5

        # Verify execution completed successfully
        final_status = engine.get_workflow_status(execution_id)
        assert final_status is not None
        assert final_status["status"] == "success"

        # Clean up
        await engine.shutdown()


class TestIntegration:
    """Integration tests for the complete orchestration system."""

    @pytest.mark.asyncio
    async def test_end_to_end_workflow(self):
        """Test complete end-to-end workflow execution."""
        # Initialize components
        engine = WorkflowEngine(max_concurrent_tasks=5)
        WorkflowScheduler(engine)
        monitor = WorkflowMonitor(engine.workflow_state)
        storage = WorkflowStorage()

        # Register test functions
        async def data_ingestion(**kwargs):
            await asyncio.sleep(0.1)  # Simulate work
            return {"records_ingested": 1000}

        async def data_processing(**kwargs):
            upstream_results = kwargs.get("upstream_results", {})
            ingestion_result = upstream_results.get("data_ingestion", {})
            records = ingestion_result.get("result_data", {}).get("records_ingested", 0)

            await asyncio.sleep(0.1)  # Simulate work
            return {"records_processed": records, "processing_status": "success"}

        engine.register_task_function("data_ingestion", data_ingestion)
        engine.register_task_function("data_processing", data_processing)

        # Create workflow
        definition = WorkflowDefinition(
            name="integration_test_workflow",
            version="1.0.0",
            owner="test_system",
            description="Integration test workflow",
        )
        dag = DAG(definition)

        # Add tasks
        ingestion_task = Task(
            config=TaskConfig(
                name="data_ingestion",
                function_name="data_ingestion",
                task_type=TaskType.DATA_INGESTION,
                resources=ResourceRequirement(cpu=0.5, memory_mb=256),
            )
        )

        processing_task = Task(
            config=TaskConfig(
                name="data_processing",
                function_name="data_processing",
                task_type=TaskType.DATA_TRANSFORMATION,
                resources=ResourceRequirement(cpu=1.0, memory_mb=512),
            ),
            upstream_tasks=["data_ingestion"],
        )

        dag.add_task(ingestion_task)
        dag.add_task(processing_task)
        dag.set_dependency("data_ingestion", "data_processing")

        # Register workflow
        engine.register_workflow(dag)

        # Store workflow version
        workflow_version = WorkflowVersion(
            name=definition.name,
            version=definition.version,
            definition=dag.to_dict(),
            created_by="test_system",
            changelog="Integration test version",
        )
        storage.store_workflow_version(workflow_version)

        # Execute workflow
        execution_id = await engine.execute_workflow(
            workflow_name="integration_test_workflow",
            workflow_version="1.0.0",
            parameters={"test_param": "integration_test"},
            user_id="test_user",
        )

        assert execution_id is not None

        # Wait for completion
        max_wait_time = 30  # seconds
        wait_time = 0
        while wait_time < max_wait_time:
            status = engine.get_workflow_status(execution_id)
            if status and status["status"] in ["success", "failed"]:
                break
            await asyncio.sleep(0.5)
            wait_time += 0.5

        # Verify execution
        final_status = engine.get_workflow_status(execution_id)
        assert final_status is not None
        assert final_status["status"] == "success"

        # Test monitoring
        metrics = monitor.get_workflow_metrics(
            "integration_test_workflow", force_refresh=True
        )
        assert metrics.total_executions >= 1

        # Test storage
        execution_history = ExecutionHistory(
            execution_id=execution_id,
            workflow_name="integration_test_workflow",
            workflow_version="1.0.0",
            status="success",
            trigger_type="manual",
            triggered_by="test_user",
            started_at=datetime.now(UTC),
            completed_at=datetime.now(UTC),
        )
        storage.store_execution_history(execution_history)

        retrieved_history = storage.get_execution_history(execution_id)
        assert retrieved_history is not None
        assert retrieved_history.status == "success"

        # Clean up
        await engine.shutdown()

    def test_system_overview(self):
        """Test system-wide overview and statistics."""
        # Initialize monitoring with mock data
        mock_state = Mock()
        mock_state.list_executions.return_value = [
            {
                "workflow_name": "workflow1",
                "status": "success",
                "start_time": datetime.now(UTC).isoformat(),
            },
            {
                "workflow_name": "workflow2",
                "status": "success",
                "start_time": datetime.now(UTC).isoformat(),
            },
            {
                "workflow_name": "workflow1",
                "status": "failed",
                "start_time": datetime.now(UTC).isoformat(),
            },
        ]

        monitor = WorkflowMonitor(mock_state)

        # Get system overview
        overview = monitor.get_system_overview()

        assert "total_workflows" in overview
        assert "total_executions_24h" in overview
        assert "system_success_rate_24h" in overview
        assert "active_alerts" in overview
        assert "last_updated" in overview

        # Verify calculations
        assert overview["total_workflows"] == 2  # workflow1 and workflow2
        assert overview["total_executions_24h"] == 3
        assert (
            abs(overview["system_success_rate_24h"] - 66.67) < 0.1
        )  # 2 success out of 3


if __name__ == "__main__":
    # Run tests with pytest
    pytest.main([__file__, "-v", "--tb=short"])
