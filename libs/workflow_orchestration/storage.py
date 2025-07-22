"""Workflow storage and execution history management."""

from datetime import datetime, timedelta, timezone
from typing import Any

import structlog
from pydantic import BaseModel, Field

logger = structlog.get_logger(__name__)


class WorkflowVersion(BaseModel):
    """Workflow version information."""

    name: str = Field(description="Workflow name")
    version: str = Field(description="Version identifier")
    definition: dict[str, Any] = Field(description="Workflow definition")
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    created_by: str = Field(description="User who created this version")
    changelog: str | None = Field(
        default=None, description="Changes in this version"
    )
    tags: list[str] = Field(default_factory=list, description="Version tags")

    # Status
    is_active: bool = Field(default=True, description="Whether version is active")
    is_deprecated: bool = Field(
        default=False, description="Whether version is deprecated"
    )

    # Validation
    validation_status: str = Field(default="pending", description="Validation status")
    validation_errors: list[str] = Field(
        default_factory=list, description="Validation errors"
    )

    def get_version_key(self) -> str:
        """Get unique version key."""
        return f"{self.name}:{self.version}"


class ExecutionArtifact(BaseModel):
    """Artifact produced during workflow execution."""

    artifact_id: str = Field(description="Unique artifact ID")
    execution_id: str = Field(description="Execution that produced this artifact")
    task_name: str | None = Field(
        default=None, description="Task that produced artifact"
    )

    # Artifact details
    artifact_type: str = Field(description="Type of artifact (data, log, report, etc.)")
    name: str = Field(description="Artifact name")
    description: str | None = Field(default=None, description="Artifact description")

    # Storage information
    storage_path: str = Field(description="Path where artifact is stored")
    storage_type: str = Field(default="filesystem", description="Storage backend type")
    size_bytes: int | None = Field(
        default=None, description="Artifact size in bytes"
    )
    mime_type: str | None = Field(default=None, description="MIME type")

    # Metadata
    metadata: dict[str, Any] = Field(
        default_factory=dict, description="Additional metadata"
    )
    checksum: str | None = Field(default=None, description="Artifact checksum")

    # Timing
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    expires_at: datetime | None = Field(default=None, description="Expiration time")

    # Access control
    is_public: bool = Field(
        default=False, description="Whether artifact is publicly accessible"
    )
    access_permissions: list[str] = Field(
        default_factory=list, description="Access permissions"
    )


class ExecutionHistory(BaseModel):
    """Complete execution history record."""

    execution_id: str = Field(description="Unique execution ID")
    workflow_name: str = Field(description="Workflow name")
    workflow_version: str = Field(description="Workflow version")

    # Execution details
    status: str = Field(description="Execution status")
    trigger_type: str = Field(description="What triggered the execution")
    triggered_by: str | None = Field(
        default=None, description="User or system that triggered"
    )

    # Parameters and context
    input_parameters: dict[str, Any] = Field(
        default_factory=dict, description="Input parameters"
    )
    execution_context: dict[str, Any] = Field(
        default_factory=dict, description="Execution context"
    )

    # Timing information
    scheduled_at: datetime | None = Field(
        default=None, description="When execution was scheduled"
    )
    started_at: datetime | None = Field(
        default=None, description="When execution started"
    )
    completed_at: datetime | None = Field(
        default=None, description="When execution completed"
    )
    duration_seconds: float | None = Field(
        default=None, description="Execution duration"
    )

    # Results and outputs
    output_data: dict[str, Any] | None = Field(
        default=None, description="Execution outputs"
    )
    error_message: str | None = Field(
        default=None, description="Error message if failed"
    )

    # Task execution details
    task_results: dict[str, dict[str, Any]] = Field(
        default_factory=dict, description="Individual task results"
    )
    task_timeline: list[dict[str, Any]] = Field(
        default_factory=list, description="Task execution timeline"
    )

    # Performance metrics
    resource_usage: dict[str, Any] = Field(
        default_factory=dict, description="Resource utilization"
    )
    performance_metrics: dict[str, Any] = Field(
        default_factory=dict, description="Performance metrics"
    )

    # Artifacts and outputs
    artifacts: list[str] = Field(
        default_factory=list, description="List of artifact IDs"
    )

    # Metadata
    metadata: dict[str, Any] = Field(
        default_factory=dict, description="Additional metadata"
    )
    tags: list[str] = Field(default_factory=list, description="Execution tags")

    # Storage
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    archived_at: datetime | None = Field(
        default=None, description="When record was archived"
    )
    is_archived: bool = Field(default=False, description="Whether record is archived")


class WorkflowStorage:
    """Handles workflow definition storage and versioning."""

    def __init__(self, storage_backend: str = "memory"):
        self.storage_backend = storage_backend

        # In-memory storage (replace with persistent storage in production)
        self.workflow_versions: dict[str, WorkflowVersion] = {}
        self.execution_histories: dict[str, ExecutionHistory] = {}
        self.execution_artifacts: dict[str, ExecutionArtifact] = {}

        # Indexes for efficient querying
        self.workflows_by_name: dict[
            str, list[str]
        ] = {}  # name -> list of version keys
        self.executions_by_workflow: dict[
            str, list[str]
        ] = {}  # workflow_name -> execution_ids
        self.artifacts_by_execution: dict[
            str, list[str]
        ] = {}  # execution_id -> artifact_ids

        logger.info("Workflow storage initialized", backend=storage_backend)

    def store_workflow_version(self, version: WorkflowVersion) -> str:
        """Store workflow version."""
        version_key = version.get_version_key()

        # Store version
        self.workflow_versions[version_key] = version

        # Update indexes
        if version.name not in self.workflows_by_name:
            self.workflows_by_name[version.name] = []
        if version_key not in self.workflows_by_name[version.name]:
            self.workflows_by_name[version.name].append(version_key)

        logger.info(
            "Workflow version stored",
            workflow_name=version.name,
            version=version.version,
            version_key=version_key,
        )

        return version_key

    def get_workflow_version(
        self, name: str, version: str
    ) -> WorkflowVersion | None:
        """Get specific workflow version."""
        version_key = f"{name}:{version}"
        return self.workflow_versions.get(version_key)

    def get_latest_workflow_version(self, name: str) -> WorkflowVersion | None:
        """Get latest version of a workflow."""
        if name not in self.workflows_by_name:
            return None

        version_keys = self.workflows_by_name[name]
        if not version_keys:
            return None

        # Get all versions and find the latest active one
        versions = [self.workflow_versions[key] for key in version_keys]
        active_versions = [v for v in versions if v.is_active and not v.is_deprecated]

        if not active_versions:
            return None

        # Sort by created_at and return the latest
        active_versions.sort(key=lambda v: v.created_at, reverse=True)
        return active_versions[0]

    def list_workflow_versions(
        self, name: str | None = None, include_deprecated: bool = False
    ) -> list[WorkflowVersion]:
        """List workflow versions."""
        if name:
            if name not in self.workflows_by_name:
                return []
            version_keys = self.workflows_by_name[name]
            versions = [self.workflow_versions[key] for key in version_keys]
        else:
            versions = list(self.workflow_versions.values())

        # Filter deprecated if requested
        if not include_deprecated:
            versions = [v for v in versions if not v.is_deprecated]

        # Sort by name, then by created_at
        versions.sort(key=lambda v: (v.name, v.created_at), reverse=True)
        return versions

    def deprecate_workflow_version(self, name: str, version: str) -> bool:
        """Mark workflow version as deprecated."""
        version_key = f"{name}:{version}"
        if version_key in self.workflow_versions:
            self.workflow_versions[version_key].is_deprecated = True
            logger.info(
                "Workflow version deprecated", workflow_name=name, version=version
            )
            return True
        return False

    def delete_workflow_version(self, name: str, version: str) -> bool:
        """Delete workflow version (use with caution)."""
        version_key = f"{name}:{version}"

        if version_key not in self.workflow_versions:
            return False

        # Check if there are active executions using this version
        active_executions = self._get_active_executions_for_version(name, version)
        if active_executions:
            logger.warning(
                "Cannot delete workflow version with active executions",
                workflow_name=name,
                version=version,
                active_executions=len(active_executions),
            )
            return False

        # Remove from storage
        del self.workflow_versions[version_key]

        # Update indexes
        if name in self.workflows_by_name:
            if version_key in self.workflows_by_name[name]:
                self.workflows_by_name[name].remove(version_key)
            if not self.workflows_by_name[name]:
                del self.workflows_by_name[name]

        logger.info("Workflow version deleted", workflow_name=name, version=version)
        return True

    def store_execution_history(self, history: ExecutionHistory) -> str:
        """Store execution history."""
        # Store history
        self.execution_histories[history.execution_id] = history

        # Update indexes
        workflow_name = history.workflow_name
        if workflow_name not in self.executions_by_workflow:
            self.executions_by_workflow[workflow_name] = []
        if history.execution_id not in self.executions_by_workflow[workflow_name]:
            self.executions_by_workflow[workflow_name].append(history.execution_id)

        logger.info(
            "Execution history stored",
            execution_id=history.execution_id,
            workflow_name=workflow_name,
            status=history.status,
        )

        return history.execution_id

    def get_execution_history(self, execution_id: str) -> ExecutionHistory | None:
        """Get execution history by ID."""
        return self.execution_histories.get(execution_id)

    def list_execution_histories(
        self,
        workflow_name: str | None = None,
        status: str | None = None,
        limit: int = 100,
        include_archived: bool = False,
    ) -> list[ExecutionHistory]:
        """List execution histories with filtering."""
        if workflow_name:
            if workflow_name not in self.executions_by_workflow:
                return []
            execution_ids = self.executions_by_workflow[workflow_name]
            histories = [
                self.execution_histories[eid]
                for eid in execution_ids
                if eid in self.execution_histories
            ]
        else:
            histories = list(self.execution_histories.values())

        # Apply filters
        if status:
            histories = [h for h in histories if h.status == status]

        if not include_archived:
            histories = [h for h in histories if not h.is_archived]

        # Sort by created_at (newest first)
        histories.sort(key=lambda h: h.created_at, reverse=True)

        # Apply limit
        return histories[:limit]

    def update_execution_history(
        self, execution_id: str, updates: dict[str, Any]
    ) -> bool:
        """Update execution history."""
        if execution_id not in self.execution_histories:
            return False

        history = self.execution_histories[execution_id]

        # Update allowed fields
        allowed_fields = [
            "status",
            "completed_at",
            "duration_seconds",
            "output_data",
            "error_message",
            "task_results",
            "task_timeline",
            "resource_usage",
            "performance_metrics",
            "artifacts",
            "metadata",
            "tags",
        ]

        for field, value in updates.items():
            if field in allowed_fields:
                setattr(history, field, value)

        logger.info("Execution history updated", execution_id=execution_id)
        return True

    def archive_execution_history(self, execution_id: str) -> bool:
        """Archive execution history."""
        if execution_id not in self.execution_histories:
            return False

        history = self.execution_histories[execution_id]
        history.is_archived = True
        history.archived_at = datetime.now(timezone.utc)

        logger.info("Execution history archived", execution_id=execution_id)
        return True

    def store_execution_artifact(self, artifact: ExecutionArtifact) -> str:
        """Store execution artifact."""
        # Store artifact
        self.execution_artifacts[artifact.artifact_id] = artifact

        # Update indexes
        execution_id = artifact.execution_id
        if execution_id not in self.artifacts_by_execution:
            self.artifacts_by_execution[execution_id] = []
        if artifact.artifact_id not in self.artifacts_by_execution[execution_id]:
            self.artifacts_by_execution[execution_id].append(artifact.artifact_id)

        # Update execution history if it exists
        if execution_id in self.execution_histories:
            history = self.execution_histories[execution_id]
            if artifact.artifact_id not in history.artifacts:
                history.artifacts.append(artifact.artifact_id)

        logger.info(
            "Execution artifact stored",
            artifact_id=artifact.artifact_id,
            execution_id=execution_id,
            artifact_type=artifact.artifact_type,
        )

        return artifact.artifact_id

    def get_execution_artifact(self, artifact_id: str) -> ExecutionArtifact | None:
        """Get execution artifact by ID."""
        return self.execution_artifacts.get(artifact_id)

    def list_execution_artifacts(
        self,
        execution_id: str | None = None,
        artifact_type: str | None = None,
        limit: int = 100,
    ) -> list[ExecutionArtifact]:
        """List execution artifacts."""
        if execution_id:
            if execution_id not in self.artifacts_by_execution:
                return []
            artifact_ids = self.artifacts_by_execution[execution_id]
            artifacts = [
                self.execution_artifacts[aid]
                for aid in artifact_ids
                if aid in self.execution_artifacts
            ]
        else:
            artifacts = list(self.execution_artifacts.values())

        # Filter by artifact type
        if artifact_type:
            artifacts = [a for a in artifacts if a.artifact_type == artifact_type]

        # Sort by created_at (newest first)
        artifacts.sort(key=lambda a: a.created_at, reverse=True)

        # Apply limit
        return artifacts[:limit]

    def delete_execution_artifact(self, artifact_id: str) -> bool:
        """Delete execution artifact."""
        if artifact_id not in self.execution_artifacts:
            return False

        artifact = self.execution_artifacts[artifact_id]
        execution_id = artifact.execution_id

        # Remove from storage
        del self.execution_artifacts[artifact_id]

        # Update indexes
        if execution_id in self.artifacts_by_execution:
            if artifact_id in self.artifacts_by_execution[execution_id]:
                self.artifacts_by_execution[execution_id].remove(artifact_id)
            if not self.artifacts_by_execution[execution_id]:
                del self.artifacts_by_execution[execution_id]

        # Update execution history
        if execution_id in self.execution_histories:
            history = self.execution_histories[execution_id]
            if artifact_id in history.artifacts:
                history.artifacts.remove(artifact_id)

        logger.info("Execution artifact deleted", artifact_id=artifact_id)
        return True

    def cleanup_expired_artifacts(self) -> int:
        """Clean up expired artifacts."""
        current_time = datetime.now(timezone.utc)
        expired_artifacts = []

        for artifact in self.execution_artifacts.values():
            if artifact.expires_at and artifact.expires_at <= current_time:
                expired_artifacts.append(artifact.artifact_id)

        # Delete expired artifacts
        deleted_count = 0
        for artifact_id in expired_artifacts:
            if self.delete_execution_artifact(artifact_id):
                deleted_count += 1

        if deleted_count > 0:
            logger.info("Expired artifacts cleaned up", deleted_count=deleted_count)

        return deleted_count

    def get_storage_statistics(self) -> dict[str, Any]:
        """Get storage statistics."""
        # Calculate statistics
        total_workflows = len(set(v.name for v in self.workflow_versions.values()))
        total_versions = len(self.workflow_versions)
        active_versions = len(
            [
                v
                for v in self.workflow_versions.values()
                if v.is_active and not v.is_deprecated
            ]
        )

        total_executions = len(self.execution_histories)
        archived_executions = len(
            [h for h in self.execution_histories.values() if h.is_archived]
        )

        total_artifacts = len(self.execution_artifacts)
        total_artifact_size = sum(
            a.size_bytes
            for a in self.execution_artifacts.values()
            if a.size_bytes is not None
        )

        # Recent activity (last 24 hours)
        recent_time = datetime.now(timezone.utc) - timedelta(hours=24)
        recent_executions = len(
            [
                h
                for h in self.execution_histories.values()
                if h.created_at >= recent_time
            ]
        )
        recent_artifacts = len(
            [
                a
                for a in self.execution_artifacts.values()
                if a.created_at >= recent_time
            ]
        )

        return {
            "workflows": {
                "total_workflows": total_workflows,
                "total_versions": total_versions,
                "active_versions": active_versions,
                "deprecated_versions": total_versions - active_versions,
            },
            "executions": {
                "total_executions": total_executions,
                "active_executions": total_executions - archived_executions,
                "archived_executions": archived_executions,
                "recent_executions_24h": recent_executions,
            },
            "artifacts": {
                "total_artifacts": total_artifacts,
                "total_size_bytes": total_artifact_size,
                "recent_artifacts_24h": recent_artifacts,
            },
            "storage_backend": self.storage_backend,
            "last_updated": datetime.now(timezone.utc).isoformat(),
        }

    def _get_active_executions_for_version(
        self, workflow_name: str, version: str
    ) -> list[str]:
        """Get active executions using specific workflow version."""
        if workflow_name not in self.executions_by_workflow:
            return []

        active_executions = []
        for execution_id in self.executions_by_workflow[workflow_name]:
            if execution_id in self.execution_histories:
                history = self.execution_histories[execution_id]
                if (
                    history.workflow_version == version
                    and history.status in ["pending", "running"]
                    and not history.is_archived
                ):
                    active_executions.append(execution_id)

        return active_executions

    def export_workflow_definition(
        self, name: str, version: str
    ) -> dict[str, Any] | None:
        """Export workflow definition for backup or migration."""
        workflow_version = self.get_workflow_version(name, version)
        if not workflow_version:
            return None

        return {
            "workflow_version": workflow_version.model_dump(),
            "export_timestamp": datetime.now(timezone.utc).isoformat(),
            "format_version": "1.0",
        }

    def import_workflow_definition(self, export_data: dict[str, Any]) -> bool:
        """Import workflow definition from export data."""
        try:
            workflow_data = export_data.get("workflow_version")
            if not workflow_data:
                logger.error("Invalid export data: missing workflow_version")
                return False

            # Create WorkflowVersion from export data
            workflow_version = WorkflowVersion(**workflow_data)

            # Store the imported version
            self.store_workflow_version(workflow_version)

            logger.info(
                "Workflow definition imported",
                workflow_name=workflow_version.name,
                version=workflow_version.version,
            )

            return True

        except Exception as e:
            logger.error("Failed to import workflow definition", error=str(e))
            return False
