"""Advanced model versioning and rollback capabilities."""

from datetime import datetime, timezone
from typing import Any

import structlog
from pydantic import BaseModel, Field

from ..observability.metrics import MLOpsMetrics
from .config import BaseConfig
from .registry import ModelRegistry

logger = structlog.get_logger(__name__)


class RollbackPermission:
    """Permission levels for rollback operations."""

    READ = "read"
    ROLLBACK_LOW_RISK = "rollback_low_risk"
    ROLLBACK_MEDIUM_RISK = "rollback_medium_risk"
    ROLLBACK_HIGH_RISK = "rollback_high_risk"
    ROLLBACK_CRITICAL_RISK = "rollback_critical_risk"
    ADMIN = "admin"


def _validate_rollback_permissions(
    user: str | None,
    required_permission: str,
    risk_level: str,
    model_name: str | None = None,
) -> bool:
    """Validate user permissions for rollback operations."""
    # If no user specified, deny access
    if not user:
        logger.warning("Rollback attempted without user identification")
        return False

    # System user has all permissions (for automated operations)
    if user == "system":
        return True

    # Map risk levels to required permissions
    risk_permission_map = {
        "low": RollbackPermission.ROLLBACK_LOW_RISK,
        "medium": RollbackPermission.ROLLBACK_MEDIUM_RISK,
        "high": RollbackPermission.ROLLBACK_HIGH_RISK,
        "critical": RollbackPermission.ROLLBACK_CRITICAL_RISK,
    }

    required_perm = risk_permission_map.get(
        risk_level, RollbackPermission.ROLLBACK_CRITICAL_RISK
    )

    # In a real implementation, this would check against an auth service
    # For now, implement basic validation logic

    # Simulate permission checking (replace with actual auth integration)
    user_permissions = _get_user_permissions(user, model_name)

    # Check if user has required permission or higher
    permission_hierarchy = [
        RollbackPermission.READ,
        RollbackPermission.ROLLBACK_LOW_RISK,
        RollbackPermission.ROLLBACK_MEDIUM_RISK,
        RollbackPermission.ROLLBACK_HIGH_RISK,
        RollbackPermission.ROLLBACK_CRITICAL_RISK,
        RollbackPermission.ADMIN,
    ]

    try:
        user_level = max(
            [
                permission_hierarchy.index(perm)
                for perm in user_permissions
                if perm in permission_hierarchy
            ]
        )
        required_level = permission_hierarchy.index(required_perm)

        has_permission = user_level >= required_level

        if not has_permission:
            logger.warning(
                "Insufficient permissions for rollback operation",
                user=user,
                required_permission=required_perm,
                risk_level=risk_level,
                model_name=model_name,
            )

        return has_permission

    except (ValueError, IndexError):
        logger.warning(
            "Invalid permission configuration",
            user=user,
            user_permissions=user_permissions,
            required_permission=required_perm,
        )
        return False


def _get_user_permissions(user: str, model_name: str | None = None) -> list[str]:
    """Get user permissions (placeholder implementation)."""
    # In production, this would integrate with your auth/RBAC system
    # For now, implement basic role simulation

    # Admin users have all permissions
    if user.endswith("_admin") or user == "admin":
        return [RollbackPermission.ADMIN]

    # ML engineers have medium to high risk permissions
    if "engineer" in user.lower() or "ml_" in user.lower():
        return [
            RollbackPermission.READ,
            RollbackPermission.ROLLBACK_LOW_RISK,
            RollbackPermission.ROLLBACK_MEDIUM_RISK,
            RollbackPermission.ROLLBACK_HIGH_RISK,
        ]

    # Data scientists have low to medium risk permissions
    if "scientist" in user.lower() or "data_" in user.lower():
        return [
            RollbackPermission.READ,
            RollbackPermission.ROLLBACK_LOW_RISK,
            RollbackPermission.ROLLBACK_MEDIUM_RISK,
        ]

    # Default users have read and low risk permissions only
    return [RollbackPermission.READ, RollbackPermission.ROLLBACK_LOW_RISK]


class ModelVersioningConfig(BaseConfig):
    """Configuration for model versioning."""

    # Versioning strategy
    versioning_strategy: str = Field(
        default="semantic",
        description="Versioning strategy (semantic, incremental, timestamp)",
    )
    auto_increment_patch: bool = Field(
        default=True, description="Auto-increment patch version for minor changes"
    )
    max_versions_to_keep: int = Field(
        default=50, description="Maximum number of versions to keep per model"
    )

    # Rollback settings
    enable_rollback_protection: bool = Field(
        default=True, description="Enable protection against accidental rollbacks"
    )
    rollback_confirmation_required: bool = Field(
        default=True, description="Require confirmation for rollback operations"
    )
    auto_rollback_on_failure: bool = Field(
        default=False, description="Auto-rollback on deployment failure"
    )

    # Change tracking
    track_model_changes: bool = Field(
        default=True, description="Track detailed model changes"
    )
    require_change_description: bool = Field(
        default=True, description="Require description for version changes"
    )


class ModelVersionInfo(BaseModel):
    """Model version information."""

    model_name: str = Field(description="Model name")
    version: str = Field(description="Version string")
    previous_version: str | None = Field(default=None, description="Previous version")
    change_type: str = Field(description="Type of change (major, minor, patch)")
    change_description: str | None = Field(
        default=None, description="Description of changes"
    )

    # Version metadata
    created_by: str | None = Field(default=None, description="User who created version")
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    model_uri: str | None = Field(default=None, description="MLflow model URI")
    run_id: str | None = Field(default=None, description="MLflow run ID")

    # Performance metrics
    performance_metrics: dict[str, float] = Field(
        default_factory=dict, description="Performance metrics for this version"
    )
    validation_results: dict[str, Any] = Field(
        default_factory=dict, description="Validation test results"
    )

    # Deployment information
    deployment_environments: list[str] = Field(
        default_factory=list, description="Environments where this version is deployed"
    )
    is_production: bool = Field(
        default=False, description="Whether this version is in production"
    )

    # Tags and labels
    tags: dict[str, str] = Field(default_factory=dict, description="Version tags")


class RollbackPlan(BaseModel):
    """Rollback execution plan."""

    model_name: str = Field(description="Model name")
    current_version: str = Field(description="Current version")
    target_version: str = Field(description="Target rollback version")

    # Rollback strategy
    rollback_strategy: str = Field(
        description="Rollback strategy (immediate, gradual, canary)"
    )
    confirmation_required: bool = Field(
        default=True, description="Whether confirmation is required"
    )

    # Risk assessment
    risk_level: str = Field(description="Risk level (low, medium, high, critical)")
    impact_assessment: dict[str, Any] = Field(
        default_factory=dict, description="Impact assessment details"
    )

    # Rollback steps
    rollback_steps: list[dict[str, Any]] = Field(
        default_factory=list, description="Detailed rollback steps"
    )

    # Validation
    validation_checks: list[str] = Field(
        default_factory=list, description="Required validation checks"
    )

    # Timing
    estimated_duration_minutes: int = Field(description="Estimated rollback duration")
    scheduled_time: datetime | None = Field(
        default=None, description="Scheduled rollback time"
    )


class ModelVersionManager:
    """Advanced model versioning and rollback manager."""

    def __init__(
        self,
        config: ModelVersioningConfig | None = None,
        model_registry: ModelRegistry | None = None,
    ):
        """Initialize version manager."""
        self.config = config or ModelVersioningConfig()
        self.model_registry = model_registry or ModelRegistry()
        self.metrics = MLOpsMetrics()

        # Version history storage (in production, use persistent storage)
        self.version_history: dict[str, list[ModelVersionInfo]] = {}

        logger.info(
            "Model version manager initialized",
            versioning_strategy=self.config.versioning_strategy,
            max_versions=self.config.max_versions_to_keep,
        )

    def create_version(
        self,
        model_name: str,
        change_type: str = "patch",
        change_description: str | None = None,
        performance_metrics: dict[str, float] | None = None,
        validation_results: dict[str, Any] | None = None,
        created_by: str | None = None,
        tags: dict[str, str] | None = None,
    ) -> ModelVersionInfo:
        """Create a new model version."""
        try:
            # Validate change type
            if change_type not in ["major", "minor", "patch"]:
                raise ValueError(f"Invalid change type: {change_type}")

            # Require change description if configured
            if self.config.require_change_description and not change_description:
                raise ValueError("Change description is required")

            # Get current version history
            history = self.version_history.get(model_name, [])

            # Determine new version number
            current_version = history[-1].version if history else None
            new_version = self._generate_next_version(current_version, change_type)

            # Get model information from registry
            model_uri = None
            run_id = None
            try:
                latest_model = self.model_registry.get_latest_model_version(model_name)
                model_uri = latest_model.source
                run_id = latest_model.run_id
            except Exception as e:
                logger.warning(
                    "Could not get latest model info from registry",
                    model_name=model_name,
                    error=str(e),
                )

            # Create version info
            version_info = ModelVersionInfo(
                model_name=model_name,
                version=new_version,
                previous_version=current_version,
                change_type=change_type,
                change_description=change_description,
                created_by=created_by,
                model_uri=model_uri,
                run_id=run_id,
                performance_metrics=performance_metrics or {},
                validation_results=validation_results or {},
                tags=tags or {},
            )

            # Add to version history
            if model_name not in self.version_history:
                self.version_history[model_name] = []

            self.version_history[model_name].append(version_info)

            # Clean up old versions if needed
            self._cleanup_old_versions(model_name)

            # Record metrics (placeholder - extend MLOpsMetrics as needed)
            # TODO: Add proper metrics tracking for model versioning

            logger.info(
                "Model version created",
                model_name=model_name,
                version=new_version,
                change_type=change_type,
                previous_version=current_version,
            )

            return version_info

        except Exception as error:
            logger.error(
                "Failed to create model version",
                model_name=model_name,
                change_type=change_type,
                error=str(error),
            )
            raise

    def _generate_next_version(
        self, current_version: str | None, change_type: str
    ) -> str:
        """Generate next version number based on strategy."""
        if self.config.versioning_strategy == "timestamp":
            return datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")

        elif self.config.versioning_strategy == "incremental":
            if current_version is None:
                return "1"
            try:
                return str(int(current_version) + 1)
            except ValueError:
                return "1"

        elif self.config.versioning_strategy == "semantic":
            if current_version is None:
                return "1.0.0"

            try:
                # Parse semantic version
                parts = current_version.split(".")
                if len(parts) != 3:
                    return "1.0.0"

                major, minor, patch = map(int, parts)

                if change_type == "major":
                    return f"{major + 1}.0.0"
                elif change_type == "minor":
                    return f"{major}.{minor + 1}.0"
                else:  # patch
                    return f"{major}.{minor}.{patch + 1}"

            except ValueError:
                return "1.0.0"

        else:
            raise ValueError(
                f"Unknown versioning strategy: {self.config.versioning_strategy}"
            )

    def _cleanup_old_versions(self, model_name: str) -> None:
        """Clean up old versions based on retention policy."""
        history = self.version_history.get(model_name, [])

        if len(history) > self.config.max_versions_to_keep:
            # Keep the most recent versions
            kept_versions = history[-self.config.max_versions_to_keep :]
            removed_count = len(history) - len(kept_versions)

            self.version_history[model_name] = kept_versions

            logger.info(
                "Cleaned up old model versions",
                model_name=model_name,
                removed_count=removed_count,
                kept_count=len(kept_versions),
            )

    def get_version_history(self, model_name: str) -> list[ModelVersionInfo]:
        """Get version history for a model."""
        return self.version_history.get(model_name, [])

    def get_version_info(
        self, model_name: str, version: str
    ) -> ModelVersionInfo | None:
        """Get information for a specific version."""
        history = self.version_history.get(model_name, [])

        for version_info in history:
            if version_info.version == version:
                return version_info

        return None

    def create_rollback_plan(
        self,
        model_name: str,
        target_version: str,
        rollback_strategy: str = "immediate",
    ) -> RollbackPlan:
        """Create a rollback plan."""
        try:
            # Get current version
            history = self.version_history.get(model_name, [])
            if not history:
                raise ValueError(f"No version history found for model {model_name}")

            current_version_info = history[-1]
            current_version = current_version_info.version

            # Validate target version exists
            target_version_info = self.get_version_info(model_name, target_version)
            if not target_version_info:
                raise ValueError(f"Target version {target_version} not found")

            # Assess rollback risk
            risk_level, impact_assessment = self._assess_rollback_risk(
                current_version_info, target_version_info
            )

            # Generate rollback steps
            rollback_steps = self._generate_rollback_steps(
                model_name, current_version, target_version, rollback_strategy
            )

            # Generate validation checks
            validation_checks = self._generate_validation_checks(
                model_name, target_version_info
            )

            # Estimate duration
            estimated_duration = self._estimate_rollback_duration(
                rollback_strategy, len(rollback_steps)
            )

            rollback_plan = RollbackPlan(
                model_name=model_name,
                current_version=current_version,
                target_version=target_version,
                rollback_strategy=rollback_strategy,
                confirmation_required=self.config.rollback_confirmation_required,
                risk_level=risk_level,
                impact_assessment=impact_assessment,
                rollback_steps=rollback_steps,
                validation_checks=validation_checks,
                estimated_duration_minutes=estimated_duration,
            )

            logger.info(
                "Rollback plan created",
                model_name=model_name,
                current_version=current_version,
                target_version=target_version,
                risk_level=risk_level,
                estimated_duration=estimated_duration,
            )

            return rollback_plan

        except Exception as error:
            logger.error(
                "Failed to create rollback plan",
                model_name=model_name,
                target_version=target_version,
                error=str(error),
            )
            raise

    def _assess_rollback_risk(
        self,
        current_version: ModelVersionInfo,
        target_version: ModelVersionInfo,
    ) -> tuple[str, dict[str, Any]]:
        """Assess the risk of rolling back to target version."""
        risk_factors = []
        impact_assessment = {}

        # Check if target version is much older
        version_gap = self._calculate_version_gap(
            current_version.version, target_version.version
        )
        if version_gap > 10:
            risk_factors.append("Large version gap")
            impact_assessment["version_gap"] = version_gap

        # Check production deployment
        if current_version.is_production:
            risk_factors.append("Current version is in production")
            impact_assessment["production_impact"] = True

        # Check performance degradation
        if current_version.performance_metrics and target_version.performance_metrics:
            for metric, current_value in current_version.performance_metrics.items():
                target_value = target_version.performance_metrics.get(metric)
                if target_value and current_value > target_value:
                    risk_factors.append(f"Performance degradation in {metric}")
                    impact_assessment[f"{metric}_degradation"] = (
                        target_value - current_value
                    ) / current_value

        # Determine overall risk level
        if len(risk_factors) == 0:
            risk_level = "low"
        elif len(risk_factors) <= 2:
            risk_level = "medium"
        elif len(risk_factors) <= 4:
            risk_level = "high"
        else:
            risk_level = "critical"

        impact_assessment["risk_factors"] = risk_factors
        impact_assessment["total_risk_factors"] = len(risk_factors)

        return risk_level, impact_assessment

    def _calculate_version_gap(self, current_version: str, target_version: str) -> int:
        """Calculate the gap between versions."""
        try:
            if self.config.versioning_strategy == "incremental":
                return abs(int(current_version) - int(target_version))
            elif self.config.versioning_strategy == "semantic":
                current_parts = list(map(int, current_version.split(".")))
                target_parts = list(map(int, target_version.split(".")))

                # Calculate semantic distance
                major_diff = abs(current_parts[0] - target_parts[0]) * 100
                minor_diff = abs(current_parts[1] - target_parts[1]) * 10
                patch_diff = abs(current_parts[2] - target_parts[2])

                return major_diff + minor_diff + patch_diff
            else:
                return 1  # Default for timestamp versions
        except Exception:
            return 1

    def _generate_rollback_steps(
        self,
        model_name: str,
        current_version: str,
        target_version: str,
        strategy: str,
    ) -> list[dict[str, Any]]:
        """Generate detailed rollback steps."""
        steps = []

        if strategy == "immediate":
            steps.extend(
                [
                    {
                        "step": 1,
                        "action": "validate_target_version",
                        "description": f"Validate target version {target_version} availability",
                        "estimated_minutes": 2,
                    },
                    {
                        "step": 2,
                        "action": "backup_current_state",
                        "description": "Backup current model state and configuration",
                        "estimated_minutes": 5,
                    },
                    {
                        "step": 3,
                        "action": "update_model_registry",
                        "description": "Update model registry to point to target version",
                        "estimated_minutes": 3,
                    },
                    {
                        "step": 4,
                        "action": "restart_inference_services",
                        "description": "Restart model inference services with new version",
                        "estimated_minutes": 10,
                    },
                    {
                        "step": 5,
                        "action": "validate_deployment",
                        "description": "Validate successful rollback deployment",
                        "estimated_minutes": 5,
                    },
                ]
            )

        elif strategy == "gradual":
            steps.extend(
                [
                    {
                        "step": 1,
                        "action": "prepare_canary_deployment",
                        "description": "Prepare canary deployment with target version",
                        "estimated_minutes": 10,
                    },
                    {
                        "step": 2,
                        "action": "deploy_canary",
                        "description": "Deploy target version to 10% of traffic",
                        "estimated_minutes": 15,
                    },
                    {
                        "step": 3,
                        "action": "monitor_canary",
                        "description": "Monitor canary deployment for 30 minutes",
                        "estimated_minutes": 30,
                    },
                    {
                        "step": 4,
                        "action": "increase_traffic",
                        "description": "Gradually increase traffic to target version",
                        "estimated_minutes": 60,
                    },
                    {
                        "step": 5,
                        "action": "complete_rollback",
                        "description": "Complete rollback to 100% traffic",
                        "estimated_minutes": 15,
                    },
                ]
            )

        return steps

    def _generate_validation_checks(
        self, model_name: str, target_version: ModelVersionInfo
    ) -> list[str]:
        """Generate validation checks for rollback."""
        checks = [
            "Model artifact is accessible",
            "Model loads successfully",
            "Model produces valid predictions",
            "Performance metrics are within acceptable range",
        ]

        if target_version.validation_results:
            checks.append("All validation tests pass")

        if target_version.is_production:
            checks.extend(
                [
                    "Production health checks pass",
                    "Monitoring alerts are clear",
                ]
            )

        return checks

    def _estimate_rollback_duration(self, strategy: str, step_count: int) -> int:
        """Estimate rollback duration in minutes."""
        base_time = step_count * 5  # 5 minutes per step on average

        if strategy == "immediate":
            return base_time
        elif strategy == "gradual":
            return base_time + 60  # Additional time for gradual rollout
        elif strategy == "canary":
            return base_time + 30  # Additional time for canary validation

        return base_time

    def execute_rollback(
        self,
        rollback_plan: RollbackPlan,
        executed_by: str,
        confirmation: bool = False,
        dry_run: bool = False,
    ) -> dict[str, Any]:
        """Execute a rollback plan."""
        try:
            # Check rollback permissions
            if not _validate_rollback_permissions(
                user=executed_by,
                required_permission="rollback",
                risk_level=rollback_plan.risk_level,
                model_name=rollback_plan.model_name,
            ):
                raise PermissionError(
                    f"User '{executed_by}' does not have permission to execute {rollback_plan.risk_level} risk rollback for model {rollback_plan.model_name}"
                )

            # Check confirmation requirement
            if rollback_plan.confirmation_required and not confirmation:
                raise ValueError("Rollback confirmation is required")

            # Validate rollback plan
            if rollback_plan.risk_level == "critical" and not confirmation:
                raise ValueError(
                    "Critical risk rollback requires explicit confirmation"
                )

            if dry_run:
                logger.info(
                    "Dry run rollback execution",
                    model_name=rollback_plan.model_name,
                    target_version=rollback_plan.target_version,
                )
                return {
                    "status": "dry_run_success",
                    "message": "Rollback plan validated successfully",
                    "steps_to_execute": len(rollback_plan.rollback_steps),
                }

            # Execute rollback steps
            execution_results = []
            start_time = datetime.now(timezone.utc)

            for step in rollback_plan.rollback_steps:
                step_start = datetime.now(timezone.utc)

                try:
                    # Execute step (this would integrate with actual deployment systems)
                    step_result = self._execute_rollback_step(rollback_plan, step)

                    step_duration = (
                        datetime.now(timezone.utc) - step_start
                    ).total_seconds()
                    execution_results.append(
                        {
                            "step": step["step"],
                            "action": step["action"],
                            "status": "success",
                            "duration_seconds": step_duration,
                            "result": step_result,
                        }
                    )

                except Exception as step_error:
                    step_duration = (
                        datetime.now(timezone.utc) - step_start
                    ).total_seconds()
                    execution_results.append(
                        {
                            "step": step["step"],
                            "action": step["action"],
                            "status": "failed",
                            "duration_seconds": step_duration,
                            "error": str(step_error),
                        }
                    )

                    # Stop execution on failure
                    break

            total_duration = (datetime.now(timezone.utc) - start_time).total_seconds()
            success_count = sum(
                1 for r in execution_results if r["status"] == "success"
            )

            # Update version history with rollback
            if success_count == len(rollback_plan.rollback_steps):
                self._record_successful_rollback(rollback_plan)

            # Record metrics (placeholder - extend MLOpsMetrics as needed)
            # TODO: Add proper metrics tracking for model rollbacks

            result = {
                "status": "success"
                if success_count == len(rollback_plan.rollback_steps)
                else "partial_failure",
                "model_name": rollback_plan.model_name,
                "target_version": rollback_plan.target_version,
                "execution_results": execution_results,
                "total_duration_seconds": total_duration,
                "successful_steps": success_count,
                "total_steps": len(rollback_plan.rollback_steps),
            }

            logger.info(
                "Rollback execution completed",
                model_name=rollback_plan.model_name,
                target_version=rollback_plan.target_version,
                executed_by=executed_by,
                success=result["status"] == "success",
                duration_seconds=total_duration,
            )

            return result

        except Exception as error:
            logger.error(
                "Failed to execute rollback",
                model_name=rollback_plan.model_name,
                target_version=rollback_plan.target_version,
                executed_by=executed_by,
                error=str(error),
            )
            raise

    def _execute_rollback_step(
        self, rollback_plan: RollbackPlan, step: dict[str, Any]
    ) -> dict[str, Any]:
        """Execute a single rollback step."""
        action = step["action"]

        if action == "validate_target_version":
            # Validate target version is available
            target_version = self.get_version_info(
                rollback_plan.model_name, rollback_plan.target_version
            )
            if not target_version:
                raise ValueError(
                    f"Target version {rollback_plan.target_version} not found"
                )
            return {"validation": "passed", "version_found": True}

        elif action == "backup_current_state":
            # Backup current state (placeholder)
            return {
                "backup_created": True,
                "backup_id": f"backup_{datetime.now(timezone.utc).isoformat()}",
            }

        elif action == "update_model_registry":
            # Update model registry to point to target version
            try:
                self.model_registry.transition_model_stage(
                    name=rollback_plan.model_name,
                    version=rollback_plan.target_version,
                    stage="Production",
                    archive_existing=True,
                )
                return {"registry_updated": True}
            except Exception as e:
                raise ValueError(f"Failed to update model registry: {e}")

        else:
            # Placeholder for other actions
            return {"action": action, "executed": True}

    def _record_successful_rollback(self, rollback_plan: RollbackPlan) -> None:
        """Record successful rollback in version history."""
        # Find target version and mark as current
        history = self.version_history.get(rollback_plan.model_name, [])

        for version_info in history:
            if version_info.version == rollback_plan.target_version:
                # Create rollback entry
                rollback_entry = ModelVersionInfo(
                    model_name=rollback_plan.model_name,
                    version=f"{rollback_plan.target_version}_rollback_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}",
                    previous_version=rollback_plan.current_version,
                    change_type="rollback",
                    change_description=f"Rollback to version {rollback_plan.target_version}",
                    model_uri=version_info.model_uri,
                    run_id=version_info.run_id,
                    performance_metrics=version_info.performance_metrics,
                    validation_results=version_info.validation_results,
                    is_production=True,
                    tags={
                        "rollback": "true",
                        "original_version": rollback_plan.target_version,
                    },
                )

                self.version_history[rollback_plan.model_name].append(rollback_entry)
                break

    def get_rollback_history(self, model_name: str) -> list[ModelVersionInfo]:
        """Get rollback history for a model."""
        history = self.version_history.get(model_name, [])
        return [v for v in history if v.change_type == "rollback"]

    def get_current_production_version(
        self, model_name: str
    ) -> ModelVersionInfo | None:
        """Get the current production version of a model."""
        history = self.version_history.get(model_name, [])

        for version_info in reversed(history):
            if version_info.is_production:
                return version_info

        return None

    def compare_versions(
        self, model_name: str, version1: str, version2: str
    ) -> dict[str, Any]:
        """Compare two model versions."""
        v1_info = self.get_version_info(model_name, version1)
        v2_info = self.get_version_info(model_name, version2)

        if not v1_info or not v2_info:
            raise ValueError("One or both versions not found")

        comparison: dict[str, Any] = {
            "model_name": model_name,
            "version1": version1,
            "version2": version2,
            "comparison_timestamp": datetime.now(timezone.utc).isoformat(),
            "performance_comparison": {},
            "metadata_comparison": {},
        }

        # Compare performance metrics
        all_metrics = set(v1_info.performance_metrics.keys()).union(
            set(v2_info.performance_metrics.keys())
        )
        for metric in all_metrics:
            v1_value = v1_info.performance_metrics.get(metric)
            v2_value = v2_info.performance_metrics.get(metric)

            if v1_value is not None and v2_value is not None:
                comparison["performance_comparison"][metric] = {
                    "version1_value": v1_value,
                    "version2_value": v2_value,
                    "difference": v2_value - v1_value,
                    "percentage_change": ((v2_value - v1_value) / v1_value) * 100
                    if v1_value != 0
                    else 0,
                }

        # Compare metadata
        comparison["metadata_comparison"] = {
            "created_at_diff": (
                v2_info.created_at - v1_info.created_at
            ).total_seconds(),
            "change_types": [v1_info.change_type, v2_info.change_type],
            "creation_users": [v1_info.created_by, v2_info.created_by],
        }

        return comparison
