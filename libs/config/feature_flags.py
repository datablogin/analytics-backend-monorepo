"""Feature flag management system with runtime controls."""

import hashlib
from datetime import datetime
from enum import Enum
from typing import Any

import structlog
from pydantic import BaseModel, Field, field_validator


class FeatureFlagType(str, Enum):
    """Types of feature flags."""

    BOOLEAN = "boolean"
    PERCENTAGE = "percentage"
    STRING = "string"
    JSON = "json"


class FeatureFlagStatus(str, Enum):
    """Feature flag status."""

    ACTIVE = "active"
    INACTIVE = "inactive"
    SCHEDULED = "scheduled"
    ARCHIVED = "archived"


class TargetingRule(BaseModel):
    """Rule for targeting specific users or segments."""

    name: str
    description: str | None = None
    conditions: dict[str, Any] = Field(default_factory=dict)
    value: Any
    priority: int = Field(
        default=100, description="Higher priority rules are evaluated first"
    )


class ABTestConfig(BaseModel):
    """A/B test configuration."""

    name: str
    description: str | None = None
    start_date: datetime | None = None
    end_date: datetime | None = None
    variants: dict[str, Any] = Field(default_factory=dict)
    traffic_allocation: dict[str, float] = Field(default_factory=dict)

    @field_validator("traffic_allocation")
    @classmethod
    def validate_allocation(cls, v):
        if sum(v.values()) > 100.0:
            raise ValueError("Total traffic allocation cannot exceed 100%")
        return v


class FeatureFlag(BaseModel):
    """Feature flag configuration."""

    key: str = Field(..., description="Unique feature flag key")
    name: str = Field(..., description="Human-readable name")
    description: str | None = None
    type: FeatureFlagType = FeatureFlagType.BOOLEAN
    status: FeatureFlagStatus = FeatureFlagStatus.ACTIVE

    # Values
    default_value: Any = False
    current_value: Any = None

    # Targeting and rollout
    percentage_rollout: float = Field(default=0.0, ge=0.0, le=100.0)
    targeting_rules: list[TargetingRule] = Field(default_factory=list)

    # A/B testing
    ab_test: ABTestConfig | None = None

    # Metadata
    created_at: datetime = Field(default_factory=datetime.utcnow)
    updated_at: datetime = Field(default_factory=datetime.utcnow)
    created_by: str | None = None
    tags: set[str] = Field(default_factory=set)

    # Environment restrictions
    environments: set[str] = Field(
        default_factory=lambda: {"development", "staging", "production"}
    )

    # Scheduling
    start_date: datetime | None = None
    end_date: datetime | None = None

    def is_active(self, environment: str = "development") -> bool:
        """Check if the feature flag is active in the given environment."""
        if self.status != FeatureFlagStatus.ACTIVE:
            return False

        if environment not in self.environments:
            return False

        now = datetime.utcnow()
        if self.start_date and now < self.start_date:
            return False

        if self.end_date and now > self.end_date:
            return False

        return True

    def evaluate(
        self,
        user_id: str | None = None,
        user_attributes: dict[str, Any] | None = None,
        environment: str = "development",
    ) -> Any:
        """Evaluate the feature flag for a given user/context."""
        if not self.is_active(environment):
            return self.default_value

        user_attributes = user_attributes or {}

        # Check targeting rules first (highest priority)
        for rule in sorted(
            self.targeting_rules, key=lambda r: r.priority, reverse=True
        ):
            if self._evaluate_targeting_rule(rule, user_id, user_attributes):
                return rule.value

        # Check A/B test configuration
        if self.ab_test and self._is_ab_test_active():
            variant = self._get_ab_test_variant(user_id)
            if variant and variant in self.ab_test.variants:
                return self.ab_test.variants[variant]

        # Check percentage rollout
        if self.percentage_rollout > 0 and user_id:
            user_percentage = self._get_user_percentage(user_id)
            if user_percentage <= self.percentage_rollout:
                return self.current_value if self.current_value is not None else True

        # Return default value
        return self.default_value

    def _evaluate_targeting_rule(
        self,
        rule: TargetingRule,
        user_id: str | None,
        user_attributes: dict[str, Any],
    ) -> bool:
        """Evaluate a targeting rule against user context."""
        for condition_key, condition_value in rule.conditions.items():
            if condition_key == "user_id":
                if user_id != condition_value:
                    return False
            elif condition_key == "user_ids":
                if user_id not in condition_value:
                    return False
            elif condition_key.startswith("attr_"):
                attr_name = condition_key[5:]  # Remove 'attr_' prefix
                if attr_name not in user_attributes:
                    return False
                if user_attributes[attr_name] != condition_value:
                    return False
            elif condition_key in user_attributes:
                if user_attributes[condition_key] != condition_value:
                    return False

        return True

    def _is_ab_test_active(self) -> bool:
        """Check if the A/B test is currently active."""
        if not self.ab_test:
            return False

        now = datetime.utcnow()
        if self.ab_test.start_date and now < self.ab_test.start_date:
            return False

        if self.ab_test.end_date and now > self.ab_test.end_date:
            return False

        return True

    def _get_ab_test_variant(self, user_id: str | None) -> str | None:
        """Get the A/B test variant for a user."""
        if not user_id or not self.ab_test:
            return None

        # Use consistent hashing to assign users to variants
        hash_input = f"{self.key}:{user_id}:{self.ab_test.name}"
        hash_value = int(hashlib.md5(hash_input.encode()).hexdigest(), 16)
        percentage = (hash_value % 10000) / 100.0  # 0-99.99%

        cumulative_percentage = 0.0
        for variant, allocation in self.ab_test.traffic_allocation.items():
            cumulative_percentage += allocation
            if percentage <= cumulative_percentage:
                return variant

        return None

    def _get_user_percentage(self, user_id: str) -> float:
        """Get consistent percentage for a user based on flag key and user ID."""
        hash_input = f"{self.key}:{user_id}"
        hash_value = int(hashlib.md5(hash_input.encode()).hexdigest(), 16)
        return (hash_value % 10000) / 100.0  # 0-99.99%


class FeatureFlagManager:
    """Manager for feature flags with runtime controls."""

    def __init__(self):
        self._flags: dict[str, FeatureFlag] = {}
        self._cache_ttl: int = 300  # 5 minutes cache TTL
        self._last_cache_refresh: datetime = datetime.utcnow()

    def register_flag(self, flag: FeatureFlag) -> None:
        """Register a feature flag."""
        self._flags[flag.key] = flag

    def create_flag(
        self,
        key: str,
        name: str,
        description: str | None = None,
        flag_type: FeatureFlagType = FeatureFlagType.BOOLEAN,
        default_value: Any = False,
        environments: set[str] | None = None,
        created_by: str | None = None,
    ) -> FeatureFlag:
        """Create and register a new feature flag."""
        flag = FeatureFlag(
            key=key,
            name=name,
            description=description,
            type=flag_type,
            default_value=default_value,
            environments=environments or {"development", "staging", "production"},
            created_by=created_by,
        )
        self.register_flag(flag)
        return flag

    def get_flag(self, key: str) -> FeatureFlag | None:
        """Get a feature flag by key."""
        return self._flags.get(key)

    def is_enabled(
        self,
        key: str,
        user_id: str | None = None,
        user_attributes: dict[str, Any] | None = None,
        environment: str = "development",
        default: Any = False,
    ) -> bool:
        """Check if a feature flag is enabled for a user/context."""
        flag = self.get_flag(key)
        if not flag:
            return default

        result = flag.evaluate(user_id, user_attributes, environment)
        return bool(result) if flag.type == FeatureFlagType.BOOLEAN else bool(default)

    def get_value(
        self,
        key: str,
        user_id: str | None = None,
        user_attributes: dict[str, Any] | None = None,
        environment: str = "development",
        default: Any = None,
    ) -> Any:
        """Get the value of a feature flag for a user/context."""
        flag = self.get_flag(key)
        if not flag:
            return default

        return flag.evaluate(user_id, user_attributes, environment)

    def update_flag(
        self,
        key: str,
        updates: dict[str, Any],
        updated_by: str | None = None,
    ) -> bool:
        """Update a feature flag configuration."""
        flag = self.get_flag(key)
        if not flag:
            return False

        try:
            # Create updated flag data
            flag_data = flag.model_dump()
            flag_data.update(updates)
            flag_data["updated_at"] = datetime.utcnow()

            # Validate and create new flag
            updated_flag = FeatureFlag(**flag_data)
            self._flags[key] = updated_flag
            return True
        except (ValueError, TypeError) as e:
            logger = structlog.get_logger()
            logger.error(
                "Failed to update feature flag",
                flag_key=key,
                error=str(e),
                error_type=type(e).__name__,
            )
            return False

    def delete_flag(self, key: str) -> bool:
        """Delete a feature flag."""
        if key in self._flags:
            del self._flags[key]
            return True
        return False

    def list_flags(
        self,
        environment: str | None = None,
        status: FeatureFlagStatus | None = None,
        tags: set[str] | None = None,
    ) -> list[FeatureFlag]:
        """List feature flags with optional filtering."""
        flags = list(self._flags.values())

        if environment:
            flags = [f for f in flags if environment in f.environments]

        if status:
            flags = [f for f in flags if f.status == status]

        if tags:
            flags = [f for f in flags if f.tags.intersection(tags)]

        return flags

    def get_flag_statistics(self, key: str) -> dict[str, Any]:
        """Get statistics for a feature flag (placeholder for metrics integration)."""
        flag = self.get_flag(key)
        if not flag:
            return {}

        # In a real implementation, this would query metrics/analytics system
        return {
            "flag_key": key,
            "evaluations_count": 0,
            "unique_users": 0,
            "rollout_percentage": flag.percentage_rollout,
            "last_evaluation": None,
        }

    def export_flags(self, environment: str | None = None) -> dict[str, Any]:
        """Export feature flags configuration."""
        flags = self.list_flags(environment=environment)
        return {
            "export_timestamp": datetime.utcnow().isoformat(),
            "environment": environment,
            "flags": [flag.model_dump() for flag in flags],
        }

    def import_flags(self, config_data: dict[str, Any]) -> bool:
        """Import feature flags from configuration data."""
        try:
            for flag_data in config_data.get("flags", []):
                flag = FeatureFlag(**flag_data)
                self.register_flag(flag)
            return True
        except (ValueError, TypeError, KeyError) as e:
            logger = structlog.get_logger()
            logger.error(
                "Failed to import feature flags",
                error=str(e),
                error_type=type(e).__name__,
            )
            return False

    def validate_flags(self) -> dict[str, list[str]]:
        """Validate all feature flags and return any issues."""
        issues = {}

        for key, flag in self._flags.items():
            flag_issues = []

            # Check for conflicting A/B tests
            if flag.ab_test and flag.percentage_rollout > 0:
                flag_issues.append(
                    "Flag has both A/B test and percentage rollout configured"
                )

            # Check for expired flags
            if flag.end_date and datetime.utcnow() > flag.end_date:
                flag_issues.append("Flag has expired and should be archived")

            # Check percentage rollout bounds
            if flag.percentage_rollout < 0 or flag.percentage_rollout > 100:
                flag_issues.append("Percentage rollout must be between 0 and 100")

            if flag_issues:
                issues[key] = flag_issues

        return issues


# Global feature flag manager instance
feature_flag_manager = FeatureFlagManager()
