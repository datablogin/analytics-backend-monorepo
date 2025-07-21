"""Base configuration management with hierarchical inheritance."""

import asyncio
import json
from abc import ABC, abstractmethod
from datetime import datetime
from enum import Enum
from pathlib import Path
from typing import Any, TypeVar

import aiofiles
import structlog
import yaml
from pydantic import BaseModel, Field
from pydantic_settings import BaseSettings, SettingsConfigDict

T = TypeVar("T", bound="BaseConfiguration")


class Environment(str, Enum):
    """Supported deployment environments."""

    DEVELOPMENT = "development"
    TESTING = "testing"
    STAGING = "staging"
    PRODUCTION = "production"


class ConfigurationPriority(int, Enum):
    """Configuration source priority levels (higher = takes precedence)."""

    DEFAULT = 0
    FILE_CONFIG = 100
    ENVIRONMENT_VARIABLES = 200
    EXTERNAL_SECRETS = 300
    RUNTIME_OVERRIDE = 400


class ConfigurationAuditLog(BaseModel):
    """Audit log entry for configuration changes."""

    timestamp: datetime = Field(default_factory=datetime.utcnow)
    environment: Environment
    key: str
    old_value: Any = None
    new_value: Any
    source: str
    user: str | None = None
    reason: str | None = None


class BaseConfiguration(BaseSettings, ABC):
    """Base configuration class with hierarchical inheritance."""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",  # Ignore unexpected configuration keys
    )

    # Core metadata
    environment: Environment = Field(
        default=Environment.DEVELOPMENT,
        description="Current deployment environment",
    )
    version: str = Field(default="1.0.0", description="Configuration version")
    last_updated: datetime = Field(
        default_factory=datetime.utcnow,
        description="Last configuration update timestamp",
    )

    # Audit and monitoring
    audit_enabled: bool = Field(
        default=True, description="Enable configuration audit logging"
    )
    hot_reload_enabled: bool = Field(
        default=False, description="Enable hot-reload for non-critical configs"
    )

    @classmethod
    @abstractmethod
    def get_config_name(cls) -> str:
        """Get the configuration name for file-based loading."""
        pass

    @classmethod
    async def load_from_environment(
        cls: type[T], environment: Environment = Environment.DEVELOPMENT
    ) -> T:
        """Load configuration for a specific environment with inheritance."""
        config_data = await cls._load_hierarchical_config(environment)
        return cls(**config_data)

    @classmethod
    async def _load_hierarchical_config(
        cls, environment: Environment
    ) -> dict[str, Any]:
        """Load configuration with hierarchical inheritance."""
        config_name = cls.get_config_name()

        # Start with default configuration
        config_data = await cls._load_config_file("default", config_name)

        # Override with environment-specific configuration
        if environment != Environment.DEVELOPMENT:
            env_config = await cls._load_config_file(environment.value, config_name)
            config_data.update(env_config)

        # Override with local configuration (for development)
        local_config = await cls._load_config_file("local", config_name)
        config_data.update(local_config)

        # Set the environment
        config_data["environment"] = environment

        return config_data

    @classmethod
    async def _load_config_file(cls, env_name: str, config_name: str) -> dict[str, Any]:
        """Load configuration from a specific file asynchronously."""
        logger = structlog.get_logger()
        config_dir = Path("config")
        config_file_patterns = [
            f"{config_name}.{env_name}.json",
            f"{config_name}.{env_name}.yaml",
            f"{config_name}.{env_name}.yml",
            f"{env_name}.{config_name}.json",
            f"{env_name}.{config_name}.yaml",
            f"{env_name}.{config_name}.yml",
        ]

        for pattern in config_file_patterns:
            config_path = config_dir / pattern
            if config_path.exists():
                try:
                    async with aiofiles.open(config_path, encoding="utf-8") as f:
                        content = await f.read()
                        if config_path.suffix.lower() == ".json":
                            return json.loads(content)
                        else:  # YAML
                            return yaml.safe_load(content) or {}
                except (json.JSONDecodeError, yaml.YAMLError) as e:
                    logger.warning(
                        "Failed to parse config file",
                        config_path=str(config_path),
                        error=str(e),
                        error_type=type(e).__name__,
                    )
                except OSError as e:
                    logger.warning(
                        "Failed to read config file",
                        config_path=str(config_path),
                        error=str(e),
                        error_type=type(e).__name__,
                    )

        return {}

    def get_audit_log(self) -> list[ConfigurationAuditLog]:
        """Get configuration audit log entries."""
        # In a real implementation, this would query a persistent store
        return []

    def log_change(
        self,
        key: str,
        old_value: Any,
        new_value: Any,
        source: str,
        user: str | None = None,
        reason: str | None = None,
    ) -> None:
        """Log a configuration change for audit purposes."""
        if not self.audit_enabled:
            return

        audit_entry = ConfigurationAuditLog(
            environment=self.environment,
            key=key,
            old_value=old_value,
            new_value=new_value,
            source=source,
            user=user,
            reason=reason,
        )

        # Log audit entry using structured logging
        logger = structlog.get_logger()
        logger.info(
            "Configuration change audit",
            environment=self.environment.value,
            key=key,
            old_value=old_value,
            new_value=new_value,
            source=source,
            user=user,
            reason=reason,
            timestamp=audit_entry.timestamp.isoformat(),
        )

    def update_setting(
        self,
        key: str,
        value: Any,
        source: str = "runtime",
        user: str | None = None,
        reason: str | None = None,
    ) -> None:
        """Update a configuration setting with audit logging."""
        if not hasattr(self, key):
            raise ValueError(f"Configuration key '{key}' does not exist")

        old_value = getattr(self, key)

        # Validate the new value using Pydantic
        try:
            # Create a temporary instance to validate the change
            temp_data = self.model_dump()
            temp_data[key] = value
            self.__class__(**temp_data)  # This will raise if invalid

            # If validation passes, update the actual value
            setattr(self, key, value)
            self.last_updated = datetime.utcnow()

            # Log the change
            self.log_change(key, old_value, value, source, user, reason)

        except Exception as e:
            raise ValueError(f"Invalid value for '{key}': {e}")

    def to_dict(self, exclude_sensitive: bool = True) -> dict[str, Any]:
        """Convert configuration to dictionary, optionally excluding sensitive data."""
        data = self.model_dump()

        if exclude_sensitive:
            # Remove sensitive fields (override in subclasses for specific sensitive fields)
            sensitive_patterns = ["password", "token", "key", "secret"]
            filtered_data = {}
            for k, v in data.items():
                if not any(pattern in k.lower() for pattern in sensitive_patterns):
                    filtered_data[k] = v
                else:
                    filtered_data[k] = "***REDACTED***"
            return filtered_data

        return data

    def validate_configuration(self) -> list[str]:
        """Validate the current configuration and return any issues."""
        issues = []

        # Basic validation - override in subclasses for specific validation
        if self.environment == Environment.PRODUCTION:
            if self.hot_reload_enabled:
                issues.append("Hot reload should be disabled in production")

        return issues


class ConfigurationManager:
    """Central configuration manager for all application configurations."""

    def __init__(self):
        self._configurations: dict[str, BaseConfiguration] = {}
        self._file_watchers: dict[str, Any] = {}

    def register_configuration(self, name: str, config: BaseConfiguration) -> None:
        """Register a configuration instance."""
        self._configurations[name] = config

        # Set up file watching for hot reload if enabled
        if config.hot_reload_enabled:
            self._setup_file_watcher(name, config)

    def get_configuration(self, name: str) -> BaseConfiguration | None:
        """Get a registered configuration by name."""
        return self._configurations.get(name)

    def reload_configuration(self, name: str) -> bool:
        """Reload a configuration from its source."""
        if name not in self._configurations:
            return False

        config = self._configurations[name]
        try:
            # Reload from the same environment
            new_config = asyncio.run(
                config.__class__.load_from_environment(config.environment)
            )
            self._configurations[name] = new_config
            return True
        except (ValueError, TypeError, OSError) as e:
            logger = structlog.get_logger()
            logger.error(
                "Failed to reload configuration",
                config_name=name,
                error=str(e),
                error_type=type(e).__name__,
            )
            return False

    def _setup_file_watcher(self, name: str, config: BaseConfiguration) -> None:
        """Set up file watching for hot reload (placeholder implementation)."""
        # In a real implementation, this would use a file system watcher
        # like watchdog to monitor configuration files for changes
        pass

    def get_all_configurations(self) -> dict[str, dict[str, Any]]:
        """Get all configurations as dictionaries."""
        return {
            name: config.to_dict(exclude_sensitive=True)
            for name, config in self._configurations.items()
        }

    def validate_all_configurations(self) -> dict[str, list[str]]:
        """Validate all registered configurations."""
        validation_results = {}
        for name, config in self._configurations.items():
            issues = config.validate_configuration()
            if issues:
                validation_results[name] = issues
        return validation_results


# Global configuration manager instance
config_manager = ConfigurationManager()
