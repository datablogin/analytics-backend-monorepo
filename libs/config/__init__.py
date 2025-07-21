"""Advanced configuration management with hierarchical inheritance and feature flags."""

from .api_config import APIConfiguration, get_api_config
from .base import (
    BaseConfiguration,
    ConfigurationManager,
    Environment,
    config_manager,
)
from .database import DatabaseSettings, get_database_settings
from .feature_flags import (
    FeatureFlag,
    FeatureFlagManager,
    FeatureFlagStatus,
    FeatureFlagType,
    feature_flag_manager,
)
from .secrets import (
    Secret,
    SecretProvider,
    SecretsManager,
    secrets_manager,
)

__all__ = [
    # Base configuration
    "BaseConfiguration",
    "ConfigurationManager",
    "Environment",
    "config_manager",
    # Database configuration
    "DatabaseSettings",
    "get_database_settings",
    # API configuration
    "APIConfiguration",
    "get_api_config",
    # Feature flags
    "FeatureFlag",
    "FeatureFlagManager",
    "FeatureFlagStatus",
    "FeatureFlagType",
    "feature_flag_manager",
    # Secrets management
    "Secret",
    "SecretProvider",
    "SecretsManager",
    "secrets_manager",
]
