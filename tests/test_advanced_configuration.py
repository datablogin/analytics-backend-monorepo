"""Tests for advanced configuration management system."""

import json
import os
import tempfile
from datetime import datetime, timedelta
from pathlib import Path
from unittest.mock import patch

import pytest

from libs.config import (
    APIConfiguration,
    BaseConfiguration,
    ConfigurationManager,
    Environment,
    FeatureFlag,
    FeatureFlagManager,
    FeatureFlagStatus,
    FeatureFlagType,
    Secret,
    SecretProvider,
    SecretsManager,
    get_api_config,
)
from libs.config.feature_flags import ABTestConfig, TargetingRule


class TestConfiguration(BaseConfiguration):
    """Test configuration class."""

    test_setting: str = "default_value"
    test_number: int = 42
    test_boolean: bool = True

    @classmethod
    def get_config_name(cls) -> str:
        return "test"


class TestBaseConfiguration:
    """Test base configuration functionality."""

    def test_configuration_creation(self):
        """Test creating a configuration instance."""
        config = TestConfiguration()
        assert config.test_setting == "default_value"
        assert config.test_number == 42
        assert config.test_boolean is True
        assert config.environment == Environment.DEVELOPMENT

    def test_configuration_with_environment(self):
        """Test configuration loading with different environments."""
        config = TestConfiguration(environment=Environment.PRODUCTION)
        assert config.environment == Environment.PRODUCTION

    def test_configuration_validation(self):
        """Test configuration validation."""
        config = TestConfiguration()
        issues = config.validate_configuration()
        assert isinstance(issues, list)

    def test_configuration_to_dict(self):
        """Test converting configuration to dictionary."""
        config = TestConfiguration()
        config_dict = config.to_dict()
        assert isinstance(config_dict, dict)
        assert "test_setting" in config_dict
        assert "environment" in config_dict

    def test_configuration_update_setting(self):
        """Test updating configuration settings."""
        config = TestConfiguration()

        config.update_setting("test_setting", "new_value", source="test")
        assert config.test_setting == "new_value"

    def test_configuration_update_invalid_setting(self):
        """Test updating invalid configuration setting."""
        config = TestConfiguration()

        with pytest.raises(ValueError):
            config.update_setting("nonexistent_key", "value")

    def test_hierarchical_config_loading(self):
        """Test hierarchical configuration loading from files."""
        with tempfile.TemporaryDirectory() as temp_dir:
            config_dir = Path(temp_dir) / "config"
            config_dir.mkdir()

            # Create default config
            default_config = {"test_setting": "default", "test_number": 10}
            with open(config_dir / "test.default.json", "w") as f:
                json.dump(default_config, f)

            # Create environment-specific config
            prod_config = {"test_setting": "production", "test_number": 20}
            with open(config_dir / "test.production.json", "w") as f:
                json.dump(prod_config, f)

            # Mock the config directory
            with patch("libs.config.base.Path") as mock_path:
                mock_path.return_value = config_dir
                config_data = TestConfiguration._load_hierarchical_config(
                    Environment.PRODUCTION
                )

                # Should have production values overriding defaults
                assert config_data["test_setting"] == "production"
                assert config_data["test_number"] == 20


class TestConfigurationManager:
    """Test configuration manager functionality."""

    def test_configuration_manager_creation(self):
        """Test creating a configuration manager."""
        manager = ConfigurationManager()
        assert isinstance(manager, ConfigurationManager)

    def test_register_configuration(self):
        """Test registering a configuration."""
        manager = ConfigurationManager()
        config = TestConfiguration()

        manager.register_configuration("test", config)
        retrieved_config = manager.get_configuration("test")
        assert retrieved_config is config

    def test_get_nonexistent_configuration(self):
        """Test getting a non-existent configuration."""
        manager = ConfigurationManager()
        config = manager.get_configuration("nonexistent")
        assert config is None

    def test_get_all_configurations(self):
        """Test getting all configurations."""
        manager = ConfigurationManager()
        config = TestConfiguration()
        manager.register_configuration("test", config)

        all_configs = manager.get_all_configurations()
        assert "test" in all_configs
        assert isinstance(all_configs["test"], dict)

    def test_validate_all_configurations(self):
        """Test validating all configurations."""
        manager = ConfigurationManager()
        config = TestConfiguration()
        manager.register_configuration("test", config)

        validation_results = manager.validate_all_configurations()
        assert isinstance(validation_results, dict)


class TestFeatureFlags:
    """Test feature flag functionality."""

    def test_feature_flag_creation(self):
        """Test creating a feature flag."""
        flag = FeatureFlag(
            key="test_feature",
            name="Test Feature",
            description="A test feature flag",
            type=FeatureFlagType.BOOLEAN,
            default_value=False,
        )
        assert flag.key == "test_feature"
        assert flag.name == "Test Feature"
        assert flag.type == FeatureFlagType.BOOLEAN
        assert flag.default_value is False

    def test_feature_flag_is_active(self):
        """Test feature flag active status."""
        flag = FeatureFlag(
            key="test_feature",
            name="Test Feature",
            status=FeatureFlagStatus.ACTIVE,
        )
        assert flag.is_active("development") is True

        flag.status = FeatureFlagStatus.INACTIVE
        assert flag.is_active("development") is False

    def test_feature_flag_environment_restriction(self):
        """Test feature flag environment restrictions."""
        flag = FeatureFlag(
            key="test_feature",
            name="Test Feature",
            environments={"production"},
        )
        assert flag.is_active("production") is True
        assert flag.is_active("development") is False

    def test_feature_flag_scheduled_activation(self):
        """Test feature flag scheduled activation."""
        future_date = datetime.utcnow() + timedelta(hours=1)
        flag = FeatureFlag(
            key="test_feature",
            name="Test Feature",
            start_date=future_date,
        )
        assert flag.is_active("development") is False

    def test_feature_flag_evaluation_basic(self):
        """Test basic feature flag evaluation."""
        flag = FeatureFlag(
            key="test_feature",
            name="Test Feature",
            status=FeatureFlagStatus.ACTIVE,
            default_value=True,
        )
        result = flag.evaluate()
        assert result is True

    def test_feature_flag_percentage_rollout(self):
        """Test feature flag percentage rollout."""
        flag = FeatureFlag(
            key="test_feature",
            name="Test Feature",
            status=FeatureFlagStatus.ACTIVE,
            percentage_rollout=50.0,
            current_value=True,
            default_value=False,
        )

        # Test with a specific user ID that should be in the rollout
        result = flag.evaluate(user_id="user_123")
        assert isinstance(result, bool)

    def test_feature_flag_targeting_rules(self):
        """Test feature flag targeting rules."""
        targeting_rule = TargetingRule(
            name="Admin Users",
            conditions={"attr_role": "admin"},
            value=True,
            priority=200,
        )

        flag = FeatureFlag(
            key="test_feature",
            name="Test Feature",
            status=FeatureFlagStatus.ACTIVE,
            targeting_rules=[targeting_rule],
            default_value=False,
        )

        # Should return True for admin users
        result = flag.evaluate(
            user_id="user_123",
            user_attributes={"role": "admin"},
        )
        assert result is True

        # Should return default for non-admin users
        result = flag.evaluate(
            user_id="user_123",
            user_attributes={"role": "user"},
        )
        assert result is False

    def test_feature_flag_ab_test(self):
        """Test feature flag A/B testing."""
        ab_test = ABTestConfig(
            name="button_color_test",
            variants={"red": "#ff0000", "blue": "#0000ff"},
            traffic_allocation={"red": 50.0, "blue": 50.0},
        )

        flag = FeatureFlag(
            key="button_color",
            name="Button Color",
            status=FeatureFlagStatus.ACTIVE,
            ab_test=ab_test,
            default_value="#000000",
        )

        result = flag.evaluate(user_id="user_123")
        # Should return one of the variant values or default
        assert result in ["#ff0000", "#0000ff", "#000000"]


class TestFeatureFlagManager:
    """Test feature flag manager functionality."""

    def test_feature_flag_manager_creation(self):
        """Test creating a feature flag manager."""
        manager = FeatureFlagManager()
        assert isinstance(manager, FeatureFlagManager)

    def test_create_and_register_flag(self):
        """Test creating and registering a feature flag."""
        manager = FeatureFlagManager()
        flag = manager.create_flag(
            key="test_feature",
            name="Test Feature",
            description="A test feature flag",
        )

        assert flag.key == "test_feature"
        retrieved_flag = manager.get_flag("test_feature")
        assert retrieved_flag is flag

    def test_is_enabled(self):
        """Test checking if a feature is enabled."""
        manager = FeatureFlagManager()
        manager.create_flag(
            key="test_feature",
            name="Test Feature",
            default_value=True,
        )

        assert manager.is_enabled("test_feature") is True
        assert manager.is_enabled("nonexistent_feature", default=False) is False

    def test_get_value(self):
        """Test getting feature flag value."""
        manager = FeatureFlagManager()
        manager.create_flag(
            key="test_setting",
            name="Test Setting",
            flag_type=FeatureFlagType.STRING,
            default_value="test_value",
        )

        value = manager.get_value("test_setting")
        assert value == "test_value"

    def test_update_flag(self):
        """Test updating a feature flag."""
        manager = FeatureFlagManager()
        manager.create_flag(
            key="test_feature",
            name="Test Feature",
            default_value=False,
        )

        success = manager.update_flag(
            "test_feature",
            {"default_value": True, "description": "Updated description"},
        )
        assert success is True

        updated_flag = manager.get_flag("test_feature")
        assert updated_flag.default_value is True
        assert updated_flag.description == "Updated description"

    def test_list_flags(self):
        """Test listing feature flags."""
        manager = FeatureFlagManager()
        manager.create_flag("feature1", "Feature 1", environments={"development"})
        manager.create_flag("feature2", "Feature 2", environments={"production"})

        all_flags = manager.list_flags()
        assert len(all_flags) == 2

        dev_flags = manager.list_flags(environment="development")
        assert len(dev_flags) == 1
        assert dev_flags[0].key == "feature1"

    def test_validate_flags(self):
        """Test validating feature flags."""
        manager = FeatureFlagManager()

        # Create a flag with conflicting configuration
        manager.create_flag("test_feature", "Test Feature")
        manager.update_flag("test_feature", {
            "percentage_rollout": 50.0,
            "ab_test": {
                "name": "test",
                "variants": {"a": True, "b": False},
                "traffic_allocation": {"a": 50.0, "b": 50.0},
            }
        })

        issues = manager.validate_flags()
        assert "test_feature" in issues
        assert len(issues["test_feature"]) > 0

    def test_export_import_flags(self):
        """Test exporting and importing feature flags."""
        manager = FeatureFlagManager()
        manager.create_flag("feature1", "Feature 1")
        manager.create_flag("feature2", "Feature 2")

        # Export flags
        exported_data = manager.export_flags()
        assert "flags" in exported_data
        assert len(exported_data["flags"]) == 2

        # Import flags to new manager
        new_manager = FeatureFlagManager()
        success = new_manager.import_flags(exported_data)
        assert success is True

        imported_flags = new_manager.list_flags()
        assert len(imported_flags) == 2


class TestSecretsManagement:
    """Test secrets management functionality."""

    def test_secrets_manager_creation(self):
        """Test creating a secrets manager."""
        manager = SecretsManager()
        assert isinstance(manager, SecretsManager)

    @pytest.mark.asyncio
    async def test_environment_secret_provider(self):
        """Test environment variable secret provider."""
        from libs.config.secrets import EnvironmentSecretProvider

        provider = EnvironmentSecretProvider()

        # Set a test secret
        await provider.set_secret("TEST_SECRET", "", "test_value")
        assert os.getenv("TEST_SECRET") == "test_value"

        # Get the secret
        value = await provider.get_secret("TEST_SECRET", "")
        assert value == "test_value"

        # List secrets
        secrets = await provider.list_secrets("TEST_")
        assert "TEST_SECRET" in secrets

        # Delete the secret
        await provider.delete_secret("TEST_SECRET", "")
        assert os.getenv("TEST_SECRET") is None

    @pytest.mark.asyncio
    async def test_file_based_secret_provider(self):
        """Test file-based secret provider."""
        from libs.config.secrets import FileBasedSecretProvider

        with tempfile.TemporaryDirectory() as temp_dir:
            provider = FileBasedSecretProvider(secrets_dir=temp_dir)

            # Store a secret
            success = await provider.set_secret("test_key", "", "secret_value")
            assert success is True

            # Retrieve the secret
            value = await provider.get_secret("test_key", "")
            assert value == "secret_value"

            # List secrets
            secrets = await provider.list_secrets()
            assert "test_key" in secrets

            # Rotate the secret
            new_value = await provider.rotate_secret("test_key", "")
            assert new_value is not None
            assert new_value != "secret_value"

            # Delete the secret
            success = await provider.delete_secret("test_key", "")
            assert success is True

            # Should not exist anymore
            value = await provider.get_secret("test_key", "")
            assert value is None

    @pytest.mark.asyncio
    async def test_secrets_manager_with_providers(self):
        """Test secrets manager with multiple providers."""
        from libs.config.secrets import EnvironmentSecretProvider

        manager = SecretsManager()
        env_provider = EnvironmentSecretProvider()
        manager.register_provider(SecretProvider.ENVIRONMENT, env_provider)

        # Store a secret
        success = await manager.set_secret(
            "TEST_API_KEY", "api_key_value", SecretProvider.ENVIRONMENT
        )
        assert success is True

        # Retrieve the secret
        secret = await manager.get_secret("TEST_API_KEY", SecretProvider.ENVIRONMENT)
        assert secret is not None
        assert secret.value.get_secret_value() == "api_key_value"
        assert secret.metadata.provider == SecretProvider.ENVIRONMENT

    def test_secret_metadata(self):
        """Test secret metadata functionality."""
        from pydantic import SecretStr

        from libs.config.secrets import SecretMetadata

        metadata = SecretMetadata(
            key="test_key",
            provider=SecretProvider.ENVIRONMENT,
            path="test/path",
        )

        secret = Secret(
            metadata=metadata,
            value=SecretStr("secret_value"),
        )

        assert secret.metadata.key == "test_key"
        assert secret.metadata.provider == SecretProvider.ENVIRONMENT
        assert secret.value.get_secret_value() == "secret_value"
        assert not secret.is_expired()


class TestAPIConfiguration:
    """Test API-specific configuration."""

    def test_api_configuration_creation(self):
        """Test creating API configuration."""
        config = APIConfiguration()
        assert config.host == "0.0.0.0"
        assert config.port == 8000
        assert config.rate_limit_per_minute == 100

    def test_api_configuration_validation(self):
        """Test API configuration validation."""
        config = APIConfiguration(environment=Environment.PRODUCTION)
        issues = config.validate_configuration()

        # Should have issues due to default values in production
        assert len(issues) > 0
        assert any("JWT secret key" in issue for issue in issues)

    def test_api_configuration_feature_flags_setup(self):
        """Test API configuration feature flags setup."""
        config = APIConfiguration()
        config.setup_feature_flags()

        # Should have created feature flags
        from libs.config import feature_flag_manager

        registration_flag = feature_flag_manager.get_flag("enable_registration")
        assert registration_flag is not None
        assert registration_flag.name == "User Registration"

    def test_api_configuration_runtime_config(self):
        """Test getting runtime configuration with feature flag overrides."""
        config = APIConfiguration()
        config.setup_feature_flags()

        runtime_config = config.get_runtime_config()
        assert isinstance(runtime_config, dict)
        assert "enable_registration" in runtime_config
        assert "enable_admin_endpoints" in runtime_config

    def test_get_api_config_function(self):
        """Test the get_api_config convenience function."""
        config = get_api_config(Environment.DEVELOPMENT)
        assert isinstance(config, APIConfiguration)
        assert config.environment == Environment.DEVELOPMENT


class TestIntegration:
    """Test integration between different configuration components."""

    def test_configuration_manager_with_api_config(self):
        """Test configuration manager with API configuration."""
        from libs.config import config_manager

        api_config = APIConfiguration()
        config_manager.register_configuration("api", api_config)

        retrieved_config = config_manager.get_configuration("api")
        assert retrieved_config is api_config

    def test_feature_flags_with_configuration(self):
        """Test feature flags integration with configuration."""
        config = APIConfiguration()
        config.setup_feature_flags()

        # Test feature flag evaluation
        is_enabled = config.is_feature_enabled("enable_registration")
        assert isinstance(is_enabled, bool)

        # Test with user context
        is_enabled_user = config.is_feature_enabled(
            "enable_admin_endpoints",
            user_id="admin_user",
            user_attributes={"role": "admin"},
        )
        assert isinstance(is_enabled_user, bool)

    def test_configuration_audit_logging(self):
        """Test configuration audit logging."""
        config = TestConfiguration()

        # Enable audit logging
        config.audit_enabled = True

        # Update a setting (should generate audit log)
        with patch("builtins.print") as mock_print:
            config.update_setting("test_setting", "new_value", source="test", user="test_user")

            # Should have printed audit log
            mock_print.assert_called()
            call_args = mock_print.call_args[0][0]
            assert "CONFIG AUDIT:" in call_args
            assert "test_setting" in call_args

