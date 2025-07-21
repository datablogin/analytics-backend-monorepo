"""Analytics API specific configuration with feature flags integration."""

from typing import Any

from pydantic import Field

from .base import BaseConfiguration, Environment
from .feature_flags import ABTestConfig, feature_flag_manager


class APIConfiguration(BaseConfiguration):
    """Analytics API configuration with feature flags."""

    # Server settings
    host: str = Field(default="0.0.0.0", description="API host address")
    port: int = Field(default=8000, description="API port number")
    workers: int = Field(default=1, description="Number of worker processes")

    # Security settings
    cors_origins: list[str] = Field(default=["*"], description="Allowed CORS origins")
    cors_credentials: bool = Field(default=True, description="Allow CORS credentials")
    rate_limit_per_minute: int = Field(
        default=100, description="Rate limit requests per minute"
    )

    # JWT settings
    jwt_secret_key: str = Field(
        default="dev-secret-key-change-in-production", description="JWT secret key"
    )
    jwt_algorithm: str = Field(default="HS256", description="JWT algorithm")
    jwt_expiration_hours: int = Field(
        default=24, description="JWT token expiration in hours"
    )

    # Database connection (inherits from database config but can override)
    db_pool_size: int = Field(default=5, description="Database pool size")
    db_max_overflow: int = Field(default=10, description="Database max overflow")

    # Logging and monitoring
    log_level: str = Field(default="INFO", description="Logging level")
    enable_sql_logging: bool = Field(
        default=False, description="Enable SQL query logging"
    )
    metrics_enabled: bool = Field(default=True, description="Enable metrics collection")

    # API Features (using feature flags)
    # These will be overridden by feature flag values at runtime
    enable_registration: bool = Field(
        default=True, description="Enable user registration"
    )
    enable_swagger_ui: bool = Field(
        default=True, description="Enable Swagger UI documentation"
    )
    enable_admin_endpoints: bool = Field(
        default=False, description="Enable admin management endpoints"
    )

    # Performance settings
    response_timeout_seconds: int = Field(
        default=30, description="Response timeout in seconds"
    )
    max_request_size_mb: int = Field(
        default=10, description="Maximum request size in MB"
    )

    # Pagination defaults
    default_page_size: int = Field(default=20, description="Default pagination size")
    max_page_size: int = Field(default=100, description="Maximum pagination size")

    @classmethod
    def get_config_name(cls) -> str:
        """Get the configuration name for file-based loading."""
        return "api"

    def is_feature_enabled(
        self,
        feature_key: str,
        user_id: str | None = None,
        user_attributes: dict[str, Any] | None = None,
    ) -> bool:
        """Check if a feature is enabled via feature flags."""
        return feature_flag_manager.is_enabled(
            feature_key,
            user_id=user_id,
            user_attributes=user_attributes,
            environment=self.environment.value,
            default=getattr(self, feature_key, False),
        )

    def get_feature_value(
        self,
        feature_key: str,
        user_id: str | None = None,
        user_attributes: dict[str, Any] | None = None,
        default: Any = None,
    ) -> Any:
        """Get feature value from feature flags."""
        return feature_flag_manager.get_value(
            feature_key,
            user_id=user_id,
            user_attributes=user_attributes,
            environment=self.environment.value,
            default=default,
        )

    def setup_feature_flags(self) -> None:
        """Initialize feature flags for API configuration."""
        # User registration feature flag
        feature_flag_manager.create_flag(
            key="enable_registration",
            name="User Registration",
            description="Enable user registration endpoint",
            default_value=self.enable_registration,
            environments={"development", "staging", "production"},
        )

        # Admin endpoints feature flag
        feature_flag_manager.create_flag(
            key="enable_admin_endpoints",
            name="Admin Endpoints",
            description="Enable administrative management endpoints",
            default_value=self.enable_admin_endpoints,
            environments={"development", "staging"},  # Not in production by default
        )

        # Documentation feature flag
        feature_flag_manager.create_flag(
            key="enable_swagger_ui",
            name="Swagger UI Documentation",
            description="Enable interactive API documentation",
            default_value=self.enable_swagger_ui,
            environments={"development", "staging", "production"},
        )

        # Rate limiting feature flag with percentage rollout
        feature_flag_manager.create_flag(
            key="enhanced_rate_limiting",
            name="Enhanced Rate Limiting",
            description="Enable advanced rate limiting with per-user limits",
            default_value=False,
            environments={"development", "staging", "production"},
        )

        # A/B test for new authentication flow
        auth_ab_test = ABTestConfig(
            name="new_auth_flow",
            description="Test new authentication flow",
            variants={
                "control": False,
                "treatment": True,
            },
            traffic_allocation={
                "control": 80.0,
                "treatment": 20.0,
            },
        )

        feature_flag_manager.create_flag(
            key="new_auth_flow",
            name="New Authentication Flow",
            description="Enable new authentication flow with improved UX",
            default_value=False,
            environments={"staging", "production"},
        )

        # Update the flag with A/B test config
        feature_flag_manager.update_flag(
            "new_auth_flow",
            {"ab_test": auth_ab_test.model_dump()},
        )

    def validate_configuration(self) -> list[str]:
        """Validate API configuration and return issues."""
        issues = super().validate_configuration()

        # API-specific validation
        if self.environment == Environment.PRODUCTION:
            if self.jwt_secret_key == "dev-secret-key-change-in-production":
                issues.append("JWT secret key must be changed in production")

            if "*" in self.cors_origins:
                issues.append("CORS origins should be restricted in production")

            if self.enable_sql_logging:
                issues.append("SQL logging should be disabled in production")

        if self.port < 1 or self.port > 65535:
            issues.append("Port must be between 1 and 65535")

        if self.rate_limit_per_minute <= 0:
            issues.append("Rate limit must be positive")

        if self.jwt_expiration_hours <= 0:
            issues.append("JWT expiration must be positive")

        if self.default_page_size > self.max_page_size:
            issues.append("Default page size cannot be larger than max page size")

        return issues

    def get_runtime_config(
        self, user_id: str | None = None, user_attributes: dict[str, Any] | None = None
    ) -> dict[str, Any]:
        """Get runtime configuration with feature flag overrides."""
        config = self.to_dict(exclude_sensitive=True)

        # Override with feature flag values
        feature_overrides = {
            "enable_registration": self.is_feature_enabled(
                "enable_registration", user_id, user_attributes
            ),
            "enable_admin_endpoints": self.is_feature_enabled(
                "enable_admin_endpoints", user_id, user_attributes
            ),
            "enable_swagger_ui": self.is_feature_enabled(
                "enable_swagger_ui", user_id, user_attributes
            ),
            "enhanced_rate_limiting": self.is_feature_enabled(
                "enhanced_rate_limiting", user_id, user_attributes
            ),
            "new_auth_flow": self.is_feature_enabled(
                "new_auth_flow", user_id, user_attributes
            ),
        }

        config.update(feature_overrides)
        return config


async def get_api_config(
    environment: Environment = Environment.DEVELOPMENT,
) -> APIConfiguration:
    """Get API configuration for the specified environment."""
    config = await APIConfiguration.load_from_environment(environment)
    config.setup_feature_flags()
    return config
