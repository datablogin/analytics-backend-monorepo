"""
Enhanced type-safe configuration classes for data warehouse connectors.

This module provides improved type safety with proper validation,
enums, and constraints for all connector configurations.
"""

from abc import ABC
from enum import Enum
from typing import Any

from pydantic import (
    BaseModel,
    Field,
    SecretStr,
    field_validator,
    model_validator,
)


class SSLMode(str, Enum):
    """SSL connection modes for database connections."""

    REQUIRE = "require"
    PREFER = "prefer"
    ALLOW = "allow"
    DISABLE = "disable"


class BigQueryLocation(str, Enum):
    """BigQuery dataset locations."""

    US = "US"
    EU = "EU"
    ASIA_EAST1 = "asia-east1"
    ASIA_NORTHEAST1 = "asia-northeast1"
    ASIA_SOUTHEAST1 = "asia-southeast1"
    AUSTRALIA_SOUTHEAST1 = "australia-southeast1"
    EUROPE_NORTH1 = "europe-north1"
    EUROPE_WEST1 = "europe-west1"
    EUROPE_WEST2 = "europe-west2"
    EUROPE_WEST3 = "europe-west3"
    EUROPE_WEST4 = "europe-west4"
    EUROPE_WEST6 = "europe-west6"
    NORTHAMERICA_NORTHEAST1 = "northamerica-northeast1"
    SOUTHAMERICA_EAST1 = "southamerica-east1"
    US_CENTRAL1 = "us-central1"
    US_EAST1 = "us-east1"
    US_EAST4 = "us-east4"
    US_WEST1 = "us-west1"
    US_WEST2 = "us-west2"
    US_WEST3 = "us-west3"
    US_WEST4 = "us-west4"


class BaseConnectionConfig(BaseModel, ABC):
    """
    Base configuration class for all data warehouse connectors.

    Provides common validation and configuration patterns.
    """

    # Common timeout settings
    connection_timeout: int = Field(
        default=60, ge=1, le=300, description="Connection timeout in seconds"
    )

    # Common SSL settings for applicable connectors
    ssl: bool = Field(default=True, description="Enable SSL connection")

    class Config:
        """Pydantic configuration."""

        extra = "forbid"  # Prevent extra fields
        str_strip_whitespace = True  # Strip whitespace from strings
        validate_assignment = True  # Validate on field assignment


class SnowflakeConfig(BaseConnectionConfig):
    """Enhanced type-safe configuration for Snowflake connection."""

    # Required fields
    account: str = Field(..., min_length=1, description="Snowflake account identifier")
    user: str = Field(..., min_length=1, description="Username for authentication")

    # Authentication (either password or private key required)
    password: SecretStr | None = Field(
        default=None, description="Password for authentication"
    )
    private_key: SecretStr | None = Field(
        default=None, description="Private key for key-pair authentication"
    )
    private_key_passphrase: SecretStr | None = Field(
        default=None, description="Passphrase for encrypted private key"
    )

    # Optional connection parameters
    database: str | None = Field(
        default=None, min_length=1, description="Default database"
    )
    schema: str | None = Field(default=None, min_length=1, description="Default schema")
    warehouse: str | None = Field(
        default=None, min_length=1, description="Virtual warehouse to use"
    )
    role: str | None = Field(default=None, min_length=1, description="Role to assume")

    # Connection behavior
    autocommit: bool = Field(default=True, description="Enable autocommit mode")
    network_timeout: int = Field(
        default=300, ge=30, le=3600, description="Network timeout in seconds"
    )
    client_session_keep_alive: bool = Field(
        default=True, description="Keep client session alive"
    )

    @model_validator(mode="after")
    def validate_authentication(self) -> "SnowflakeConfig":
        """Ensure either password or private key is provided."""
        if not self.password and not self.private_key:
            raise ValueError("Either password or private_key must be provided")
        return self

    def get_connection_params(self) -> dict[str, Any]:
        """Get connection parameters for snowflake-connector-python."""
        params = {
            "account": self.account,
            "user": self.user,
            "autocommit": self.autocommit,
            "connection_timeout": self.connection_timeout,
            "network_timeout": self.network_timeout,
            "client_session_keep_alive": self.client_session_keep_alive,
        }

        # Add authentication
        if self.password:
            params["password"] = self.password.get_secret_value()
        elif self.private_key:
            params["private_key"] = self.private_key.get_secret_value()
            if self.private_key_passphrase:
                params["private_key_passphrase"] = (
                    self.private_key_passphrase.get_secret_value()
                )

        # Add optional parameters
        if self.database:
            params["database"] = self.database
        if self.schema:
            params["schema"] = self.schema
        if self.warehouse:
            params["warehouse"] = self.warehouse
        if self.role:
            params["role"] = self.role

        return params


class BigQueryConfig(BaseConnectionConfig):
    """Enhanced type-safe configuration for BigQuery connection."""

    project_id: str = Field(..., min_length=1, description="Google Cloud project ID")

    # Authentication (either credentials_path or credentials_json)
    credentials_path: str | None = Field(
        default=None, description="Path to service account JSON file"
    )
    credentials_json: dict[str, Any] | None = Field(
        default=None, description="Service account credentials as JSON object"
    )

    # Configuration options
    location: BigQueryLocation = Field(
        default=BigQueryLocation.US,
        description="Default location for datasets and jobs",
    )
    default_dataset: str | None = Field(
        default=None, min_length=1, description="Default dataset for queries"
    )
    maximum_bytes_billed: int | None = Field(
        default=None, ge=0, description="Maximum bytes that can be billed for queries"
    )
    use_legacy_sql: bool = Field(default=False, description="Use legacy SQL syntax")

    # Timeouts
    job_timeout: int = Field(
        default=300, ge=1, le=3600, description="Job execution timeout in seconds"
    )
    job_retry_timeout: int = Field(
        default=600, ge=1, le=3600, description="Job retry timeout in seconds"
    )

    @model_validator(mode="after")
    def validate_credentials(self) -> "BigQueryConfig":
        """Ensure credentials are provided in some form."""
        if not self.credentials_path and not self.credentials_json:
            raise ValueError(
                "Either credentials_path or credentials_json must be provided"
            )
        return self

    @field_validator("credentials_path")
    @classmethod
    def validate_credentials_path(cls, v: str | None) -> str | None:
        """Validate credentials file path."""
        if v is not None:
            if not v.endswith(".json"):
                raise ValueError("credentials_path must be a JSON file")
        return v


class RedshiftConfig(BaseConnectionConfig):
    """Enhanced type-safe configuration for Redshift connection."""

    # Required connection parameters
    host: str = Field(..., min_length=1, description="Redshift cluster endpoint")
    database: str = Field(..., min_length=1, description="Database name")
    user: str = Field(..., min_length=1, description="Username for authentication")

    # Network configuration
    port: int = Field(default=5439, ge=1, le=65535, description="Port number")

    # Authentication
    password: SecretStr | None = Field(
        default=None, description="Password for authentication"
    )
    cluster_identifier: str | None = Field(
        default=None,
        min_length=1,
        description="Cluster identifier for IAM authentication",
    )
    db_user: str | None = Field(
        default=None, min_length=1, description="Database user for IAM authentication"
    )
    iam: bool = Field(default=False, description="Use IAM authentication")

    # SSL configuration
    sslmode: SSLMode = Field(default=SSLMode.REQUIRE, description="SSL connection mode")

    # Connection options
    schema: str = Field(default="public", min_length=1, description="Default schema")
    application_name: str = Field(
        default="analytics-backend",
        min_length=1,
        description="Application name for connection tracking",
    )

    @model_validator(mode="after")
    def validate_authentication(self) -> "RedshiftConfig":
        """Validate authentication configuration."""
        if self.iam:
            if not self.cluster_identifier:
                raise ValueError(
                    "cluster_identifier is required for IAM authentication"
                )
            if not self.db_user:
                raise ValueError("db_user is required for IAM authentication")
        else:
            if not self.password:
                raise ValueError("password is required for non-IAM authentication")
        return self

    def get_connection_params(self) -> dict[str, Any]:
        """Get connection parameters for redshift_connector."""
        params = {
            "host": self.host,
            "port": self.port,
            "database": self.database,
            "user": self.user,
            "sslmode": self.sslmode.value,
            "application_name": self.application_name,
            "connect_timeout": self.connection_timeout,
        }

        if self.iam:
            params.update(
                {
                    "cluster_identifier": self.cluster_identifier,
                    "db_user": self.db_user,
                    "iam": True,
                }
            )
        else:
            params["password"] = self.password.get_secret_value()

        return params


# Type alias for any connector configuration
ConnectorConfig = SnowflakeConfig | BigQueryConfig | RedshiftConfig

# Mapping from warehouse types to config classes
WAREHOUSE_CONFIG_MAP: dict[str, type[BaseConnectionConfig]] = {
    "snowflake": SnowflakeConfig,
    "bigquery": BigQueryConfig,
    "redshift": RedshiftConfig,
}
