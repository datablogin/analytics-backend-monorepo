"""
Factory for creating data warehouse connectors.

This module provides a factory pattern for instantiating the appropriate
connector based on warehouse type and configuration with enhanced type safety.
"""

from typing import Any

import structlog

from .base import DataWarehouseConnector, WarehouseType
from .bigquery import BigQueryConnector
from .config import (
    WAREHOUSE_CONFIG_MAP,
    BaseConnectionConfig,
)
from .redshift import RedshiftConnector
from .snowflake import SnowflakeConnector


class ConnectorFactory:
    """Factory for creating data warehouse connectors."""

    logger = structlog.get_logger(__name__)

    _connectors = {
        WarehouseType.SNOWFLAKE: SnowflakeConnector,
        WarehouseType.BIGQUERY: BigQueryConnector,
        WarehouseType.REDSHIFT: RedshiftConnector,
    }

    @classmethod
    def create_connector_typed(
        cls, warehouse_type: WarehouseType, config: BaseConnectionConfig
    ) -> DataWarehouseConnector:
        """
        Create a connector instance using a typed configuration object.

        Args:
            warehouse_type: Type of data warehouse
            config: Typed configuration object

        Returns:
            DataWarehouseConnector: Configured connector instance

        Raises:
            ValueError: If warehouse type is not supported or config type mismatch
            TypeError: If config is not the correct type for warehouse_type
            RuntimeError: If connector instantiation fails
        """
        # Validate inputs
        if not isinstance(warehouse_type, WarehouseType):
            cls.logger.error(
                "invalid_warehouse_type",
                warehouse_type=warehouse_type,
                type=type(warehouse_type),
            )
            raise TypeError(
                f"warehouse_type must be WarehouseType enum, got {type(warehouse_type)}"
            )

        if not isinstance(config, BaseConnectionConfig):
            cls.logger.error("invalid_config_type", config_type=type(config))
            raise TypeError(
                f"config must be BaseConnectionConfig subclass, got {type(config)}"
            )

        # Check if warehouse type is supported
        if warehouse_type not in cls._connectors:
            supported = ", ".join([wt.value for wt in cls._connectors.keys()])
            cls.logger.error(
                "unsupported_warehouse_type",
                warehouse_type=warehouse_type.value,
                supported_types=list(cls._connectors.keys()),
            )
            raise ValueError(
                f"Unsupported warehouse type: {warehouse_type.value}. "
                f"Supported types: {supported}"
            )

        # Validate config type matches warehouse type
        expected_config_type = WAREHOUSE_CONFIG_MAP.get(warehouse_type.value)
        if expected_config_type and not isinstance(config, expected_config_type):
            cls.logger.error(
                "config_type_mismatch",
                warehouse_type=warehouse_type.value,
                expected_type=expected_config_type.__name__,
                actual_type=type(config).__name__,
            )
            raise TypeError(
                f"Config type mismatch for {warehouse_type.value}: "
                f"expected {expected_config_type.__name__}, got {type(config).__name__}"
            )

        # Convert typed config to dict for legacy connector compatibility
        if hasattr(config, "get_connection_params"):
            connection_params = config.get_connection_params()
        else:
            connection_params = config.model_dump(exclude_unset=True)

        # Attempt to create connector with proper error handling
        connector_class = cls._connectors[warehouse_type]
        try:
            cls.logger.info(
                "creating_connector_typed",
                warehouse_type=warehouse_type.value,
                connector_class=connector_class.__name__,
                config_type=type(config).__name__,
            )

            connector = connector_class(connection_params)  # type: ignore[abstract]

            cls.logger.info(
                "connector_created_successfully",
                warehouse_type=warehouse_type.value,
                connector_id=id(connector),
            )

            return connector

        except Exception as e:
            cls.logger.error(
                "connector_creation_failed",
                warehouse_type=warehouse_type.value,
                connector_class=connector_class.__name__,
                config_type=type(config).__name__,
                error=str(e),
                error_type=type(e).__name__,
            )
            raise RuntimeError(
                f"Failed to create {warehouse_type.value} connector: {str(e)}"
            ) from e

    @classmethod
    def create_connector_from_dict(
        cls, warehouse_type: WarehouseType, connection_params: dict[str, Any]
    ) -> DataWarehouseConnector:
        """
        Create a connector instance using a typed configuration parsed from dict.

        This method provides type safety by parsing the connection parameters
        into the appropriate typed configuration class before creating the connector.

        Args:
            warehouse_type: Type of data warehouse
            connection_params: Connection configuration parameters as dict

        Returns:
            DataWarehouseConnector: Configured connector instance

        Raises:
            ValueError: If warehouse type is not supported or config validation fails
            TypeError: If connection_params is not a dictionary
            RuntimeError: If connector instantiation fails
        """
        # Validate inputs
        if not isinstance(warehouse_type, WarehouseType):
            cls.logger.error(
                "invalid_warehouse_type",
                warehouse_type=warehouse_type,
                type=type(warehouse_type),
            )
            raise TypeError(
                f"warehouse_type must be WarehouseType enum, got {type(warehouse_type)}"
            )

        if not isinstance(connection_params, dict):
            cls.logger.error(
                "invalid_connection_params",
                connection_params_type=type(connection_params),
            )
            raise TypeError(
                f"connection_params must be dict, got {type(connection_params)}"
            )

        if not connection_params:
            cls.logger.error(
                "empty_connection_params", warehouse_type=warehouse_type.value
            )
            raise ValueError("connection_params cannot be empty")

        # Get the appropriate config class
        config_class = WAREHOUSE_CONFIG_MAP.get(warehouse_type.value)
        if not config_class:
            supported = ", ".join(WAREHOUSE_CONFIG_MAP.keys())
            cls.logger.error(
                "unsupported_warehouse_type_for_typed_config",
                warehouse_type=warehouse_type.value,
                supported_types=list(WAREHOUSE_CONFIG_MAP.keys()),
            )
            raise ValueError(
                f"Unsupported warehouse type for typed config: {warehouse_type.value}. "
                f"Supported types: {supported}"
            )

        # Parse parameters into typed config
        try:
            cls.logger.info(
                "parsing_config",
                warehouse_type=warehouse_type.value,
                config_class=config_class.__name__,
            )

            config = config_class(**connection_params)

            cls.logger.info(
                "config_parsed_successfully",
                warehouse_type=warehouse_type.value,
                config_class=config_class.__name__,
            )

            # Use the typed creation method
            return cls.create_connector_typed(warehouse_type, config)

        except Exception as e:
            cls.logger.error(
                "config_parsing_failed",
                warehouse_type=warehouse_type.value,
                config_class=config_class.__name__,
                error=str(e),
                error_type=type(e).__name__,
            )
            raise ValueError(
                f"Failed to parse {warehouse_type.value} config: {str(e)}"
            ) from e

    @classmethod
    def create_connector(
        cls, warehouse_type: WarehouseType, connection_params: dict[str, Any]
    ) -> DataWarehouseConnector:
        """
        Create a connector instance for the specified warehouse type.

        DEPRECATED: Use create_connector_from_dict for better type safety.
        This method is maintained for backward compatibility and delegates
        to the typed implementation.

        Args:
            warehouse_type: Type of data warehouse
            connection_params: Connection configuration parameters

        Returns:
            DataWarehouseConnector: Configured connector instance

        Raises:
            ValueError: If warehouse type is not supported or parameters are invalid
            TypeError: If connection_params is not a dictionary
            RuntimeError: If connector instantiation fails
        """
        # Convert string to WarehouseType enum if needed
        if isinstance(warehouse_type, str):
            try:
                warehouse_type_enum = WarehouseType(warehouse_type)
            except ValueError:
                raise ValueError(f"Unsupported warehouse type: {warehouse_type}")
        else:
            warehouse_type_enum = warehouse_type

        cls.logger.warning(
            "using_deprecated_create_connector",
            warehouse_type=warehouse_type_enum.value,
            recommendation="Use create_connector_from_dict for better type safety",
        )

        # Delegate to the new typed method
        return cls.create_connector_from_dict(warehouse_type_enum, connection_params)

    @classmethod
    def register_connector(
        cls,
        warehouse_type: WarehouseType,
        connector_class: type[DataWarehouseConnector],
    ) -> None:
        """
        Register a new connector type.

        Args:
            warehouse_type: Type of data warehouse
            connector_class: Connector class to register

        Raises:
            TypeError: If inputs are not of the correct type
            ValueError: If connector_class is not a valid DataWarehouseConnector subclass
        """
        # Validate inputs
        if not isinstance(warehouse_type, WarehouseType):
            cls.logger.error(
                "invalid_warehouse_type_for_registration",
                warehouse_type=warehouse_type,
                type=type(warehouse_type),
            )
            raise TypeError(
                f"warehouse_type must be WarehouseType enum, got {type(warehouse_type)}"
            )

        if not isinstance(connector_class, type):
            cls.logger.error(
                "invalid_connector_class",
                connector_class=connector_class,
                type=type(connector_class),
            )
            raise TypeError(
                f"connector_class must be a class, got {type(connector_class)}"
            )

        # Verify it's a subclass of DataWarehouseConnector
        if not issubclass(connector_class, DataWarehouseConnector):
            cls.logger.error(
                "invalid_connector_subclass", connector_class=connector_class.__name__
            )
            raise ValueError(
                f"connector_class must be a subclass of DataWarehouseConnector, got {connector_class.__name__}"
            )

        # Log existing connector warning if overwriting
        if warehouse_type in cls._connectors:
            old_connector = cls._connectors[warehouse_type]
            cls.logger.warning(
                "overwriting_existing_connector",
                warehouse_type=warehouse_type.value,
                old_connector=old_connector.__name__,
                new_connector=connector_class.__name__,
            )

        cls._connectors[warehouse_type] = connector_class
        cls.logger.info(
            "connector_registered",
            warehouse_type=warehouse_type.value,
            connector_class=connector_class.__name__,
        )

    @classmethod
    def get_supported_types(cls) -> list[WarehouseType]:
        """Get list of supported warehouse types."""
        return list(cls._connectors.keys())

    @classmethod
    def get_config_schema(cls, warehouse_type: WarehouseType) -> dict[str, Any]:
        """
        Get the JSON schema for a warehouse type configuration.

        Args:
            warehouse_type: Type of data warehouse

        Returns:
            dict: JSON schema for the configuration

        Raises:
            ValueError: If warehouse type is not supported
        """
        config_class = WAREHOUSE_CONFIG_MAP.get(warehouse_type.value)
        if not config_class:
            supported = ", ".join(WAREHOUSE_CONFIG_MAP.keys())
            raise ValueError(
                f"Unsupported warehouse type: {warehouse_type.value}. "
                f"Supported types: {supported}"
            )

        return config_class.model_json_schema()

    @classmethod
    def validate_config(
        cls, warehouse_type: WarehouseType, connection_params: dict[str, Any]
    ) -> BaseConnectionConfig:
        """
        Validate connection parameters against the typed configuration.

        Args:
            warehouse_type: Type of data warehouse
            connection_params: Connection configuration parameters

        Returns:
            BaseConnectionConfig: Validated configuration object

        Raises:
            ValueError: If configuration is invalid
        """
        config_class = WAREHOUSE_CONFIG_MAP.get(warehouse_type.value)
        if not config_class:
            supported = ", ".join(WAREHOUSE_CONFIG_MAP.keys())
            raise ValueError(
                f"Unsupported warehouse type: {warehouse_type.value}. "
                f"Supported types: {supported}"
            )

        return config_class(**connection_params)
