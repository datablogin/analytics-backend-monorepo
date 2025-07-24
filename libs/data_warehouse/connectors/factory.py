"""
Factory for creating data warehouse connectors.

This module provides a factory pattern for instantiating the appropriate
connector based on warehouse type and configuration.
"""

from typing import Any

from .base import DataWarehouseConnector, WarehouseType
from .bigquery import BigQueryConnector
from .redshift import RedshiftConnector
from .snowflake import SnowflakeConnector


class ConnectorFactory:
    """Factory for creating data warehouse connectors."""

    _connectors = {
        WarehouseType.SNOWFLAKE: SnowflakeConnector,
        WarehouseType.BIGQUERY: BigQueryConnector,
        WarehouseType.REDSHIFT: RedshiftConnector,
    }

    @classmethod
    def create_connector(
        cls, warehouse_type: WarehouseType, connection_params: dict[str, Any]
    ) -> DataWarehouseConnector:
        """
        Create a connector instance for the specified warehouse type.

        Args:
            warehouse_type: Type of data warehouse
            connection_params: Connection configuration parameters

        Returns:
            DataWarehouseConnector: Configured connector instance

        Raises:
            ValueError: If warehouse type is not supported
        """
        if warehouse_type not in cls._connectors:
            supported = ", ".join([wt.value for wt in cls._connectors.keys()])
            raise ValueError(
                f"Unsupported warehouse type: {warehouse_type}. "
                f"Supported types: {supported}"
            )

        connector_class = cls._connectors[warehouse_type]
        # Type ignore because mypy can't infer that this is always a concrete implementation
        return connector_class(connection_params)  # type: ignore

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
        """
        cls._connectors[warehouse_type] = connector_class

    @classmethod
    def get_supported_types(cls) -> list[WarehouseType]:
        """Get list of supported warehouse types."""
        return list(cls._connectors.keys())
