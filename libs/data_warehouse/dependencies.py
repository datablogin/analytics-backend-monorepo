"""
Dependency injection system for data warehouse components.

This module provides a clean dependency injection system to manage
data warehouse connections, engines, and caches without global state.
"""

from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from typing import Any

import structlog

from .connectors.base import DataWarehouseConnector
from .connectors.factory import ConnectorFactory
from .olap.engine import OLAPEngine
from .query.cache import QueryCache
from .query.federation import FederatedQueryEngine


class DataWarehouseManager:
    """
    Manager for data warehouse connections and associated components.
    
    Provides dependency injection for connectors, OLAP engines, caches,
    and federation engines without relying on global state.
    """

    def __init__(self):
        """Initialize the data warehouse manager."""
        self.logger = structlog.get_logger(__name__)
        self._connections: dict[str, DataWarehouseConnector] = {}
        self._olap_engines: dict[str, OLAPEngine] = {}
        self._query_cache = QueryCache()
        self._federation_engine = FederatedQueryEngine()

    async def create_connection(
        self,
        connection_id: str,
        warehouse_type: str,
        connection_params: dict[str, Any]
    ) -> DataWarehouseConnector:
        """
        Create and register a new data warehouse connection.
        
        Args:
            connection_id: Unique identifier for the connection
            warehouse_type: Type of data warehouse
            connection_params: Connection configuration parameters
            
        Returns:
            DataWarehouseConnector: The created connector
        """
        self.logger.info("creating_connection",
                        connection_id=connection_id,
                        warehouse_type=warehouse_type)

        # Create connector using factory
        from .connectors.base import WarehouseType
        wh_type = WarehouseType(warehouse_type)
        connector = ConnectorFactory.create_connector(wh_type, connection_params)

        # Connect to the warehouse
        await connector.connect()

        # Store the connection
        self._connections[connection_id] = connector

        self.logger.info("connection_created",
                        connection_id=connection_id,
                        status=connector.status.value)

        return connector

    async def get_connection(self, connection_id: str) -> DataWarehouseConnector | None:
        """
        Get an existing connection by ID.
        
        Args:
            connection_id: The connection identifier
            
        Returns:
            DataWarehouseConnector if found, None otherwise
        """
        return self._connections.get(connection_id)

    async def remove_connection(self, connection_id: str) -> bool:
        """
        Remove and disconnect a connection.
        
        Args:
            connection_id: The connection identifier
            
        Returns:
            bool: True if connection was removed, False if not found
        """
        if connection_id not in self._connections:
            return False

        connector = self._connections[connection_id]
        try:
            await connector.disconnect()
        except Exception as e:
            self.logger.warning("disconnect_failed",
                              connection_id=connection_id,
                              error=str(e))

        del self._connections[connection_id]

        # Clean up associated OLAP engine
        if connection_id in self._olap_engines:
            del self._olap_engines[connection_id]

        self.logger.info("connection_removed", connection_id=connection_id)
        return True

    async def get_olap_engine(self, connection_id: str) -> OLAPEngine | None:
        """
        Get or create an OLAP engine for a connection.
        
        Args:
            connection_id: The connection identifier
            
        Returns:
            OLAPEngine if connection exists, None otherwise
        """
        if connection_id not in self._connections:
            return None

        if connection_id not in self._olap_engines:
            connector = self._connections[connection_id]
            self._olap_engines[connection_id] = OLAPEngine(connector)
            self.logger.info("olap_engine_created", connection_id=connection_id)

        return self._olap_engines[connection_id]

    def get_query_cache(self) -> QueryCache:
        """Get the shared query cache instance."""
        return self._query_cache

    def get_federation_engine(self) -> FederatedQueryEngine:
        """Get the shared federation engine instance."""
        return self._federation_engine

    async def cleanup(self) -> None:
        """Clean up all connections and resources."""
        self.logger.info("cleaning_up_connections", count=len(self._connections))

        for connection_id in list(self._connections.keys()):
            await self.remove_connection(connection_id)

        self._olap_engines.clear()
        self._query_cache.clear()


# Global instance for FastAPI dependency injection
_manager_instance: DataWarehouseManager | None = None


def get_data_warehouse_manager() -> DataWarehouseManager:
    """
    Get the global DataWarehouseManager instance.
    
    This function serves as a FastAPI dependency to provide
    access to the data warehouse manager throughout the application.
    
    Returns:
        DataWarehouseManager: The global manager instance
    """
    global _manager_instance
    if _manager_instance is None:
        _manager_instance = DataWarehouseManager()
    return _manager_instance


@asynccontextmanager
async def data_warehouse_lifespan() -> AsyncGenerator[DataWarehouseManager, None]:
    """
    Context manager for managing data warehouse lifecycle.
    
    This can be used with FastAPI's lifespan events to ensure
    proper cleanup of connections when the application shuts down.
    
    Yields:
        DataWarehouseManager: The manager instance
    """
    manager = get_data_warehouse_manager()
    try:
        yield manager
    finally:
        await manager.cleanup()
