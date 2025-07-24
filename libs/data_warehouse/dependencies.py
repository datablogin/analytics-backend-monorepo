"""
Dependency injection system for data warehouse components.

This module provides a clean dependency injection system to manage
data warehouse connections, engines, and caches without global state.
"""

from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from typing import Any

import structlog

from .connectors.base import DataWarehouseConnector, WarehouseType
from .olap.engine import OLAPEngine
from .pool import ConnectionPool, ConnectionPoolConfig
from .query.cache import QueryCache
from .query.federation import FederatedQueryEngine


class DataWarehouseManager:
    """
    Manager for data warehouse connections and associated components.

    Provides dependency injection for connectors, OLAP engines, caches,
    and federation engines with connection pooling support.
    """

    def __init__(self, pool_config: ConnectionPoolConfig | None = None):
        """Initialize the data warehouse manager."""
        self.logger = structlog.get_logger(__name__)
        self._connection_pools: dict[str, ConnectionPool] = {}
        self._olap_engines: dict[str, OLAPEngine] = {}
        self._query_cache = QueryCache()
        self._federation_engine = FederatedQueryEngine()
        self._default_pool_config = pool_config or ConnectionPoolConfig()

    async def create_connection_pool(
        self,
        connection_id: str,
        warehouse_type: str,
        connection_params: dict[str, Any],
        pool_config: ConnectionPoolConfig | None = None,
    ) -> ConnectionPool:
        """
        Create and register a new connection pool.

        Args:
            connection_id: Unique identifier for the connection pool
            warehouse_type: Type of data warehouse
            connection_params: Connection configuration parameters
            pool_config: Optional pool configuration override

        Returns:
            ConnectionPool: The created connection pool
        """
        self.logger.info(
            "creating_connection_pool",
            connection_id=connection_id,
            warehouse_type=warehouse_type,
        )

        wh_type = WarehouseType(warehouse_type)
        config = pool_config or self._default_pool_config

        # Create connection pool
        pool = ConnectionPool(connection_id, wh_type, connection_params, config)

        # Initialize the pool
        await pool.initialize()

        # Store the pool
        self._connection_pools[connection_id] = pool

        self.logger.info(
            "connection_pool_created",
            connection_id=connection_id,
            warehouse_type=warehouse_type,
            min_connections=config.min_connections,
            max_connections=config.max_connections,
        )

        return pool

    async def create_connection(
        self, connection_id: str, warehouse_type: str, connection_params: dict[str, Any]
    ) -> DataWarehouseConnector:
        """
        Create a connection through the pool system.

        This method maintains backward compatibility by creating a pool
        and returning a connection from it.

        Args:
            connection_id: Unique identifier for the connection
            warehouse_type: Type of data warehouse
            connection_params: Connection configuration parameters

        Returns:
            DataWarehouseConnector: A connector from the pool
        """
        if connection_id not in self._connection_pools:
            await self.create_connection_pool(
                connection_id, warehouse_type, connection_params
            )

        pool = self._connection_pools[connection_id]
        async with pool.acquire() as connector:
            return connector

    async def get_connection(
        self, connection_id: str
    ) -> object | None:
        """
        Get a connection from the pool by ID.

        Args:
            connection_id: The connection identifier

        Returns:
            AsyncContextManager yielding DataWarehouseConnector if pool found, None otherwise
        """
        pool = self._connection_pools.get(connection_id)
        if pool:
            return pool.acquire()
        return None

    async def get_connection_pool(self, connection_id: str) -> ConnectionPool | None:
        """
        Get a connection pool by ID.

        Args:
            connection_id: The connection identifier

        Returns:
            ConnectionPool if found, None otherwise
        """
        return self._connection_pools.get(connection_id)

    async def remove_connection(self, connection_id: str) -> bool:
        """
        Remove and close a connection pool.

        Args:
            connection_id: The connection identifier

        Returns:
            bool: True if pool was removed, False if not found
        """
        if connection_id not in self._connection_pools:
            return False

        pool = self._connection_pools[connection_id]
        try:
            await pool.close()
        except Exception as e:
            self.logger.warning(
                "pool_close_failed", connection_id=connection_id, error=str(e)
            )

        del self._connection_pools[connection_id]

        # Clean up associated OLAP engine
        if connection_id in self._olap_engines:
            del self._olap_engines[connection_id]

        self.logger.info("connection_pool_removed", connection_id=connection_id)
        return True

    async def get_olap_engine(self, connection_id: str) -> OLAPEngine | None:
        """
        Get or create an OLAP engine for a connection pool.

        Args:
            connection_id: The connection identifier

        Returns:
            OLAPEngine if connection pool exists, None otherwise
        """
        if connection_id not in self._connection_pools:
            return None

        if connection_id not in self._olap_engines:
            # Create OLAP engine with a pooled connector
            pool = self._connection_pools[connection_id]
            # For OLAP engines, we'll use a simple connector proxy that manages pool access
            from .connectors.pool_proxy import PoolProxyConnector

            proxy_connector = PoolProxyConnector(pool)
            self._olap_engines[connection_id] = OLAPEngine(proxy_connector)
            self.logger.info("olap_engine_created", connection_id=connection_id)

        return self._olap_engines[connection_id]

    def get_query_cache(self) -> QueryCache:
        """Get the shared query cache instance."""
        return self._query_cache

    def get_federation_engine(self) -> FederatedQueryEngine:
        """Get the shared federation engine instance."""
        return self._federation_engine

    async def cleanup(self) -> None:
        """Clean up all connection pools and resources."""
        self.logger.info(
            "cleaning_up_connection_pools", count=len(self._connection_pools)
        )

        for connection_id in list(self._connection_pools.keys()):
            await self.remove_connection(connection_id)

        self._olap_engines.clear()
        self._query_cache.clear()

    def get_pool_stats(self) -> dict[str, Any]:
        """Get statistics for all connection pools."""
        return {
            pool_id: pool.get_stats()
            for pool_id, pool in self._connection_pools.items()
        }


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
