"""
Pool proxy connector for OLAP engines.

This module provides a proxy connector that manages connection pool access
for components like OLAP engines that need persistent access to connections.
"""

from typing import Any

from ..pool import ConnectionPool
from .base import (
    ConnectionStatus,
    DataWarehouseConnector,
    QueryResult,
    SchemaInfo,
    TableInfo,
    WarehouseType,
)


class PoolProxyConnector(DataWarehouseConnector):
    """
    Proxy connector that manages pool access for persistent components.

    This connector provides a stable interface for components like OLAP engines
    while managing the underlying connection pool access transparently.
    """

    def __init__(self, pool: ConnectionPool):
        """
        Initialize the pool proxy connector.

        Args:
            pool: The connection pool to proxy
        """
        # Initialize with dummy params since we won't use them
        super().__init__({})
        self.pool = pool

    def _get_warehouse_type(self) -> WarehouseType:
        """Return the warehouse type from the pool."""
        return self.pool.warehouse_type

    @property
    def status(self) -> ConnectionStatus:
        """Get connection status based on pool state."""
        if self.pool._closed:
            return ConnectionStatus.DISCONNECTED
        elif len(self.pool._connections) > 0:
            return ConnectionStatus.CONNECTED
        else:
            return ConnectionStatus.CONNECTING

    async def connect(self) -> None:
        """Connect method - no-op since pool manages connections."""
        pass

    async def disconnect(self) -> None:
        """Disconnect method - no-op since pool manages connections."""
        pass

    async def test_connection(self) -> bool:
        """Test connection health using pool."""
        if self.pool._closed:
            return False

        try:
            async with self.pool.acquire() as connector:
                return await connector.test_connection()
        except Exception:
            return False

    async def execute_query(
        self,
        query: str,
        params: dict[str, Any] | None = None,
        timeout: int | None = None,
    ) -> QueryResult:
        """Execute query using connection from pool."""
        async with self.pool.acquire() as connector:
            return await connector.execute_query(query, params, timeout)

    async def execute_async_query(
        self, query: str, params: dict[str, Any] | None = None
    ) -> str:
        """Execute async query using connection from pool."""
        async with self.pool.acquire() as connector:
            return await connector.execute_async_query(query, params)

    async def get_query_status(self, query_id: str) -> dict[str, Any]:
        """Get query status using connection from pool."""
        async with self.pool.acquire() as connector:
            return await connector.get_query_status(query_id)

    async def get_query_result(self, query_id: str) -> QueryResult:
        """Get query result using connection from pool."""
        async with self.pool.acquire() as connector:
            return await connector.get_query_result(query_id)

    async def get_schema_info(
        self, schema_name: str | None = None
    ) -> SchemaInfo | list[SchemaInfo]:
        """Get schema info using connection from pool."""
        async with self.pool.acquire() as connector:
            return await connector.get_schema_info(schema_name)

    async def get_table_info(
        self, table_name: str, schema_name: str | None = None
    ) -> TableInfo:
        """Get table info using connection from pool."""
        async with self.pool.acquire() as connector:
            return await connector.get_table_info(table_name, schema_name)

    async def get_table_sample(
        self, table_name: str, schema_name: str | None = None, limit: int = 100
    ) -> QueryResult:
        """Get table sample using connection from pool."""
        async with self.pool.acquire() as connector:
            return await connector.get_table_sample(table_name, schema_name, limit)
