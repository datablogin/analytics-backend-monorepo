"""
Connection pooling for data warehouse connectors.

This module provides connection pooling functionality to improve
performance and resource utilization for data warehouse connections.
"""

import asyncio
import time
from contextlib import AbstractAsyncContextManager
from datetime import datetime
from enum import Enum
from typing import Any

import structlog
from pydantic import BaseModel

from .connectors.base import DataWarehouseConnector, WarehouseType
from .connectors.factory import ConnectorFactory


class PoolConnectionState(str, Enum):
    """State of a pooled connection."""

    AVAILABLE = "available"
    IN_USE = "in_use"
    VALIDATING = "validating"
    INVALID = "invalid"
    CLOSED = "closed"


class PooledConnection:
    """A wrapper for a connection in the pool."""

    def __init__(self, connector: DataWarehouseConnector):
        self.connector = connector
        self.state = PoolConnectionState.AVAILABLE
        self.created_at = datetime.now()
        self.last_used = datetime.now()
        self.last_validated = datetime.now()
        self.use_count = 0
        self.pool_id: str | None = None

    def mark_in_use(self) -> None:
        """Mark connection as in use."""
        self.state = PoolConnectionState.IN_USE
        self.last_used = datetime.now()
        self.use_count += 1

    def mark_available(self) -> None:
        """Mark connection as available."""
        self.state = PoolConnectionState.AVAILABLE

    def mark_invalid(self) -> None:
        """Mark connection as invalid."""
        self.state = PoolConnectionState.INVALID

    async def validate(self) -> bool:
        """Validate the connection health."""
        self.state = PoolConnectionState.VALIDATING
        try:
            is_healthy = await self.connector.test_connection()
            if is_healthy:
                self.state = PoolConnectionState.AVAILABLE
                self.last_validated = datetime.now()
                return True
            else:
                self.state = PoolConnectionState.INVALID
                return False
        except Exception:
            self.state = PoolConnectionState.INVALID
            return False

    def is_expired(self, max_age_seconds: int) -> bool:
        """Check if connection has exceeded max age."""
        age = (datetime.now() - self.created_at).total_seconds()
        return age > max_age_seconds

    def needs_validation(self, validation_interval_seconds: int) -> bool:
        """Check if connection needs validation."""
        time_since_validation = (datetime.now() - self.last_validated).total_seconds()
        return time_since_validation > validation_interval_seconds


class ConnectionPoolConfig(BaseModel):
    """Configuration for connection pool."""

    min_connections: int = 1
    max_connections: int = 10
    max_idle_time_seconds: int = 300  # 5 minutes
    max_connection_age_seconds: int = 3600  # 1 hour
    validation_interval_seconds: int = 30  # 30 seconds
    acquire_timeout_seconds: int = 30
    connection_timeout_seconds: int = 10
    health_check_interval_seconds: int = 60
    retry_attempts: int = 3
    retry_delay_seconds: float = 1.0


class ConnectionPool:
    """
    Connection pool for data warehouse connectors.

    Manages a pool of connections for efficient reuse and resource management.
    """

    def __init__(
        self,
        pool_id: str,
        warehouse_type: WarehouseType,
        connection_params: dict[str, Any],
        config: ConnectionPoolConfig | None = None,
    ):
        self.pool_id = pool_id
        self.warehouse_type = warehouse_type
        self.connection_params = connection_params
        self.config = config or ConnectionPoolConfig()

        self._connections: list[PooledConnection] = []
        self._lock = asyncio.Lock()
        self._condition = asyncio.Condition(self._lock)
        self._closed = False
        self._health_check_task: asyncio.Task | None = None

        self.logger = structlog.get_logger(__name__).bind(
            pool_id=pool_id, warehouse_type=warehouse_type.value
        )

    async def initialize(self) -> None:
        """Initialize the connection pool."""
        async with self._lock:
            if self._closed:
                raise RuntimeError("Cannot initialize closed pool")

            # Create minimum connections
            for _ in range(self.config.min_connections):
                try:
                    await self._create_connection()
                except Exception as e:
                    self.logger.error(
                        "failed_to_create_initial_connection", error=str(e)
                    )

            # Start health check task
            self._health_check_task = asyncio.create_task(self._health_check_loop())

            self.logger.info(
                "pool_initialized",
                min_connections=self.config.min_connections,
                current_connections=len(self._connections),
            )

    async def _create_connection(self) -> PooledConnection:
        """Create a new pooled connection."""
        connector = ConnectorFactory.create_connector_from_dict(
            self.warehouse_type, self.connection_params
        )

        # Connect with timeout
        try:
            await asyncio.wait_for(
                connector.connect(), timeout=self.config.connection_timeout_seconds
            )
        except TimeoutError:
            raise ConnectionError(
                f"Connection timeout after {self.config.connection_timeout_seconds}s"
            )

        pooled_conn = PooledConnection(connector)
        pooled_conn.pool_id = self.pool_id
        self._connections.append(pooled_conn)

        self.logger.debug(
            "connection_created", total_connections=len(self._connections)
        )

        return pooled_conn

    def acquire(self) -> AbstractAsyncContextManager[DataWarehouseConnector]:
        """
        Acquire a connection from the pool.

        Returns an async context manager that automatically releases
        the connection back to the pool when done.
        """
        return PooledConnectionManager(self)

    async def _acquire_connection(self) -> PooledConnection:
        """Internal method to acquire a connection."""
        if self._closed:
            raise RuntimeError("Cannot acquire from closed pool")

        start_time = time.time()

        async with self._condition:
            while True:
                # Check for available connection
                for conn in self._connections:
                    if conn.state == PoolConnectionState.AVAILABLE:
                        # Validate if needed
                        if conn.needs_validation(
                            self.config.validation_interval_seconds
                        ):
                            if not await conn.validate():
                                continue

                        conn.mark_in_use()
                        self.logger.debug(
                            "connection_acquired",
                            connection_age_seconds=(
                                datetime.now() - conn.created_at
                            ).total_seconds(),
                            use_count=conn.use_count,
                        )
                        return conn

                # Try to create new connection if under max
                if len(self._connections) < self.config.max_connections:
                    try:
                        conn = await self._create_connection()
                        conn.mark_in_use()
                        return conn
                    except Exception as e:
                        self.logger.error("failed_to_create_connection", error=str(e))

                # Check timeout
                elapsed = time.time() - start_time
                if elapsed >= self.config.acquire_timeout_seconds:
                    raise TimeoutError(
                        f"Failed to acquire connection within {self.config.acquire_timeout_seconds}s"
                    )

                # Wait for connection to become available
                try:
                    await asyncio.wait_for(
                        self._condition.wait(),
                        timeout=self.config.acquire_timeout_seconds - elapsed,
                    )
                except TimeoutError:
                    raise TimeoutError(
                        f"Failed to acquire connection within {self.config.acquire_timeout_seconds}s"
                    )

    async def _release_connection(self, conn: PooledConnection) -> None:
        """Internal method to release a connection back to the pool."""
        async with self._condition:
            if conn.state == PoolConnectionState.IN_USE:
                # Check if connection should be removed
                if (
                    conn.is_expired(self.config.max_connection_age_seconds)
                    or len(self._connections) > self.config.min_connections
                ):
                    await self._remove_connection(conn)
                else:
                    conn.mark_available()

                # Notify waiting acquirers
                self._condition.notify()

    async def _remove_connection(self, conn: PooledConnection) -> None:
        """Remove and close a connection."""
        if conn in self._connections:
            self._connections.remove(conn)

        conn.state = PoolConnectionState.CLOSED

        try:
            await conn.connector.disconnect()
        except Exception as e:
            self.logger.error("error_closing_connection", error=str(e))

        self.logger.debug(
            "connection_removed", total_connections=len(self._connections)
        )

    async def _health_check_loop(self) -> None:
        """Background task to maintain pool health."""
        while not self._closed:
            try:
                await self._health_check()
                await asyncio.sleep(self.config.health_check_interval_seconds)
            except Exception as e:
                self.logger.error("health_check_error", error=str(e))
                await asyncio.sleep(self.config.health_check_interval_seconds)

    async def _health_check(self) -> None:
        """Perform health check on pool connections."""
        async with self._lock:
            connections_to_remove = []

            for conn in self._connections:
                if conn.state == PoolConnectionState.AVAILABLE:
                    # Check age and idle time
                    if conn.is_expired(self.config.max_connection_age_seconds):
                        connections_to_remove.append(conn)
                        continue

                    idle_time = (datetime.now() - conn.last_used).total_seconds()
                    if idle_time > self.config.max_idle_time_seconds:
                        if len(self._connections) > self.config.min_connections:
                            connections_to_remove.append(conn)
                            continue

                    # Validate connection health
                    if conn.needs_validation(self.config.validation_interval_seconds):
                        if not await conn.validate():
                            connections_to_remove.append(conn)

            # Remove unhealthy connections
            for conn in connections_to_remove:
                await self._remove_connection(conn)

            # Ensure minimum connections
            while len(self._connections) < self.config.min_connections:
                try:
                    await self._create_connection()
                except Exception as e:
                    self.logger.error(
                        "failed_to_maintain_min_connections", error=str(e)
                    )
                    break

    async def close(self) -> None:
        """Close the connection pool and all connections."""
        async with self._lock:
            if self._closed:
                return

            self._closed = True

            # Cancel health check task
            if self._health_check_task:
                self._health_check_task.cancel()
                try:
                    await self._health_check_task
                except asyncio.CancelledError:
                    pass

            # Close all connections
            for conn in self._connections[:]:
                await self._remove_connection(conn)

            self.logger.info("pool_closed")

    def get_stats(self) -> dict[str, Any]:
        """Get pool statistics."""
        available = sum(
            1 for c in self._connections if c.state == PoolConnectionState.AVAILABLE
        )
        in_use = sum(
            1 for c in self._connections if c.state == PoolConnectionState.IN_USE
        )
        invalid = sum(
            1 for c in self._connections if c.state == PoolConnectionState.INVALID
        )

        return {
            "pool_id": self.pool_id,
            "warehouse_type": self.warehouse_type.value,
            "total_connections": len(self._connections),
            "available_connections": available,
            "in_use_connections": in_use,
            "invalid_connections": invalid,
            "min_connections": self.config.min_connections,
            "max_connections": self.config.max_connections,
            "pool_utilization": in_use / max(1, len(self._connections)),
            "closed": self._closed,
        }


class PooledConnectionManager:
    """Context manager for pooled connections."""

    def __init__(self, pool: ConnectionPool):
        self.pool = pool
        self.connection: PooledConnection | None = None

    async def __aenter__(self) -> DataWarehouseConnector:
        self.connection = await self.pool._acquire_connection()
        return self.connection.connector

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        if self.connection:
            await self.pool._release_connection(self.connection)
            self.connection = None
