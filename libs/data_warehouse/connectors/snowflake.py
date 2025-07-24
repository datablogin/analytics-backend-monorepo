"""
Snowflake data warehouse connector implementation.

This module provides a connector for Snowflake using the official
snowflake-connector-python library with async support.
"""

import asyncio
import time
from typing import Any

import structlog
from pydantic import BaseModel

from .base import (
    ColumnInfo,
    ConnectionStatus,
    DataWarehouseConnector,
    QueryError,
    QueryMetadata,
    QueryResult,
    SchemaInfo,
    TableInfo,
    WarehouseType,
    sanitize_error_message,
    validate_sql_identifier,
)
from .base import (
    ConnectionError as ConnectorConnectionError,
)


class SnowflakeConfig(BaseModel):
    """Configuration for Snowflake connection."""

    account: str
    user: str
    password: str | None = None
    private_key: str | None = None
    private_key_passphrase: str | None = None
    database: str | None = None
    schema: str | None = None
    warehouse: str | None = None
    role: str | None = None
    autocommit: bool = True
    connection_timeout: int = 60
    network_timeout: int = 300
    client_session_keep_alive: bool = True


class SnowflakeConnector(DataWarehouseConnector):
    """Snowflake data warehouse connector."""

    def __init__(self, connection_params: dict[str, Any]):
        """Initialize Snowflake connector with configuration."""
        super().__init__(connection_params)
        self.config = SnowflakeConfig(**connection_params)
        self._connection = None
        self._cursor = None
        self.logger = structlog.get_logger(__name__).bind(
            warehouse_type="snowflake",
            account=self.config.account,
            user=self.config.user,
            connector_id=id(self),
        )

    def _get_warehouse_type(self) -> WarehouseType:
        """Return Snowflake warehouse type."""
        return WarehouseType.SNOWFLAKE

    def _create_sync_connection(self, conn_params: dict[str, Any]) -> tuple[Any, Any]:
        """
        Create a synchronous Snowflake connection.

        This method performs the actual sync connection creation that will be
        executed asynchronously using run_in_executor.

        Args:
            conn_params: Connection parameters for Snowflake

        Returns:
            tuple: (connection, cursor) objects
        """
        import snowflake.connector
        from snowflake.connector import DictCursor

        connection = snowflake.connector.connect(**conn_params)
        cursor = connection.cursor(DictCursor) if connection else None
        return connection, cursor

    def _execute_sync_query(
        self,
        query: str,
        params: dict[str, Any] | None = None,
        timeout: int | None = None,
    ) -> tuple[list[Any], list[str], str | None]:
        """
        Execute a synchronous query on Snowflake.

        This method performs the actual sync query execution that will be
        executed asynchronously using run_in_executor.

        Args:
            query: SQL query to execute
            params: Optional query parameters
            timeout: Optional query timeout

        Returns:
            tuple: (results, columns, query_id)
        """
        if not self._cursor:
            raise QueryError("Not connected to Snowflake", query)

        # Set query timeout if specified
        if timeout:
            self._cursor.execute(
                f"ALTER SESSION SET STATEMENT_TIMEOUT_IN_SECONDS = {timeout}"
            )

        # Execute query with parameters if provided
        if params:
            self._cursor.execute(query, params)
        else:
            self._cursor.execute(query)

        # Fetch results
        results = self._cursor.fetchall()
        columns = (
            [desc[0] for desc in self._cursor.description]
            if self._cursor.description
            else []
        )

        # Get query ID if available
        query_id = self._cursor.sfqid if hasattr(self._cursor, "sfqid") else None

        return results, columns, query_id

    async def connect(self) -> None:
        """Establish connection to Snowflake."""
        self.logger.info("attempting_connection", status="connecting")
        try:
            self._status = ConnectionStatus.CONNECTING

            # Build connection parameters
            conn_params = {
                "account": self.config.account,
                "user": self.config.user,
                "autocommit": self.config.autocommit,
                "connection_timeout": self.config.connection_timeout,
                "network_timeout": self.config.network_timeout,
                "client_session_keep_alive": self.config.client_session_keep_alive,
            }

            # Add authentication
            if self.config.password:
                conn_params["password"] = self.config.password
            elif self.config.private_key:
                conn_params["private_key"] = self.config.private_key
                if self.config.private_key_passphrase:
                    conn_params["private_key_passphrase"] = (
                        self.config.private_key_passphrase
                    )

            # Add optional parameters
            if self.config.database:
                conn_params["database"] = self.config.database
            if self.config.schema:
                conn_params["schema"] = self.config.schema
            if self.config.warehouse:
                conn_params["warehouse"] = self.config.warehouse
            if self.config.role:
                conn_params["role"] = self.config.role

            # Create connection asynchronously using executor
            loop = asyncio.get_event_loop()
            self._connection, self._cursor = await loop.run_in_executor(
                None, self._create_sync_connection, conn_params
            )

            self._status = ConnectionStatus.CONNECTED
            self.logger.info("connection_successful", status="connected")

        except Exception as e:
            self._status = ConnectionStatus.ERROR
            sanitized_error = sanitize_error_message(str(e))
            self.logger.error(
                "connection_failed", status="error", error=sanitized_error
            )
            raise ConnectorConnectionError(
                f"Failed to connect to Snowflake: {sanitized_error}",
                WarehouseType.SNOWFLAKE,
            )

    async def disconnect(self) -> None:
        """Close Snowflake connection."""
        self.logger.info("disconnecting", status="disconnecting")
        try:
            if self._cursor:
                self._cursor.close()
                self._cursor = None
            if self._connection:
                self._connection.close()
                self._connection = None
            self._status = ConnectionStatus.DISCONNECTED
            self.logger.info("disconnected_successfully", status="disconnected")
        except Exception as e:
            # Log error but don't raise - disconnection should be best-effort
            self.logger.error("disconnect_failed", status="error", error=str(e))
            self._status = ConnectionStatus.ERROR

    async def test_connection(self) -> bool:
        """Test Snowflake connection health."""
        try:
            if not self._cursor:
                return False

            # Execute test query asynchronously
            loop = asyncio.get_event_loop()
            results, _, _ = await loop.run_in_executor(
                None, self._execute_sync_query, "SELECT 1", None, None
            )
            return len(results) > 0 and results[0] is not None
        except Exception:
            return False

    async def execute_query(
        self,
        query: str,
        params: dict[str, Any] | None = None,
        timeout: int | None = None,
    ) -> QueryResult:
        """Execute a SQL query on Snowflake."""
        if not self._cursor:
            raise QueryError("Not connected to Snowflake", query)

        query_id = f"query_{int(time.time() * 1000)}"
        self.logger.info(
            "executing_query", query_id=query_id, has_params=params is not None
        )
        start_time = time.time()

        try:
            # Execute query asynchronously using executor
            loop = asyncio.get_event_loop()
            results, columns, sf_query_id = await loop.run_in_executor(
                None, self._execute_sync_query, query, params, timeout
            )

            # Convert dict results to list format
            data = []
            if results and isinstance(results[0], dict):
                data = [[row[col] for col in columns] for row in results]
            else:
                data = results

            execution_time = int((time.time() - start_time) * 1000)

            metadata = QueryMetadata(
                query_id=sf_query_id,
                execution_time_ms=execution_time,
                rows_scanned=len(data) if data else 0,
                cache_hit=False,
            )

            self.logger.info(
                "query_completed",
                query_id=query_id,
                snowflake_query_id=sf_query_id,
                execution_time_ms=execution_time,
                row_count=len(data) if data else 0,
            )

            return QueryResult(
                columns=columns,
                data=data,
                metadata=metadata,
                total_rows=len(data) if data else 0,
            )

        except Exception as e:
            execution_time = int((time.time() - start_time) * 1000)
            self.logger.error(
                "query_failed",
                query_id=query_id,
                execution_time_ms=execution_time,
                error=str(e),
            )
            raise QueryError(f"Query execution failed: {str(e)}", query)

    async def execute_async_query(
        self, query: str, params: dict[str, Any] | None = None
    ) -> str:
        """Execute query asynchronously and return query ID."""
        if not self._cursor:
            raise QueryError("Not connected to Snowflake", query)

        try:
            # Execute query asynchronously
            if params:
                self._cursor.execute_async(query, params)
            else:
                self._cursor.execute_async(query)

            # Return query ID for tracking
            return self._cursor.sfqid

        except Exception as e:
            raise QueryError(f"Async query execution failed: {str(e)}", query)

    async def get_query_status(self, query_id: str) -> dict[str, Any]:
        """Get status of an async query."""
        if not self._cursor:
            raise QueryError("Not connected to Snowflake", "")

        try:
            status_query = f"SELECT * FROM TABLE(RESULT_SCAN('{query_id}')) LIMIT 0"
            self._cursor.execute(status_query)

            # Query completed if no exception
            return {"query_id": query_id, "status": "SUCCEEDED", "completed": True}

        except Exception as e:
            error_msg = str(e).lower()
            if "does not exist" in error_msg or "not found" in error_msg:
                return {"query_id": query_id, "status": "RUNNING", "completed": False}
            else:
                return {
                    "query_id": query_id,
                    "status": "FAILED",
                    "completed": True,
                    "error": str(e),
                }

    async def get_query_result(self, query_id: str) -> QueryResult:
        """Get results from a completed async query."""
        if not self._cursor:
            raise QueryError("Not connected to Snowflake", "")

        try:
            # Get results using RESULT_SCAN
            result_query = f"SELECT * FROM TABLE(RESULT_SCAN('{query_id}'))"
            return await self.execute_query(result_query)

        except Exception as e:
            raise QueryError(
                f"Failed to retrieve async query results: {str(e)}", result_query
            )

    async def get_schema_info(
        self, schema_name: str | None = None
    ) -> SchemaInfo | list[SchemaInfo]:
        """Get schema information from Snowflake."""
        if schema_name:
            # Get specific schema info - placeholder for future implementation
            pass
        else:
            # Get all schemas
            schemas_query = """
                SELECT SCHEMA_NAME, COMMENT
                FROM INFORMATION_SCHEMA.SCHEMATA
                ORDER BY SCHEMA_NAME
            """

            result = await self.execute_query(schemas_query)
            schemas = []

            for row in result.data:
                schema_name = row[0]
                comment = row[1] if len(row) > 1 else None

                # Get tables for this schema
                tables = await self._get_schema_tables(schema_name)

                schemas.append(
                    SchemaInfo(name=schema_name, tables=tables, comment=comment)
                )

            return schemas

        # Single schema case
        tables = await self._get_schema_tables(schema_name)
        return SchemaInfo(name=schema_name, tables=tables)

    async def _get_schema_tables(self, schema_name: str) -> list[TableInfo]:
        """Get tables for a specific schema."""
        tables_query = """
            SELECT TABLE_NAME, COMMENT, ROW_COUNT, CREATED, LAST_ALTERED
            FROM INFORMATION_SCHEMA.TABLES
            WHERE TABLE_SCHEMA = :schema_name
            ORDER BY TABLE_NAME
        """

        result = await self.execute_query(tables_query, {"schema_name": schema_name})
        tables = []

        for row in result.data:
            table_name = row[0]
            comment = row[1] if len(row) > 1 else None
            row_count = row[2] if len(row) > 2 else None
            created_at = row[3] if len(row) > 3 else None
            updated_at = row[4] if len(row) > 4 else None

            # Get columns for this table
            columns = await self._get_table_columns(table_name, schema_name)

            tables.append(
                TableInfo(
                    name=table_name,
                    schema=schema_name,
                    columns=columns,
                    row_count=row_count,
                    comment=comment,
                    created_at=created_at,
                    updated_at=updated_at,
                )
            )

        return tables

    async def _get_table_columns(
        self, table_name: str, schema_name: str
    ) -> list[ColumnInfo]:
        """Get columns for a specific table."""
        columns_query = """
            SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE, COLUMN_DEFAULT, COMMENT
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = :schema_name AND TABLE_NAME = :table_name
            ORDER BY ORDINAL_POSITION
        """

        result = await self.execute_query(
            columns_query, {"schema_name": schema_name, "table_name": table_name}
        )
        columns = []

        for row in result.data:
            columns.append(
                ColumnInfo(
                    name=row[0],
                    data_type=row[1],
                    is_nullable=row[2] == "YES",
                    default_value=row[3] if len(row) > 3 else None,
                    comment=row[4] if len(row) > 4 else None,
                )
            )

        return columns

    async def get_table_info(
        self, table_name: str, schema_name: str | None = None
    ) -> TableInfo:
        """Get detailed information about a specific table."""
        schema = schema_name or self.config.schema
        if not schema:
            raise ValueError("Schema name is required")

        tables = await self._get_schema_tables(schema)

        for table in tables:
            if table.name.upper() == table_name.upper():
                return table

        raise ValueError(f"Table {table_name} not found in schema {schema}")

    async def get_table_sample(
        self, table_name: str, schema_name: str | None = None, limit: int = 100
    ) -> QueryResult:
        """Get a sample of data from a table."""
        # Validate inputs to prevent SQL injection
        validate_sql_identifier(table_name, "table name")
        schema = schema_name or self.config.schema
        if schema:
            validate_sql_identifier(schema, "schema name")

        # Validate limit to prevent abuse
        if not isinstance(limit, int) or limit < 1 or limit > 10000:
            raise ValueError("Limit must be an integer between 1 and 10000")

        full_table_name = f"{schema}.{table_name}" if schema else table_name
        sample_query = f"SELECT * FROM {full_table_name} LIMIT {limit}"
        return await self.execute_query(sample_query)
