"""
Amazon Redshift data warehouse connector implementation.

This module provides a connector for Redshift using SQLAlchemy
with the redshift_connector library for optimal performance.
"""

import asyncio
import time
from typing import Any

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
    validate_sql_identifier,
)
from .base import (
    ConnectionError as ConnectorConnectionError,
)


class RedshiftConfig(BaseModel):
    """Configuration for Redshift connection."""

    host: str
    port: int = 5439
    database: str
    user: str
    password: str | None = None
    cluster_identifier: str | None = None
    db_user: str | None = None  # For IAM authentication
    iam: bool = False
    ssl: bool = True
    sslmode: str = "require"
    connect_timeout: int = 60
    application_name: str = "analytics-backend"
    schema: str | None = "public"


class RedshiftConnector(DataWarehouseConnector):
    """Amazon Redshift data warehouse connector."""

    def __init__(self, connection_params: dict[str, Any]):
        """Initialize Redshift connector with configuration."""
        super().__init__(connection_params)
        self.config = RedshiftConfig(**connection_params)
        self._connection = None
        self._cursor = None

    def _get_warehouse_type(self) -> WarehouseType:
        """Return Redshift warehouse type."""
        return WarehouseType.REDSHIFT

    async def connect(self) -> None:
        """Establish connection to Redshift."""
        try:
            import redshift_connector

            self._status = ConnectionStatus.CONNECTING

            # Build connection parameters
            conn_params = {
                "host": self.config.host,
                "port": self.config.port,
                "database": self.config.database,
                "user": self.config.user,
                "ssl": self.config.ssl,
                "sslmode": self.config.sslmode,
                "timeout": self.config.connect_timeout,
                "application_name": self.config.application_name,
            }

            # Add authentication
            if self.config.iam and self.config.cluster_identifier:
                # IAM authentication
                conn_params["cluster_identifier"] = self.config.cluster_identifier
                if self.config.db_user:
                    conn_params["db_user"] = self.config.db_user
                conn_params["iam"] = True
            elif self.config.password:
                # Password authentication
                conn_params["password"] = self.config.password
            else:
                raise ValueError(
                    "Either password or IAM authentication must be configured"
                )

            # Create connection
            self._connection = redshift_connector.connect(**conn_params)
            if self._connection:
                self._cursor = self._connection.cursor()

            # Set default schema if specified
            if self.config.schema and self.config.schema != "public" and self._cursor:
                self._cursor.execute(f"SET search_path TO {self.config.schema}, public")

            self._status = ConnectionStatus.CONNECTED

        except Exception as e:
            self._status = ConnectionStatus.ERROR
            raise ConnectorConnectionError(
                f"Failed to connect to Redshift: {str(e)}", WarehouseType.REDSHIFT
            )

    async def disconnect(self) -> None:
        """Close Redshift connection."""
        try:
            if self._cursor:
                self._cursor.close()
                self._cursor = None
            if self._connection:
                self._connection.close()
                self._connection = None
            self._status = ConnectionStatus.DISCONNECTED
        except Exception:
            # Log error but don't raise - disconnection should be best-effort
            self._status = ConnectionStatus.ERROR

    async def test_connection(self) -> bool:
        """Test Redshift connection health."""
        try:
            if not self._cursor:
                return False

            self._cursor.execute("SELECT 1")
            result = self._cursor.fetchone()
            return result is not None
        except Exception:
            return False

    async def execute_query(
        self,
        query: str,
        params: dict[str, Any] | None = None,
        timeout: int | None = None,
    ) -> QueryResult:
        """Execute a SQL query on Redshift."""
        if not self._cursor:
            raise QueryError("Not connected to Redshift", query)

        start_time = time.time()

        try:
            # Set statement timeout if specified
            if timeout:
                self._cursor.execute(
                    f"SET statement_timeout = {timeout * 1000}"
                )  # Redshift uses milliseconds

            # Execute query with parameters
            if params:
                # Convert named parameters to positional for psycopg2-style
                formatted_query = query
                param_values = []
                for key, value in params.items():
                    formatted_query = formatted_query.replace(f":{key}", "%s")
                    param_values.append(value)
                self._cursor.execute(formatted_query, param_values)
            else:
                self._cursor.execute(query)

            # Fetch results
            results = self._cursor.fetchall()
            columns = (
                [desc[0] for desc in self._cursor.description]
                if self._cursor.description
                else []
            )

            # Convert results to list format
            data = [list(row) for row in results] if results else []

            execution_time = int((time.time() - start_time) * 1000)

            metadata = QueryMetadata(
                query_id=None,  # Redshift doesn't provide query IDs directly
                execution_time_ms=execution_time,
                rows_scanned=len(data) if data else 0,
                cache_hit=False,
            )

            return QueryResult(
                columns=columns,
                data=data,
                metadata=metadata,
                total_rows=len(data) if data else 0,
            )

        except Exception as e:
            raise QueryError(f"Query execution failed: {str(e)}", query)

    async def execute_async_query(
        self, query: str, params: dict[str, Any] | None = None
    ) -> str:
        """Execute query asynchronously and return session ID."""
        # Redshift doesn't have native async query support like Snowflake
        # We'll simulate it by running in a thread and returning a session-based ID
        import threading
        import uuid

        query_id = str(uuid.uuid4())

        # Store query for later retrieval (in production, use Redis/database)
        if not hasattr(self, "_async_queries"):
            self._async_queries = {}

        self._async_queries[query_id] = {
            "status": "RUNNING",
            "query": query,
            "params": params,
            "result": None,
            "error": None,
        }

        # Execute in background thread
        def execute_background():
            try:
                result = asyncio.run(self.execute_query(query, params))
                self._async_queries[query_id]["status"] = "SUCCEEDED"
                self._async_queries[query_id]["result"] = result
            except Exception as e:
                self._async_queries[query_id]["status"] = "FAILED"
                self._async_queries[query_id]["error"] = str(e)

        thread = threading.Thread(target=execute_background)
        thread.start()

        return query_id

    async def get_query_status(self, query_id: str) -> dict[str, Any]:
        """Get status of an async query."""
        if not hasattr(self, "_async_queries") or query_id not in self._async_queries:
            return {
                "query_id": query_id,
                "status": "NOT_FOUND",
                "completed": True,
                "error": "Query ID not found",
            }

        query_info = self._async_queries[query_id]

        return {
            "query_id": query_id,
            "status": query_info["status"],
            "completed": query_info["status"] in ["SUCCEEDED", "FAILED"],
            "error": query_info.get("error"),
        }

    async def get_query_result(self, query_id: str) -> QueryResult:
        """Get results from a completed async query."""
        if not hasattr(self, "_async_queries") or query_id not in self._async_queries:
            raise QueryError(f"Query ID {query_id} not found", "")

        query_info = self._async_queries[query_id]

        if query_info["status"] == "RUNNING":
            raise QueryError(f"Query {query_id} is still running", "")

        if query_info["status"] == "FAILED":
            raise QueryError(f"Query failed: {query_info['error']}", "")

        if query_info["result"] is None:
            raise QueryError(f"No result available for query {query_id}", "")

        result = query_info["result"]
        if isinstance(result, QueryResult):
            return result
        else:
            raise QueryError(f"Invalid result type for query {query_id}", "")

    async def get_schema_info(
        self, schema_name: str | None = None
    ) -> SchemaInfo | list[SchemaInfo]:
        """Get schema information from Redshift."""
        if schema_name:
            # Get specific schema info
            tables = await self._get_schema_tables(schema_name)
            return SchemaInfo(name=schema_name, tables=tables)
        else:
            # Get all schemas
            schemas_query = """
                SELECT schema_name
                FROM information_schema.schemata
                WHERE schema_name NOT IN ('information_schema', 'pg_catalog', 'pg_internal')
                ORDER BY schema_name
            """

            result = await self.execute_query(schemas_query)
            schemas = []

            for row in result.data:
                schema_name = row[0]
                tables = await self._get_schema_tables(schema_name)

                schemas.append(SchemaInfo(name=schema_name, tables=tables))

            return schemas

    async def _get_schema_tables(self, schema_name: str) -> list[TableInfo]:
        """Get tables for a specific schema."""
        tables_query = """
            SELECT table_name, table_type
            FROM information_schema.tables
            WHERE table_schema = %s
            AND table_type = 'BASE TABLE'
            ORDER BY table_name
        """

        result = await self.execute_query(tables_query, {"schema": schema_name})
        tables = []

        for row in result.data:
            table_name = row[0]

            # Get columns for this table
            columns = await self._get_table_columns(table_name, schema_name)

            # Get row count estimate
            count_query = f"""
                SELECT COUNT(*)
                FROM "{schema_name}"."{table_name}"
                LIMIT 1000000
            """

            try:
                count_result = await self.execute_query(count_query)
                row_count = count_result.data[0][0] if count_result.data else None
            except Exception:
                row_count = None  # Table might be empty or inaccessible

            tables.append(
                TableInfo(
                    name=table_name,
                    schema=schema_name,
                    columns=columns,
                    row_count=row_count,
                )
            )

        return tables

    async def _get_table_columns(
        self, table_name: str, schema_name: str
    ) -> list[ColumnInfo]:
        """Get columns for a specific table."""
        columns_query = """
            SELECT
                column_name,
                data_type,
                is_nullable,
                column_default
            FROM information_schema.columns
            WHERE table_schema = %s AND table_name = %s
            ORDER BY ordinal_position
        """

        # Use positional parameters for Redshift
        formatted_query = columns_query.replace("%s", "'{}'").format(
            schema_name, table_name
        )
        result = await self.execute_query(formatted_query)

        columns = []
        for row in result.data:
            columns.append(
                ColumnInfo(
                    name=row[0],
                    data_type=row[1],
                    is_nullable=row[2] == "YES",
                    default_value=row[3] if len(row) > 3 else None,
                )
            )

        return columns

    async def get_table_info(
        self, table_name: str, schema_name: str | None = None
    ) -> TableInfo:
        """Get detailed information about a specific table."""
        schema = schema_name or self.config.schema or "public"

        tables = await self._get_schema_tables(schema)

        for table in tables:
            if table.name.lower() == table_name.lower():
                return table

        raise ValueError(f"Table {table_name} not found in schema {schema}")

    async def get_table_sample(
        self, table_name: str, schema_name: str | None = None, limit: int = 100
    ) -> QueryResult:
        """Get a sample of data from a table."""
        # Validate inputs to prevent SQL injection
        validate_sql_identifier(table_name, "table name")
        schema = schema_name or self.config.schema or "public"
        validate_sql_identifier(schema, "schema name")

        # Validate limit to prevent abuse
        if not isinstance(limit, int) or limit < 1 or limit > 10000:
            raise ValueError("Limit must be an integer between 1 and 10000")

        full_table_name = f'"{schema}"."{table_name}"'
        sample_query = f"SELECT * FROM {full_table_name} LIMIT {limit}"
        return await self.execute_query(sample_query)
