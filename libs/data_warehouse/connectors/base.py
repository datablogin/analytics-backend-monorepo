"""
Base data warehouse connector interface and common data structures.

This module defines the abstract base class for all data warehouse connectors
and provides common data structures used across the library.
"""

import re
from abc import ABC, abstractmethod
from datetime import datetime
from enum import Enum
from typing import Any

from pydantic import BaseModel


def sanitize_error_message(error_message: str) -> str:
    """
    Sanitize error messages to remove sensitive information.

    Args:
        error_message: The raw error message

    Returns:
        str: Sanitized error message
    """
    # List of sensitive patterns to remove/mask
    sensitive_patterns = [
        (r'password[=:]\s*[\'"][^\'";]+[\'"]', "password=***"),
        (r"password[=:]\s*\w+", "password=***"),
        (r'user[=:]\s*[\'"][^\'";]+[\'"]', "user=***"),
        (r'account[=:]\s*[\'"][^\'";]+[\'"]', "account=***"),
        (r'token[=:]\s*[\'"][^\'";]+[\'"]', "token=***"),
        (r'key[=:]\s*[\'"][^\'";]+[\'"]', "key=***"),
        (r'host[=:]\s*[\'"][^\'";]+[\'"]', "host=***"),
        (r'server[=:]\s*[\'"][^\'";]+[\'"]', "server=***"),
    ]

    sanitized_message = error_message
    for pattern, replacement in sensitive_patterns:
        sanitized_message = re.sub(
            pattern, replacement, sanitized_message, flags=re.IGNORECASE
        )

    return sanitized_message


def validate_sql_identifier(
    identifier: str, identifier_type: str = "identifier"
) -> str:
    """
    Validate and sanitize SQL identifiers to prevent injection attacks.

    Args:
        identifier: The identifier to validate
        identifier_type: Type of identifier for error messages

    Returns:
        The validated identifier

    Raises:
        ValueError: If the identifier is invalid or potentially malicious
    """
    if not identifier:
        raise ValueError(f"Empty {identifier_type} not allowed")

    # Check for basic SQL injection patterns
    if any(
        char in identifier.lower()
        for char in [";", "--", "/*", "*/", "drop", "delete", "update", "insert"]
    ):
        raise ValueError(f"Potentially malicious {identifier_type}: {identifier}")

    # Allow only alphanumeric characters, underscores, and dots for schema.table format
    if not re.match(r"^[a-zA-Z0-9_\.]+$", identifier):
        raise ValueError(
            f"Invalid {identifier_type}: {identifier}. Only alphanumeric characters, underscores, and dots allowed"
        )

    # Limit length to prevent abuse
    if len(identifier) > 128:
        raise ValueError(f"{identifier_type} too long: {identifier}")

    return identifier


class WarehouseType(str, Enum):
    """Supported data warehouse types."""

    SNOWFLAKE = "snowflake"
    BIGQUERY = "bigquery"
    REDSHIFT = "redshift"
    SYNAPSE = "synapse"
    DATABRICKS = "databricks"


class ConnectionStatus(str, Enum):
    """Connection status states."""

    CONNECTED = "connected"
    DISCONNECTED = "disconnected"
    CONNECTING = "connecting"
    ERROR = "error"


class ColumnInfo(BaseModel):
    """Information about a table column."""

    name: str
    data_type: str
    is_nullable: bool
    default_value: str | None = None
    comment: str | None = None


class TableInfo(BaseModel):
    """Information about a database table."""

    name: str
    schema: str
    columns: list[ColumnInfo]
    row_count: int | None = None
    comment: str | None = None
    created_at: datetime | None = None
    updated_at: datetime | None = None


class SchemaInfo(BaseModel):
    """Information about a database schema."""

    name: str
    tables: list[TableInfo]
    comment: str | None = None


class QueryMetadata(BaseModel):
    """Metadata about query execution."""

    query_id: str | None = None
    execution_time_ms: int
    rows_scanned: int | None = None
    bytes_scanned: int | None = None
    warehouse_credits_used: float | None = None
    cache_hit: bool = False


class QueryResult(BaseModel):
    """Result of a warehouse query execution."""

    columns: list[str]
    data: list[list[Any]]
    metadata: QueryMetadata
    total_rows: int | None = None

    def to_dict(self) -> dict[str, Any]:
        """Convert result to dictionary format."""
        return {
            "columns": self.columns,
            "data": self.data,
            "metadata": self.metadata.model_dump(),
            "total_rows": self.total_rows,
        }


class DataWarehouseConnector(ABC):
    """
    Abstract base class for data warehouse connectors.

    All data warehouse implementations must inherit from this class
    and implement the required abstract methods.
    """

    def __init__(self, connection_params: dict[str, Any]):
        """Initialize the connector with connection parameters."""
        self.connection_params = connection_params
        self._status = ConnectionStatus.DISCONNECTED
        self._connection = None
        self.logger: Any = None  # Will be initialized in concrete classes

    @property
    def status(self) -> ConnectionStatus:
        """Get current connection status."""
        return self._status

    @property
    def warehouse_type(self) -> WarehouseType:
        """Get the warehouse type for this connector."""
        return self._get_warehouse_type()

    @abstractmethod
    def _get_warehouse_type(self) -> WarehouseType:
        """Return the warehouse type for this connector."""
        pass

    @abstractmethod
    async def connect(self) -> None:
        """
        Establish connection to the data warehouse.

        Raises:
            ConnectionError: If connection cannot be established
        """
        pass

    @abstractmethod
    async def disconnect(self) -> None:
        """Close the connection to the data warehouse."""
        pass

    @abstractmethod
    async def test_connection(self) -> bool:
        """
        Test if the connection is working.

        Returns:
            bool: True if connection is healthy, False otherwise
        """
        pass

    @abstractmethod
    async def execute_query(
        self,
        query: str,
        params: dict[str, Any] | None = None,
        timeout: int | None = None,
    ) -> QueryResult:
        """
        Execute a SQL query and return results.

        Args:
            query: SQL query string
            params: Optional query parameters
            timeout: Optional timeout in seconds

        Returns:
            QueryResult: Query execution results

        Raises:
            QueryError: If query execution fails
            TimeoutError: If query times out
        """
        pass

    @abstractmethod
    async def execute_async_query(
        self, query: str, params: dict[str, Any] | None = None
    ) -> str:
        """
        Execute a query asynchronously and return query ID for tracking.

        Args:
            query: SQL query string
            params: Optional query parameters

        Returns:
            str: Query ID for tracking execution status
        """
        pass

    @abstractmethod
    async def get_query_status(self, query_id: str) -> dict[str, Any]:
        """
        Get status of an asynchronously executed query.

        Args:
            query_id: ID of the query to check

        Returns:
            Dict containing query status information
        """
        pass

    @abstractmethod
    async def get_query_result(self, query_id: str) -> QueryResult:
        """
        Get results of a completed asynchronous query.

        Args:
            query_id: ID of the completed query

        Returns:
            QueryResult: Query execution results
        """
        pass

    @abstractmethod
    async def get_schema_info(
        self, schema_name: str | None = None
    ) -> SchemaInfo | list[SchemaInfo]:
        """
        Get schema information.

        Args:
            schema_name: Optional specific schema name

        Returns:
            SchemaInfo or list of SchemaInfo objects
        """
        pass

    @abstractmethod
    async def get_table_info(
        self, table_name: str, schema_name: str | None = None
    ) -> TableInfo:
        """
        Get detailed information about a specific table.

        Args:
            table_name: Name of the table
            schema_name: Optional schema name

        Returns:
            TableInfo: Detailed table information
        """
        pass

    @abstractmethod
    async def get_table_sample(
        self, table_name: str, schema_name: str | None = None, limit: int = 100
    ) -> QueryResult:
        """
        Get a sample of data from a table.

        Args:
            table_name: Name of the table
            schema_name: Optional schema name
            limit: Maximum number of rows to return

        Returns:
            QueryResult: Sample data from the table
        """
        pass

    async def __aenter__(self):
        """Async context manager entry."""
        await self.connect()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit."""
        await self.disconnect()


class QueryError(Exception):
    """Exception raised for query execution errors."""

    def __init__(self, message: str, query: str, error_code: str | None = None):
        super().__init__(message)
        self.query = query
        self.error_code = error_code


class ConnectionError(Exception):
    """Exception raised for connection errors."""

    def __init__(self, message: str, warehouse_type: WarehouseType):
        super().__init__(message)
        self.warehouse_type = warehouse_type
