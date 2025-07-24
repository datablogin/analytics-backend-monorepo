"""
Google BigQuery data warehouse connector implementation.

This module provides a connector for BigQuery using the official
google-cloud-bigquery library with async support.
"""

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
)
from .base import (
    ConnectionError as ConnectorConnectionError,
)


class BigQueryConfig(BaseModel):
    """Configuration for BigQuery connection."""

    project_id: str
    credentials_path: str | None = None
    credentials_json: dict[str, Any] | None = None
    location: str = "US"
    default_dataset: str | None = None
    maximum_bytes_billed: int | None = None
    use_legacy_sql: bool = False
    job_timeout: int = 300
    job_retry_timeout: int = 600


class BigQueryConnector(DataWarehouseConnector):
    """Google BigQuery data warehouse connector."""

    def __init__(self, connection_params: dict[str, Any]):
        """Initialize BigQuery connector with configuration."""
        super().__init__(connection_params)
        self.config = BigQueryConfig(**connection_params)
        self._client = None

    def _get_warehouse_type(self) -> WarehouseType:
        """Return BigQuery warehouse type."""
        return WarehouseType.BIGQUERY

    async def connect(self) -> None:
        """Establish connection to BigQuery."""
        try:

            from google.cloud import bigquery
            from google.oauth2 import service_account

            self._status = ConnectionStatus.CONNECTING

            # Setup credentials
            credentials = None
            if self.config.credentials_path:
                credentials = service_account.Credentials.from_service_account_file(
                    self.config.credentials_path
                )
            elif self.config.credentials_json:
                credentials = service_account.Credentials.from_service_account_info(
                    self.config.credentials_json
                )

            # Create BigQuery client
            self._client = bigquery.Client(
                project=self.config.project_id,
                credentials=credentials,
                location=self.config.location,
            )

            # Test connection
            await self.test_connection()
            self._status = ConnectionStatus.CONNECTED

        except Exception as e:
            self._status = ConnectionStatus.ERROR
            raise ConnectorConnectionError(
                f"Failed to connect to BigQuery: {str(e)}", WarehouseType.BIGQUERY
            )

    async def disconnect(self) -> None:
        """Close BigQuery connection."""
        try:
            if self._client:
                self._client.close()
                self._client = None
            self._status = ConnectionStatus.DISCONNECTED
        except Exception:
            # Log error but don't raise - disconnection should be best-effort
            self._status = ConnectionStatus.ERROR

    async def test_connection(self) -> bool:
        """Test BigQuery connection health."""
        try:
            if not self._client:
                return False

            # Simple query to test connection
            query = "SELECT 1 as test"
            query_job = self._client.query(query)
            result = query_job.result(timeout=30)
            return len(list(result)) > 0

        except Exception:
            return False

    async def execute_query(
        self,
        query: str,
        params: dict[str, Any] | None = None,
        timeout: int | None = None,
    ) -> QueryResult:
        """Execute a SQL query on BigQuery."""
        if not self._client:
            raise QueryError("Not connected to BigQuery", query)

        start_time = time.time()

        try:
            from google.cloud import bigquery

            # Configure job
            job_config = bigquery.QueryJobConfig()
            job_config.use_legacy_sql = self.config.use_legacy_sql

            if self.config.maximum_bytes_billed:
                job_config.maximum_bytes_billed = self.config.maximum_bytes_billed

            if params:
                # Convert params to BigQuery parameters
                query_parameters = []
                for key, value in params.items():
                    param_type = self._get_bigquery_param_type(value)
                    query_parameters.append(
                        bigquery.ScalarQueryParameter(key, param_type, value)
                    )
                job_config.query_parameters = query_parameters

            # Execute query
            query_job = self._client.query(query, job_config=job_config)

            # Wait for completion with timeout
            query_timeout = timeout or self.config.job_timeout
            result = query_job.result(timeout=query_timeout)

            # Process results
            columns = [field.name for field in result.schema]
            data = []

            for row in result:
                data.append(list(row.values()))

            execution_time = int((time.time() - start_time) * 1000)

            # Get query metadata
            metadata = QueryMetadata(
                query_id=query_job.job_id,
                execution_time_ms=execution_time,
                rows_scanned=query_job.total_bytes_processed,
                bytes_scanned=query_job.total_bytes_processed,
                cache_hit=query_job.cache_hit or False,
            )

            return QueryResult(
                columns=columns,
                data=data,
                metadata=metadata,
                total_rows=result.total_rows,
            )

        except Exception as e:
            raise QueryError(f"Query execution failed: {str(e)}", query)

    def _get_bigquery_param_type(self, value: Any) -> str:
        """Determine BigQuery parameter type from Python value."""
        if isinstance(value, bool):
            return "BOOL"
        elif isinstance(value, int):
            return "INT64"
        elif isinstance(value, float):
            return "FLOAT64"
        elif isinstance(value, str):
            return "STRING"
        else:
            return "STRING"  # Default to string

    async def execute_async_query(
        self, query: str, params: dict[str, Any] | None = None
    ) -> str:
        """Execute query asynchronously and return job ID."""
        if not self._client:
            raise QueryError("Not connected to BigQuery", query)

        try:
            from google.cloud import bigquery

            # Configure job
            job_config = bigquery.QueryJobConfig()
            job_config.use_legacy_sql = self.config.use_legacy_sql

            if params:
                # Convert params to BigQuery parameters
                query_parameters = []
                for key, value in params.items():
                    param_type = self._get_bigquery_param_type(value)
                    query_parameters.append(
                        bigquery.ScalarQueryParameter(key, param_type, value)
                    )
                job_config.query_parameters = query_parameters

            # Start async query
            query_job = self._client.query(query, job_config=job_config)

            return query_job.job_id

        except Exception as e:
            raise QueryError(f"Async query execution failed: {str(e)}", query)

    async def get_query_status(self, query_id: str) -> dict[str, Any]:
        """Get status of an async query."""
        if not self._client:
            raise QueryError("Not connected to BigQuery", "")

        try:
            job = self._client.get_job(query_id)

            status_map = {
                "PENDING": "RUNNING",
                "RUNNING": "RUNNING",
                "DONE": "SUCCEEDED",
            }

            bq_state = job.state
            status = status_map.get(bq_state, "UNKNOWN")

            result = {
                "query_id": query_id,
                "status": status,
                "completed": bq_state == "DONE",
            }

            if job.error_result:
                result["error"] = job.error_result["message"]
                result["status"] = "FAILED"

            return result

        except Exception as e:
            return {
                "query_id": query_id,
                "status": "FAILED",
                "completed": True,
                "error": str(e),
            }

    async def get_query_result(self, query_id: str) -> QueryResult:
        """Get results from a completed async query."""
        if not self._client:
            raise QueryError("Not connected to BigQuery", "")

        try:
            job = self._client.get_job(query_id)

            if job.state != "DONE":
                raise QueryError(f"Query {query_id} is not completed", "")

            if job.error_result:
                raise QueryError(f"Query failed: {job.error_result['message']}", "")

            # Get results
            result = job.result()

            columns = [field.name for field in result.schema]
            data = []

            for row in result:
                data.append(list(row.values()))

            metadata = QueryMetadata(
                query_id=query_id,
                execution_time_ms=0,  # Not available for completed jobs
                rows_scanned=job.total_bytes_processed,
                bytes_scanned=job.total_bytes_processed,
                cache_hit=job.cache_hit or False,
            )

            return QueryResult(
                columns=columns,
                data=data,
                metadata=metadata,
                total_rows=result.total_rows,
            )

        except Exception as e:
            raise QueryError(f"Failed to retrieve async query results: {str(e)}", "")

    async def get_schema_info(
        self, schema_name: str | None = None
    ) -> SchemaInfo | list[SchemaInfo]:
        """Get schema (dataset) information from BigQuery."""
        if not self._client:
            raise QueryError("Not connected to BigQuery", "")

        try:
            if schema_name:
                # Get specific dataset info
                dataset = self._client.get_dataset(schema_name)
                tables = await self._get_dataset_tables(schema_name)

                return SchemaInfo(
                    name=dataset.dataset_id, tables=tables, comment=dataset.description
                )
            else:
                # Get all datasets
                datasets = list(self._client.list_datasets())
                schemas = []

                for dataset in datasets:
                    tables = await self._get_dataset_tables(dataset.dataset_id)

                    schemas.append(
                        SchemaInfo(
                            name=dataset.dataset_id,
                            tables=tables,
                            comment=dataset.description,
                        )
                    )

                return schemas

        except Exception as e:
            raise QueryError(f"Failed to get schema info: {str(e)}", "")

    async def _get_dataset_tables(self, dataset_id: str) -> list[TableInfo]:
        """Get tables for a specific dataset."""
        try:
            if not self._client:
                raise QueryError("Not connected to BigQuery", "")
            dataset_ref = self._client.dataset(dataset_id)
            tables = list(self._client.list_tables(dataset_ref))

            table_infos = []

            for table in tables:
                # Get detailed table info
                table_ref = dataset_ref.table(table.table_id)
                full_table = self._client.get_table(table_ref)

                # Get columns
                columns = []
                for field in full_table.schema:
                    columns.append(
                        ColumnInfo(
                            name=field.name,
                            data_type=field.field_type,
                            is_nullable=field.mode != "REQUIRED",
                            comment=field.description,
                        )
                    )

                table_infos.append(
                    TableInfo(
                        name=table.table_id,
                        schema=dataset_id,
                        columns=columns,
                        row_count=full_table.num_rows,
                        comment=full_table.description,
                        created_at=full_table.created,
                        updated_at=full_table.modified,
                    )
                )

            return table_infos

        except Exception as e:
            raise QueryError(f"Failed to get dataset tables: {str(e)}", "")

    async def get_table_info(
        self, table_name: str, schema_name: str | None = None
    ) -> TableInfo:
        """Get detailed information about a specific table."""
        dataset_id = schema_name or self.config.default_dataset
        if not dataset_id:
            raise ValueError("Dataset ID is required")

        try:
            if not self._client:
                raise QueryError("Not connected to BigQuery", "")
            dataset_ref = self._client.dataset(dataset_id)
            table_ref = dataset_ref.table(table_name)
            table = self._client.get_table(table_ref)

            # Get columns
            columns = []
            for field in table.schema:
                columns.append(
                    ColumnInfo(
                        name=field.name,
                        data_type=field.field_type,
                        is_nullable=field.mode != "REQUIRED",
                        comment=field.description,
                    )
                )

            return TableInfo(
                name=table.table_id,
                schema=dataset_id,
                columns=columns,
                row_count=table.num_rows,
                comment=table.description,
                created_at=table.created,
                updated_at=table.modified,
            )

        except Exception as e:
            raise QueryError(f"Failed to get table info: {str(e)}", "")

    async def get_table_sample(
        self, table_name: str, schema_name: str | None = None, limit: int = 100
    ) -> QueryResult:
        """Get a sample of data from a table."""
        dataset_id = schema_name or self.config.default_dataset
        if not dataset_id:
            raise ValueError("Dataset ID is required")

        full_table_name = f"`{self.config.project_id}.{dataset_id}.{table_name}`"
        sample_query = f"SELECT * FROM {full_table_name} LIMIT {limit}"

        return await self.execute_query(sample_query)
