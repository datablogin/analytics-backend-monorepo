"""
Federated query engine for cross-source analytics.

This module provides capabilities to execute queries across
multiple data warehouses and combine results.
"""

import asyncio
from typing import Any

from pydantic import BaseModel

from ..connectors.base import (
    DataWarehouseConnector,
    QueryMetadata,
    QueryResult,
    WarehouseType,
)


class DataSource(BaseModel):
    """Represents a data source in federated queries."""

    name: str
    connector: DataWarehouseConnector
    warehouse_type: WarehouseType
    priority: int = 1  # Higher priority sources are queried first
    timeout_seconds: int = 300
    retry_count: int = 2

    class Config:
        arbitrary_types_allowed = True


class FederatedQuery(BaseModel):
    """Represents a query that spans multiple data sources."""

    name: str
    sources: list[str]  # Data source names
    queries: dict[str, str]  # Source name -> SQL query
    join_strategy: str = "union"  # union, join, merge
    join_keys: list[str] | None = None
    aggregation: dict[str, str] | None = None  # column -> aggregation type

    def validate_sources(self, available_sources: list[str]) -> None:
        """Validate that all required sources are available."""
        missing = set(self.sources) - set(available_sources)
        if missing:
            raise ValueError(f"Missing data sources: {missing}")


class FederatedResult(BaseModel):
    """Result of a federated query execution."""

    query_name: str
    source_results: dict[str, QueryResult]
    combined_result: QueryResult | None = None
    execution_order: list[str]
    total_execution_time_ms: int
    success: bool = True
    errors: dict[str, str] = {}

    def get_total_rows(self) -> int:
        """Get total rows across all sources."""
        if self.combined_result:
            return self.combined_result.total_rows or 0

        return sum(result.total_rows or 0 for result in self.source_results.values())

    def get_source_count(self) -> int:
        """Get number of data sources queried."""
        return len(self.source_results)


class FederatedQueryEngine:
    """
    Engine for executing queries across multiple data warehouses.

    Provides capabilities for:
    - Parallel query execution across multiple sources
    - Result combination and aggregation
    - Query optimization and routing
    - Error handling and fallback strategies
    """

    def __init__(self):
        """Initialize federated query engine."""
        self._sources: dict[str, DataSource] = {}
        self._queries: dict[str, FederatedQuery] = {}

    def register_source(
        self,
        name: str,
        connector: DataWarehouseConnector,
        priority: int = 1,
        timeout_seconds: int = 300,
        retry_count: int = 2,
    ) -> None:
        """
        Register a data source for federated queries.

        Args:
            name: Unique name for the data source
            connector: Data warehouse connector
            priority: Query priority (higher = earlier execution)
            timeout_seconds: Query timeout for this source
            retry_count: Number of retries on failure
        """
        source = DataSource(
            name=name,
            connector=connector,
            warehouse_type=connector.warehouse_type,
            priority=priority,
            timeout_seconds=timeout_seconds,
            retry_count=retry_count,
        )

        self._sources[name] = source

    def unregister_source(self, name: str) -> bool:
        """
        Unregister a data source.

        Args:
            name: Name of source to remove

        Returns:
            bool: True if source was removed, False if not found
        """
        if name in self._sources:
            del self._sources[name]
            return True
        return False

    def register_query(self, query: FederatedQuery) -> None:
        """
        Register a federated query definition.

        Args:
            query: Federated query to register
        """
        query.validate_sources(list(self._sources.keys()))
        self._queries[query.name] = query

    def get_sources(self) -> list[str]:
        """Get names of all registered sources."""
        return list(self._sources.keys())

    def get_queries(self) -> list[str]:
        """Get names of all registered queries."""
        return list(self._queries.keys())

    async def execute_on_source(
        self, source_name: str, query: str, params: dict[str, Any] | None = None
    ) -> QueryResult:
        """
        Execute a query on a specific data source.

        Args:
            source_name: Name of the data source
            query: SQL query to execute
            params: Optional query parameters

        Returns:
            QueryResult: Query execution result
        """
        if source_name not in self._sources:
            raise ValueError(f"Data source '{source_name}' not found")

        source = self._sources[source_name]

        # Execute with retries
        last_exception = None
        for attempt in range(source.retry_count + 1):
            try:
                return await source.connector.execute_query(
                    query, params, timeout=source.timeout_seconds
                )
            except Exception as e:
                last_exception = e
                if attempt < source.retry_count:
                    # Wait before retry (exponential backoff)
                    await asyncio.sleep(2**attempt)
                else:
                    raise e

        # This shouldn't be reached, but just in case
        if last_exception:
            raise last_exception
        else:
            raise RuntimeError("Unexpected execution path")

    async def execute_parallel(
        self,
        queries: dict[str, str],
        params: dict[str, dict[str, Any]] | None = None,
    ) -> dict[str, QueryResult | Exception]:
        """
        Execute queries in parallel across multiple sources.

        Args:
            queries: Dictionary of source_name -> query
            params: Optional parameters for each query

        Returns:
            Dictionary of source_name -> result or exception
        """
        if not queries:
            return {}

        # Validate all sources exist
        missing_sources = set(queries.keys()) - set(self._sources.keys())
        if missing_sources:
            raise ValueError(f"Unknown data sources: {missing_sources}")

        # Create tasks for parallel execution
        tasks = {}
        for source_name, query in queries.items():
            source_params = params.get(source_name) if params else None

            task = asyncio.create_task(
                self._execute_with_error_handling(source_name, query, source_params)
            )
            tasks[source_name] = task

        # Wait for all tasks to complete
        results = {}
        for source_name, task in tasks.items():
            results[source_name] = await task

        return results

    async def _execute_with_error_handling(
        self, source_name: str, query: str, params: dict[str, Any] | None = None
    ) -> QueryResult | Exception:
        """Execute query with error handling."""
        try:
            return await self.execute_on_source(source_name, query, params)
        except Exception as e:
            return e

    async def execute_federated_query(
        self, query_name: str, params: dict[str, dict[str, Any]] | None = None
    ) -> FederatedResult:
        """
        Execute a registered federated query.

        Args:
            query_name: Name of the federated query
            params: Optional parameters for each source query

        Returns:
            FederatedResult: Combined results from all sources
        """
        if query_name not in self._queries:
            raise ValueError(f"Federated query '{query_name}' not found")

        federated_query = self._queries[query_name]

        import time

        start_time = time.time()

        # Execute queries in parallel
        results = await self.execute_parallel(federated_query.queries, params)

        # Separate successful results from errors
        source_results = {}
        errors = {}

        for source_name, result in results.items():
            if isinstance(result, Exception):
                errors[source_name] = str(result)
            else:
                source_results[source_name] = result

        # Determine execution order (by priority, then alphabetically)
        execution_order = sorted(
            federated_query.sources, key=lambda s: (-self._sources[s].priority, s)
        )

        # Combine results if we have successful executions
        combined_result = None
        if source_results:
            combined_result = await self._combine_results(
                source_results,
                federated_query.join_strategy,
                federated_query.join_keys,
                federated_query.aggregation,
            )

        total_time = int((time.time() - start_time) * 1000)

        return FederatedResult(
            query_name=query_name,
            source_results=source_results,
            combined_result=combined_result,
            execution_order=execution_order,
            total_execution_time_ms=total_time,
            success=len(errors) == 0,
            errors=errors,
        )

    async def _combine_results(
        self,
        results: dict[str, QueryResult],
        join_strategy: str,
        join_keys: list[str] | None = None,
        aggregation: dict[str, str] | None = None,
    ) -> QueryResult:
        """
        Combine results from multiple sources.

        Args:
            results: Results from each source
            join_strategy: How to combine results
            join_keys: Keys for joining (if applicable)
            aggregation: Aggregation rules (if applicable)

        Returns:
            QueryResult: Combined result
        """
        if not results:
            return QueryResult(
                columns=[],
                data=[],
                metadata=QueryMetadata(execution_time_ms=0),
                total_rows=0,
            )

        if join_strategy == "union":
            return await self._union_results(results)
        elif join_strategy == "join" and join_keys:
            return await self._join_results(results, join_keys)
        elif join_strategy == "merge":
            return await self._merge_results(results, aggregation)
        else:
            # Default to union
            return await self._union_results(results)

    async def _union_results(self, results: dict[str, QueryResult]) -> QueryResult:
        """Union all results together."""
        if not results:
            return QueryResult(
                columns=[],
                data=[],
                metadata=QueryMetadata(execution_time_ms=0),
                total_rows=0,
            )

        # Get first result to establish schema
        first_result = next(iter(results.values()))
        combined_columns = first_result.columns.copy()
        combined_data = []
        total_execution_time = 0
        total_rows = 0

        # Combine data from all results
        for source_name, result in results.items():
            # Ensure column compatibility
            if result.columns != combined_columns:
                # Try to align columns - this is a simplified approach
                # In production, you'd want more sophisticated schema alignment
                aligned_data = self._align_columns(result, combined_columns)
                combined_data.extend(aligned_data)
            else:
                combined_data.extend(result.data)

            total_execution_time += result.metadata.execution_time_ms
            total_rows += result.total_rows or 0

        # Create combined metadata
        combined_metadata = QueryMetadata(
            execution_time_ms=total_execution_time, rows_scanned=total_rows
        )

        return QueryResult(
            columns=combined_columns,
            data=combined_data,
            metadata=combined_metadata,
            total_rows=total_rows,
        )

    async def _join_results(
        self, results: dict[str, QueryResult], join_keys: list[str]
    ) -> QueryResult:
        """Join results based on specified keys."""
        # This is a simplified implementation
        # A full implementation would handle different join types

        if len(results) < 2:
            return await self._union_results(results)

        # Convert first result to lookup dict
        result_items = list(results.items())
        left_name, left_result = result_items[0]

        # Build lookup index for left result
        left_key_indices = []
        for key in join_keys:
            if key in left_result.columns:
                left_key_indices.append(left_result.columns.index(key))

        if not left_key_indices:
            # No join keys found, fall back to union
            return await self._union_results(results)

        # Build lookup dictionary
        left_lookup = {}
        for row in left_result.data:
            key_values = tuple(row[i] for i in left_key_indices)
            left_lookup[key_values] = row

        # Join with remaining results
        for right_name, right_result in result_items[1:]:
            # Find matching join key indices in right result
            right_key_indices = []
            for key in join_keys:
                if key in right_result.columns:
                    right_key_indices.append(right_result.columns.index(key))

            if not right_key_indices:
                continue

            # Perform join
            joined_data = []
            for right_row in right_result.data:
                key_values = tuple(right_row[i] for i in right_key_indices)
                if key_values in left_lookup:
                    left_row = left_lookup[key_values]
                    # Combine rows (simplified - avoid duplicate join keys)
                    joined_row = left_row + [
                        right_row[i]
                        for i in range(len(right_row))
                        if i not in right_key_indices
                    ]
                    joined_data.append(joined_row)

            # Update for next iteration
            left_result = QueryResult(
                columns=left_result.columns
                + [
                    col
                    for i, col in enumerate(right_result.columns)
                    if i not in right_key_indices
                ],
                data=joined_data,
                metadata=left_result.metadata,
                total_rows=len(joined_data),
            )

            # Rebuild lookup
            left_lookup = {}
            for row in joined_data:
                key_values = tuple(row[i] for i in left_key_indices)
                left_lookup[key_values] = row

        return left_result

    async def _merge_results(
        self,
        results: dict[str, QueryResult],
        aggregation: dict[str, str] | None = None,
    ) -> QueryResult:
        """Merge results with aggregation."""
        # This is a placeholder for aggregation logic
        # Would implement grouping and aggregation functions
        return await self._union_results(results)

    def _align_columns(
        self, result: QueryResult, target_columns: list[str]
    ) -> list[list[Any]]:
        """Align result columns to match target schema."""
        if result.columns == target_columns:
            return result.data

        # Create mapping from source to target columns
        column_mapping = {}
        for i, col in enumerate(result.columns):
            if col in target_columns:
                column_mapping[i] = target_columns.index(col)

        # Align data
        aligned_data = []
        for row in result.data:
            aligned_row = [None] * len(target_columns)

            for source_idx, target_idx in column_mapping.items():
                if source_idx < len(row):
                    aligned_row[target_idx] = row[source_idx]

            aligned_data.append(aligned_row)

        return aligned_data

    async def test_connectivity(self) -> dict[str, bool]:
        """
        Test connectivity to all registered data sources.

        Returns:
            Dictionary of source_name -> connectivity status
        """
        results = {}

        for source_name, source in self._sources.items():
            try:
                is_connected = await source.connector.test_connection()
                results[source_name] = is_connected
            except Exception:
                results[source_name] = False

        return results

    def get_federation_stats(self) -> dict[str, Any]:
        """Get statistics about the federation."""
        source_types: dict[str, int] = {}
        for source in self._sources.values():
            warehouse_type = source.warehouse_type.value
            source_types[warehouse_type] = source_types.get(warehouse_type, 0) + 1

        return {
            "total_sources": len(self._sources),
            "total_queries": len(self._queries),
            "source_types": source_types,
            "avg_priority": (
                sum(s.priority for s in self._sources.values()) / len(self._sources)
                if self._sources
                else 0
            ),
        }
