"""
OLAP Engine for executing multidimensional queries.

This module provides the main engine for executing OLAP operations
on data warehouse connectors.
"""

import time
from typing import Any

from ..connectors.base import DataWarehouseConnector, QueryResult
from .cube import CubeCell, CubeQuery, CubeResult, CubeSchema, DataCube
from .operations import OLAPOperation


class OLAPEngine:
    """
    Main engine for executing OLAP operations on data warehouses.

    This class coordinates between OLAP operations, data cubes,
    and data warehouse connectors to execute multidimensional queries.
    """

    def __init__(self, connector: DataWarehouseConnector):
        """
        Initialize OLAP engine with a data warehouse connector.

        Args:
            connector: Data warehouse connector to use for queries
        """
        self.connector = connector
        self._cubes: dict[str, DataCube] = {}

    def register_cube(self, cube: DataCube) -> None:
        """
        Register a data cube for OLAP operations.

        Args:
            cube: Data cube to register
        """
        self._cubes[cube.schema.name] = cube

    def get_cube(self, name: str) -> DataCube | None:
        """
        Get a registered cube by name.

        Args:
            name: Name of the cube

        Returns:
            DataCube if found, None otherwise
        """
        return self._cubes.get(name)

    def list_cubes(self) -> list[str]:
        """Get names of all registered cubes."""
        return list(self._cubes.keys())

    async def execute_query(self, query: CubeQuery) -> CubeResult:
        """
        Execute an OLAP cube query.

        Args:
            query: Cube query to execute

        Returns:
            CubeResult: Query results

        Raises:
            ValueError: If cube not found or query is invalid
        """
        # Get the cube
        cube = self.get_cube(query.cube_name)
        if not cube:
            raise ValueError(f"Cube '{query.cube_name}' not found")

        # Build SQL query
        sql_query = cube.build_sql_query(query)

        # Execute on data warehouse
        start_time = time.time()
        result = await self.connector.execute_query(sql_query)
        execution_time = int((time.time() - start_time) * 1000)

        # Convert to cube result
        return self._convert_to_cube_result(query, result, execution_time)

    async def execute_operation(
        self,
        cube_name: str,
        operation: OLAPOperation,
        base_query: CubeQuery | None = None,
    ) -> CubeResult:
        """
        Execute an OLAP operation on a cube.

        Args:
            cube_name: Name of the cube
            operation: OLAP operation to execute
            base_query: Optional base query to modify (creates empty query if None)

        Returns:
            CubeResult: Operation results
        """
        # Get the cube
        cube = self.get_cube(cube_name)
        if not cube:
            raise ValueError(f"Cube '{cube_name}' not found")

        # Create base query if not provided
        if base_query is None:
            base_query = cube.create_query()

        # Apply operation
        modified_query = operation.apply(cube, base_query)

        # Execute the modified query
        return await self.execute_query(modified_query)

    async def execute_operations(
        self,
        cube_name: str,
        operations: list[OLAPOperation],
        base_query: CubeQuery | None = None,
    ) -> CubeResult:
        """
        Execute multiple OLAP operations in sequence.

        Args:
            cube_name: Name of the cube
            operations: List of operations to execute in order
            base_query: Optional base query to modify

        Returns:
            CubeResult: Final operation results
        """
        # Get the cube
        cube = self.get_cube(cube_name)
        if not cube:
            raise ValueError(f"Cube '{cube_name}' not found")

        # Create base query if not provided
        current_query = base_query or cube.create_query()

        # Apply operations in sequence
        for operation in operations:
            current_query = operation.apply(cube, current_query)

        # Execute the final query
        return await self.execute_query(current_query)

    def _convert_to_cube_result(
        self, query: CubeQuery, db_result: QueryResult, execution_time: int
    ) -> CubeResult:
        """
        Convert database query result to cube result.

        Args:
            query: Original cube query
            db_result: Database query result
            execution_time: Query execution time in milliseconds

        Returns:
            CubeResult: Converted cube result
        """
        cells = []

        # Get dimension and measure column indices
        dimension_indices = {}
        measure_indices = {}

        for i, column in enumerate(db_result.columns):
            if column in query.dimensions:
                dimension_indices[column] = i
            elif column in query.measures:
                measure_indices[column] = i

        # Convert each row to a cube cell
        for row in db_result.data:
            coordinates = {}
            measures = {}

            # Extract dimension values
            for dim_name, col_index in dimension_indices.items():
                coordinates[dim_name] = row[col_index] if col_index < len(row) else None

            # Extract measure values
            for measure_name, col_index in measure_indices.items():
                measures[measure_name] = (
                    row[col_index] if col_index < len(row) else None
                )

            cells.append(CubeCell(coordinates=coordinates, measures=measures))

        return CubeResult(
            cube_name=query.cube_name,
            query=query,
            cells=cells,
            total_cells=len(cells),
            execution_time_ms=execution_time,
        )

    async def get_cube_metadata(self, cube_name: str) -> dict[str, Any]:
        """
        Get metadata about a cube including available dimensions and measures.

        Args:
            cube_name: Name of the cube

        Returns:
            Dict containing cube metadata
        """
        cube = self.get_cube(cube_name)
        if not cube:
            raise ValueError(f"Cube '{cube_name}' not found")

        return {
            "name": cube.schema.name,
            "description": cube.schema.description,
            "table": cube.schema.get_full_table_name(),
            "dimensions": [
                {
                    "name": dim.name,
                    "type": dim.dimension_type.value,
                    "description": dim.description,
                    "hierarchical": dim.is_hierarchical(),
                    "levels": dim.get_level_names() if dim.is_hierarchical() else None,
                }
                for dim in cube.schema.dimensions
            ],
            "measures": [
                {
                    "name": measure.name,
                    "aggregation": measure.aggregation.value,
                    "description": measure.description,
                    "format": measure.format,
                }
                for measure in cube.schema.measures
            ],
            "created_at": cube.schema.created_at.isoformat(),
        }

    async def discover_cube_from_table(
        self,
        table_name: str,
        schema_name: str | None = None,
        cube_name: str | None = None,
    ) -> DataCube:
        """
        Automatically discover and create a cube from a database table.

        This method analyzes a table structure and creates a basic cube
        definition with reasonable defaults for dimensions and measures.

        Args:
            table_name: Name of the table
            schema_name: Optional schema name
            cube_name: Optional cube name (defaults to table name)

        Returns:
            DataCube: Auto-generated cube
        """
        # Get table information
        table_info = await self.connector.get_table_info(table_name, schema_name)

        # Analyze columns to identify potential dimensions and measures
        dimensions = []
        measures = []

        for column in table_info.columns:
            col_type = column.data_type.lower()

            # Heuristics for identifying dimensions vs measures
            if any(
                keyword in col_type for keyword in ["varchar", "char", "text", "string"]
            ):
                # String columns are likely dimensions
                from .cube import Dimension, DimensionType

                dimensions.append(
                    Dimension(
                        name=column.name,
                        column=column.name,
                        dimension_type=DimensionType.CATEGORICAL,
                        description=column.comment,
                    )
                )

            elif any(keyword in col_type for keyword in ["date", "time", "timestamp"]):
                # Date/time columns are temporal dimensions
                from .cube import Dimension, DimensionType

                dimensions.append(
                    Dimension(
                        name=column.name,
                        column=column.name,
                        dimension_type=DimensionType.TEMPORAL,
                        description=column.comment,
                    )
                )

            elif any(
                keyword in col_type
                for keyword in ["int", "float", "decimal", "numeric"]
            ):
                # Numeric columns could be measures or dimensions
                # Use naming heuristics
                col_name_lower = column.name.lower()

                if any(
                    keyword in col_name_lower
                    for keyword in ["id", "key", "code", "type"]
                ):
                    # Likely a dimension
                    from .cube import Dimension, DimensionType

                    dimensions.append(
                        Dimension(
                            name=column.name,
                            column=column.name,
                            dimension_type=DimensionType.NUMERICAL,
                            description=column.comment,
                        )
                    )
                else:
                    # Likely a measure
                    from .cube import AggregationType, Measure

                    # Choose aggregation based on column name
                    if any(
                        keyword in col_name_lower
                        for keyword in ["count", "qty", "quantity"]
                    ):
                        aggregation = AggregationType.SUM
                    elif any(
                        keyword in col_name_lower
                        for keyword in ["amount", "total", "sum"]
                    ):
                        aggregation = AggregationType.SUM
                    elif any(
                        keyword in col_name_lower
                        for keyword in ["avg", "average", "mean"]
                    ):
                        aggregation = AggregationType.AVG
                    else:
                        aggregation = AggregationType.SUM  # Default

                    measures.append(
                        Measure(
                            name=column.name,
                            column=column.name,
                            aggregation=aggregation,
                            description=column.comment,
                        )
                    )

        # Ensure we have at least one dimension and one measure
        if not dimensions:
            raise ValueError(f"No suitable dimensions found in table {table_name}")

        if not measures:
            # Create a count measure as fallback
            from .cube import AggregationType, Measure

            measures.append(
                Measure(
                    name="record_count",
                    column="*",
                    aggregation=AggregationType.COUNT,
                    description="Count of records",
                )
            )

        # Create cube schema
        cube_name = cube_name or table_name
        schema = CubeSchema(
            name=cube_name,
            table=table_name,
            schema=schema_name,
            dimensions=dimensions,
            measures=measures,
            description=f"Auto-generated cube for table {table_name}",
        )

        # Create and register cube
        cube = DataCube(schema)
        self.register_cube(cube)

        return cube
