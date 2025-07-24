"""
Data cube definitions and structures for OLAP operations.

This module defines the core data structures for representing
multidimensional data cubes in OLAP operations.
"""

from datetime import datetime
from enum import Enum
from typing import Any

from pydantic import BaseModel, Field


class AggregationType(str, Enum):
    """Supported aggregation types for measures."""

    SUM = "sum"
    COUNT = "count"
    AVG = "avg"
    MIN = "min"
    MAX = "max"
    COUNT_DISTINCT = "count_distinct"
    STDDEV = "stddev"
    VARIANCE = "variance"


class DimensionType(str, Enum):
    """Types of dimensions in OLAP cubes."""

    CATEGORICAL = "categorical"
    TEMPORAL = "temporal"
    NUMERICAL = "numerical"
    HIERARCHICAL = "hierarchical"


class Level(BaseModel):
    """A level in a hierarchical dimension."""

    name: str
    column: str
    order: int
    description: str | None = None


class Dimension(BaseModel):
    """A dimension in an OLAP cube."""

    name: str
    column: str
    dimension_type: DimensionType
    description: str | None = None
    levels: list[Level] | None = None  # For hierarchical dimensions

    def is_hierarchical(self) -> bool:
        """Check if this dimension has hierarchical structure."""
        return (
            self.dimension_type == DimensionType.HIERARCHICAL
            and self.levels is not None
        )

    def get_level_names(self) -> list[str]:
        """Get names of all levels in hierarchical dimension."""
        if not self.is_hierarchical():
            return [self.name]
        return [level.name for level in sorted(self.levels, key=lambda x: x.order)]


class Measure(BaseModel):
    """A measure (metric) in an OLAP cube."""

    name: str
    column: str
    aggregation: AggregationType
    description: str | None = None
    format: str | None = None  # Display format (e.g., "${:,.2f}")

    def get_sql_expression(self, alias: str | None = None) -> str:
        """Generate SQL expression for this measure."""
        expr = f"{self.aggregation.value.upper()}({self.column})"
        if alias:
            expr += f" AS {alias}"
        elif self.name != self.column:
            expr += f" AS {self.name}"
        return expr


class CubeSchema(BaseModel):
    """Schema definition for an OLAP cube."""

    name: str
    table: str
    schema: str | None = None
    dimensions: list[Dimension]
    measures: list[Measure]
    description: str | None = None
    created_at: datetime = Field(default_factory=datetime.now)

    def get_dimension(self, name: str) -> Dimension | None:
        """Get dimension by name."""
        for dim in self.dimensions:
            if dim.name == name:
                return dim
        return None

    def get_measure(self, name: str) -> Measure | None:
        """Get measure by name."""
        for measure in self.measures:
            if measure.name == name:
                return measure
        return None

    def get_full_table_name(self) -> str:
        """Get fully qualified table name."""
        if self.schema:
            return f"{self.schema}.{self.table}"
        return self.table


class CubeQuery(BaseModel):
    """Query definition for OLAP cube operations."""

    cube_name: str
    dimensions: list[str] = Field(default_factory=list)
    measures: list[str] = Field(default_factory=list)
    filters: dict[str, Any] = Field(default_factory=dict)
    having: dict[str, Any] = Field(default_factory=dict)
    order_by: list[str] = Field(default_factory=list)
    limit: int | None = None

    def add_dimension(self, dimension: str) -> None:
        """Add a dimension to the query."""
        if dimension not in self.dimensions:
            self.dimensions.append(dimension)

    def add_measure(self, measure: str) -> None:
        """Add a measure to the query."""
        if measure not in self.measures:
            self.measures.append(measure)

    def add_filter(self, dimension: str, value: Any) -> None:
        """Add a filter condition."""
        self.filters[dimension] = value

    def add_having_condition(self, measure: str, condition: Any) -> None:
        """Add a HAVING condition for aggregated measures."""
        self.having[measure] = condition


class CubeCell(BaseModel):
    """A single cell in an OLAP cube result."""

    coordinates: dict[str, Any]  # Dimension values
    measures: dict[str, Any]  # Measure values

    def get_dimension_value(self, dimension: str) -> Any:
        """Get value for a specific dimension."""
        return self.coordinates.get(dimension)

    def get_measure_value(self, measure: str) -> Any:
        """Get value for a specific measure."""
        return self.measures.get(measure)


class CubeResult(BaseModel):
    """Result of an OLAP cube query."""

    cube_name: str
    query: CubeQuery
    cells: list[CubeCell]
    total_cells: int
    execution_time_ms: int

    def to_dataframe(self):
        """Convert result to pandas DataFrame."""
        try:
            import pandas as pd

            if not self.cells:
                return pd.DataFrame()

            # Extract all data into rows
            rows = []
            for cell in self.cells:
                row = {}
                row.update(cell.coordinates)
                row.update(cell.measures)
                rows.append(row)

            return pd.DataFrame(rows)

        except ImportError:
            raise ImportError("pandas is required to convert to DataFrame")

    def get_dimension_values(self, dimension: str) -> list[Any]:
        """Get all unique values for a dimension."""
        values = set()
        for cell in self.cells:
            value = cell.get_dimension_value(dimension)
            if value is not None:
                values.add(value)
        return sorted(list(values))

    def get_measure_summary(self, measure: str) -> dict[str, Any]:
        """Get summary statistics for a measure."""
        values = []
        for cell in self.cells:
            value = cell.get_measure_value(measure)
            if value is not None and isinstance(value, int | float):
                values.append(value)

        if not values:
            return {"count": 0}

        return {
            "count": len(values),
            "sum": sum(values),
            "min": min(values),
            "max": max(values),
            "avg": sum(values) / len(values),
        }


class DataCube:
    """
    Represents a multidimensional data cube for OLAP operations.

    This class provides methods for performing OLAP operations like
    slice, dice, drill-down, drill-up, and pivot on multidimensional data.
    """

    def __init__(self, schema: CubeSchema):
        """Initialize data cube with schema definition."""
        self.schema = schema
        self._validate_schema()

    def _validate_schema(self) -> None:
        """Validate cube schema for consistency."""
        if not self.schema.dimensions:
            raise ValueError("Cube must have at least one dimension")

        if not self.schema.measures:
            raise ValueError("Cube must have at least one measure")

        # Check for duplicate dimension names
        dim_names = [dim.name for dim in self.schema.dimensions]
        if len(dim_names) != len(set(dim_names)):
            raise ValueError("Duplicate dimension names found")

        # Check for duplicate measure names
        measure_names = [measure.name for measure in self.schema.measures]
        if len(measure_names) != len(set(measure_names)):
            raise ValueError("Duplicate measure names found")

    def create_query(self) -> CubeQuery:
        """Create a new cube query."""
        return CubeQuery(cube_name=self.schema.name)

    def get_dimension_names(self) -> list[str]:
        """Get names of all dimensions."""
        return [dim.name for dim in self.schema.dimensions]

    def get_measure_names(self) -> list[str]:
        """Get names of all measures."""
        return [measure.name for measure in self.schema.measures]

    def get_hierarchical_dimensions(self) -> list[Dimension]:
        """Get all hierarchical dimensions."""
        return [dim for dim in self.schema.dimensions if dim.is_hierarchical()]

    def build_sql_query(self, query: CubeQuery) -> tuple[str, dict[str, Any]]:
        """Build SQL query from cube query definition.

        Returns:
            tuple: (SQL query with placeholders, parameters dict)
        """
        # SELECT clause
        select_parts = []

        # Add dimensions
        for dim_name in query.dimensions:
            dimension = self.schema.get_dimension(dim_name)
            if dimension:
                select_parts.append(f"{dimension.column} AS {dimension.name}")

        # Add measures
        for measure_name in query.measures:
            measure = self.schema.get_measure(measure_name)
            if measure:
                select_parts.append(measure.get_sql_expression())

        if not select_parts:
            raise ValueError("Query must include at least one dimension or measure")

        select_clause = "SELECT " + ", ".join(select_parts)

        # FROM clause
        from_clause = f"FROM {self.schema.get_full_table_name()}"

        # WHERE clause with parameterized queries
        where_conditions = []
        parameters: dict[str, Any] = {}
        param_counter = 0

        for dim_name, value in query.filters.items():
            dimension = self.schema.get_dimension(dim_name)
            if dimension:
                if isinstance(value, list):
                    # IN clause with parameters
                    param_names = []
                    for v in value:
                        param_name = f"param_{param_counter}"
                        parameters[param_name] = v
                        param_names.append(f":{param_name}")
                        param_counter += 1
                    where_conditions.append(
                        f"{dimension.column} IN ({', '.join(param_names)})"
                    )
                else:
                    # Equality with parameter
                    param_name = f"param_{param_counter}"
                    parameters[param_name] = value
                    where_conditions.append(f"{dimension.column} = :{param_name}")
                    param_counter += 1

        where_clause = ""
        if where_conditions:
            where_clause = "WHERE " + " AND ".join(where_conditions)

        # GROUP BY clause
        group_by_clause = ""
        if query.dimensions:
            group_columns = []
            for dim_name in query.dimensions:
                dimension = self.schema.get_dimension(dim_name)
                if dimension:
                    group_columns.append(dimension.column)

            if group_columns:
                group_by_clause = "GROUP BY " + ", ".join(group_columns)

        # HAVING clause (Note: HAVING conditions would need separate parameterization
        # for complex conditions, but keeping simple for now)
        having_clause = ""
        if query.having:
            having_conditions = []
            for measure_name, condition in query.having.items():
                measure = self.schema.get_measure(measure_name)
                if measure:
                    # For basic conditions like "> 100", keeping as-is
                    # TODO: Implement full parameterization for complex HAVING conditions
                    having_conditions.append(
                        f"{measure.get_sql_expression()} {condition}"
                    )

            if having_conditions:
                having_clause = "HAVING " + " AND ".join(having_conditions)

        # ORDER BY clause
        order_by_clause = ""
        if query.order_by:
            order_by_clause = "ORDER BY " + ", ".join(query.order_by)

        # LIMIT clause
        limit_clause = ""
        if query.limit:
            limit_clause = f"LIMIT {query.limit}"

        # Combine all clauses
        sql_parts = [
            select_clause,
            from_clause,
            where_clause,
            group_by_clause,
            having_clause,
            order_by_clause,
            limit_clause,
        ]

        sql_query = "\n".join(part for part in sql_parts if part)
        return sql_query, parameters
