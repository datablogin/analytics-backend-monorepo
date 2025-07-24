"""
OLAP operation implementations.

This module provides implementations for core OLAP operations:
- Slice: Select a subset of dimensions with specific values
- Dice: Filter data based on multiple dimension criteria
- Drill-down: Navigate from higher to lower level of detail
- Drill-up: Navigate from lower to higher level of detail
- Pivot: Rotate cube to change dimension orientation
"""

from abc import ABC, abstractmethod
from typing import Any

from .cube import CubeQuery, DataCube


class OLAPOperation(ABC):
    """Abstract base class for OLAP operations."""

    @abstractmethod
    def apply(self, cube: DataCube, query: CubeQuery) -> CubeQuery:
        """Apply the operation to a cube query."""
        pass

    @abstractmethod
    def get_operation_name(self) -> str:
        """Get the name of this operation."""
        pass


class SliceOperation(OLAPOperation):
    """
    Slice operation: Select a subset by fixing one or more dimensions.

    Example: Show sales data for only "California" region.
    """

    def __init__(self, dimension: str, value: Any):
        """
        Initialize slice operation.

        Args:
            dimension: Name of dimension to slice on
            value: Value to filter by
        """
        self.dimension = dimension
        self.value = value

    def apply(self, cube: DataCube, query: CubeQuery) -> CubeQuery:
        """Apply slice operation to query."""
        new_query = query.model_copy(deep=True)

        # Remove dimension from selection if it exists
        if self.dimension in new_query.dimensions:
            new_query.dimensions.remove(self.dimension)

        # Add filter condition
        new_query.add_filter(self.dimension, self.value)

        return new_query

    def get_operation_name(self) -> str:
        """Get operation name."""
        return f"slice({self.dimension}={self.value})"


class DiceOperation(OLAPOperation):
    """
    Dice operation: Select a subcube by specifying ranges/sets for multiple dimensions.

    Example: Show sales data for "California" and "New York" regions in Q1 and Q2.
    """

    def __init__(self, filters: dict[str, Any | list[Any]]):
        """
        Initialize dice operation.

        Args:
            filters: Dictionary of dimension filters
        """
        self.filters = filters

    def apply(self, cube: DataCube, query: CubeQuery) -> CubeQuery:
        """Apply dice operation to query."""
        new_query = query.model_copy(deep=True)

        # Add all filter conditions
        for dimension, value in self.filters.items():
            new_query.add_filter(dimension, value)

        return new_query

    def get_operation_name(self) -> str:
        """Get operation name."""
        filter_strs = [f"{k}={v}" for k, v in self.filters.items()]
        return f"dice({', '.join(filter_strs)})"


class DrillDownOperation(OLAPOperation):
    """
    Drill-down operation: Navigate from summary to more detailed data.

    Example: From yearly sales to monthly sales, or from region to city.
    """

    def __init__(
        self,
        dimension: str,
        from_level: str | None = None,
        to_level: str | None = None,
    ):
        """
        Initialize drill-down operation.

        Args:
            dimension: Name of hierarchical dimension
            from_level: Current level (optional)
            to_level: Target level to drill down to (optional)
        """
        self.dimension = dimension
        self.from_level = from_level
        self.to_level = to_level

    def apply(self, cube: DataCube, query: CubeQuery) -> CubeQuery:
        """Apply drill-down operation to query."""
        new_query = query.model_copy(deep=True)

        # Get dimension definition
        dimension_def = cube.schema.get_dimension(self.dimension)
        if not dimension_def:
            raise ValueError(f"Dimension '{self.dimension}' not found in cube")

        if not dimension_def.is_hierarchical():
            raise ValueError(f"Dimension '{self.dimension}' is not hierarchical")

        # Add the dimension to query if not already present
        if self.dimension not in new_query.dimensions:
            new_query.add_dimension(self.dimension)

        # If specific levels are provided, we would need to modify the
        # dimension column to use the appropriate level
        # This would require extending the schema to support level-specific columns

        return new_query

    def get_operation_name(self) -> str:
        """Get operation name."""
        if self.from_level and self.to_level:
            return f"drill_down({self.dimension}: {self.from_level} -> {self.to_level})"
        return f"drill_down({self.dimension})"


class DrillUpOperation(OLAPOperation):
    """
    Drill-up operation: Navigate from detailed to summary data.

    Example: From monthly sales to yearly sales, or from city to region.
    """

    def __init__(
        self,
        dimension: str,
        from_level: str | None = None,
        to_level: str | None = None,
    ):
        """
        Initialize drill-up operation.

        Args:
            dimension: Name of hierarchical dimension
            from_level: Current level (optional)
            to_level: Target level to drill up to (optional)
        """
        self.dimension = dimension
        self.from_level = from_level
        self.to_level = to_level

    def apply(self, cube: DataCube, query: CubeQuery) -> CubeQuery:
        """Apply drill-up operation to query."""
        new_query = query.model_copy(deep=True)

        # Get dimension definition
        dimension_def = cube.schema.get_dimension(self.dimension)
        if not dimension_def:
            raise ValueError(f"Dimension '{self.dimension}' not found in cube")

        if not dimension_def.is_hierarchical():
            raise ValueError(f"Dimension '{self.dimension}' is not hierarchical")

        # For drill-up, we might need to remove the dimension from the query
        # or change it to a higher level aggregation
        # This depends on the specific hierarchy implementation

        return new_query

    def get_operation_name(self) -> str:
        """Get operation name."""
        if self.from_level and self.to_level:
            return f"drill_up({self.dimension}: {self.from_level} -> {self.to_level})"
        return f"drill_up({self.dimension})"


class PivotOperation(OLAPOperation):
    """
    Pivot operation: Rotate the cube to change dimension orientation.

    Example: Change from rows=[Region, Product] to rows=[Product, Region].
    """

    def __init__(
        self,
        new_row_dimensions: list[str],
        new_column_dimensions: list[str] | None = None,
    ):
        """
        Initialize pivot operation.

        Args:
            new_row_dimensions: New order of row dimensions
            new_column_dimensions: Optional column dimensions for 2D pivot
        """
        self.new_row_dimensions = new_row_dimensions
        self.new_column_dimensions = new_column_dimensions or []

    def apply(self, cube: DataCube, query: CubeQuery) -> CubeQuery:
        """Apply pivot operation to query."""
        new_query = query.model_copy(deep=True)

        # Reorder dimensions
        all_new_dimensions = self.new_row_dimensions + self.new_column_dimensions

        # Validate that all dimensions exist in the cube
        cube_dimensions = cube.get_dimension_names()
        for dim in all_new_dimensions:
            if dim not in cube_dimensions:
                raise ValueError(f"Dimension '{dim}' not found in cube")

        # Update query dimensions
        new_query.dimensions = all_new_dimensions

        return new_query

    def get_operation_name(self) -> str:
        """Get operation name."""
        if self.new_column_dimensions:
            return f"pivot(rows={self.new_row_dimensions}, cols={self.new_column_dimensions})"
        return f"pivot({self.new_row_dimensions})"


class RollUpOperation(OLAPOperation):
    """
    Roll-up operation: Remove a dimension to get higher-level aggregation.

    Example: Remove the "Product" dimension to get regional totals.
    """

    def __init__(self, dimension: str):
        """
        Initialize roll-up operation.

        Args:
            dimension: Name of dimension to remove
        """
        self.dimension = dimension

    def apply(self, cube: DataCube, query: CubeQuery) -> CubeQuery:
        """Apply roll-up operation to query."""
        new_query = query.model_copy(deep=True)

        # Remove dimension from query
        if self.dimension in new_query.dimensions:
            new_query.dimensions.remove(self.dimension)

        # Remove any filters on this dimension
        if self.dimension in new_query.filters:
            del new_query.filters[self.dimension]

        return new_query

    def get_operation_name(self) -> str:
        """Get operation name."""
        return f"roll_up({self.dimension})"


class FilterOperation(OLAPOperation):
    """
    Filter operation: Apply additional filtering conditions.

    This is similar to dice but focuses on adding constraints
    without necessarily removing dimensions from the result.
    """

    def __init__(self, conditions: dict[str, Any]):
        """
        Initialize filter operation.

        Args:
            conditions: Dictionary of filter conditions
        """
        self.conditions = conditions

    def apply(self, cube: DataCube, query: CubeQuery) -> CubeQuery:
        """Apply filter operation to query."""
        new_query = query.model_copy(deep=True)

        # Add filter conditions
        for dimension, condition in self.conditions.items():
            new_query.add_filter(dimension, condition)

        return new_query

    def get_operation_name(self) -> str:
        """Get operation name."""
        condition_strs = [f"{k}={v}" for k, v in self.conditions.items()]
        return f"filter({', '.join(condition_strs)})"


class TopNOperation(OLAPOperation):
    """
    Top-N operation: Get top N records based on a measure.

    Example: Get top 10 products by sales revenue.
    """

    def __init__(self, measure: str, n: int, ascending: bool = False):
        """
        Initialize top-N operation.

        Args:
            measure: Name of measure to rank by
            n: Number of top records to return
            ascending: If True, get bottom N instead of top N
        """
        self.measure = measure
        self.n = n
        self.ascending = ascending

    def apply(self, cube: DataCube, query: CubeQuery) -> CubeQuery:
        """Apply top-N operation to query."""
        new_query = query.model_copy(deep=True)

        # Add the measure if not already present
        if self.measure not in new_query.measures:
            new_query.add_measure(self.measure)

        # Set ordering
        order_direction = "ASC" if self.ascending else "DESC"
        new_query.order_by = [f"{self.measure} {order_direction}"]

        # Set limit
        new_query.limit = self.n

        return new_query

    def get_operation_name(self) -> str:
        """Get operation name."""
        direction = "bottom" if self.ascending else "top"
        return f"{direction}_{self.n}({self.measure})"


class CompositeOperation(OLAPOperation):
    """
    Composite operation: Combine multiple OLAP operations.

    This allows chaining operations like slice + dice + drill-down.
    """

    def __init__(self, operations: list[OLAPOperation]):
        """
        Initialize composite operation.

        Args:
            operations: List of operations to apply in sequence
        """
        self.operations = operations

    def apply(self, cube: DataCube, query: CubeQuery) -> CubeQuery:
        """Apply all operations in sequence."""
        result_query = query

        for operation in self.operations:
            result_query = operation.apply(cube, result_query)

        return result_query

    def get_operation_name(self) -> str:
        """Get operation name."""
        op_names = [op.get_operation_name() for op in self.operations]
        return f"composite({' -> '.join(op_names)})"
