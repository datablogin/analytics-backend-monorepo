"""Data lineage tracking and impact analysis."""

from datetime import datetime
from typing import Any
from uuid import uuid4

import structlog
from pydantic import BaseModel, Field

logger = structlog.get_logger(__name__)


class DataAsset(BaseModel):
    """Data asset representation."""

    id: str = Field(default_factory=lambda: str(uuid4()))
    name: str
    type: str  # table, view, file, api, etc.
    schema_name: str | None = Field(None, alias="schema")
    database: str | None = None
    location: str | None = None
    metadata: dict[str, Any] = Field(default_factory=dict)
    created_at: datetime = Field(default_factory=datetime.utcnow)


class DataTransformation(BaseModel):
    """Data transformation operation."""

    id: str = Field(default_factory=lambda: str(uuid4()))
    name: str
    type: str  # etl, ml_training, aggregation, etc.
    description: str | None = None
    code: str | None = None
    parameters: dict[str, Any] = Field(default_factory=dict)
    metadata: dict[str, Any] = Field(default_factory=dict)
    created_at: datetime = Field(default_factory=datetime.utcnow)


class LineageEdge(BaseModel):
    """Lineage relationship between assets."""

    source_asset_id: str
    target_asset_id: str
    transformation_id: str | None = None
    relationship_type: str = "produces"  # produces, derived_from, etc.
    metadata: dict[str, Any] = Field(default_factory=dict)
    created_at: datetime = Field(default_factory=datetime.utcnow)


class LineageGraph(BaseModel):
    """Complete lineage graph."""

    assets: dict[str, DataAsset] = Field(default_factory=dict)
    transformations: dict[str, DataTransformation] = Field(default_factory=dict)
    edges: list[LineageEdge] = Field(default_factory=list)


class DataLineageTracker:
    """Data lineage tracking and analysis."""

    def __init__(self, storage_backend: str = "memory"):
        self.storage_backend = storage_backend
        self.graph = LineageGraph()
        self.logger = structlog.get_logger(__name__)

    def register_asset(
        self,
        name: str,
        asset_type: str,
        schema: str | None = None,
        database: str | None = None,
        location: str | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> str:
        """Register a data asset."""
        asset = DataAsset(
            name=name,
            type=asset_type,
            schema=schema,
            database=database,
            location=location,
            metadata=metadata or {},
        )

        self.graph.assets[asset.id] = asset

        self.logger.info(
            "Data asset registered", asset_id=asset.id, name=name, type=asset_type
        )

        return asset.id

    def register_transformation(
        self,
        name: str,
        transformation_type: str,
        description: str | None = None,
        code: str | None = None,
        parameters: dict[str, Any] | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> str:
        """Register a data transformation."""
        transformation = DataTransformation(
            name=name,
            type=transformation_type,
            description=description,
            code=code,
            parameters=parameters or {},
            metadata=metadata or {},
        )

        self.graph.transformations[transformation.id] = transformation

        self.logger.info(
            "Data transformation registered",
            transformation_id=transformation.id,
            name=name,
            type=transformation_type,
        )

        return transformation.id

    def add_lineage(
        self,
        source_asset_id: str,
        target_asset_id: str,
        transformation_id: str | None = None,
        relationship_type: str = "produces",
        metadata: dict[str, Any] | None = None,
    ) -> None:
        """Add lineage relationship between assets."""
        if source_asset_id not in self.graph.assets:
            raise ValueError(f"Source asset {source_asset_id} not found")
        if target_asset_id not in self.graph.assets:
            raise ValueError(f"Target asset {target_asset_id} not found")
        if transformation_id and transformation_id not in self.graph.transformations:
            raise ValueError(f"Transformation {transformation_id} not found")

        edge = LineageEdge(
            source_asset_id=source_asset_id,
            target_asset_id=target_asset_id,
            transformation_id=transformation_id,
            relationship_type=relationship_type,
            metadata=metadata or {},
        )

        self.graph.edges.append(edge)

        self.logger.info(
            "Lineage relationship added",
            source_asset_id=source_asset_id,
            target_asset_id=target_asset_id,
            transformation_id=transformation_id,
        )

    def get_upstream_assets(self, asset_id: str, max_depth: int = 10) -> list[str]:
        """Get all upstream assets for a given asset."""
        if asset_id not in self.graph.assets:
            raise ValueError(f"Asset {asset_id} not found")

        visited = set()
        upstream = []

        def traverse_upstream(current_id: str, depth: int):
            if depth >= max_depth or current_id in visited:
                return

            visited.add(current_id)

            for edge in self.graph.edges:
                if edge.target_asset_id == current_id:
                    upstream.append(edge.source_asset_id)
                    traverse_upstream(edge.source_asset_id, depth + 1)

        traverse_upstream(asset_id, 0)
        return list(set(upstream))

    def get_downstream_assets(self, asset_id: str, max_depth: int = 10) -> list[str]:
        """Get all downstream assets for a given asset."""
        if asset_id not in self.graph.assets:
            raise ValueError(f"Asset {asset_id} not found")

        visited = set()
        downstream = []

        def traverse_downstream(current_id: str, depth: int):
            if depth >= max_depth or current_id in visited:
                return

            visited.add(current_id)

            for edge in self.graph.edges:
                if edge.source_asset_id == current_id:
                    downstream.append(edge.target_asset_id)
                    traverse_downstream(edge.target_asset_id, depth + 1)

        traverse_downstream(asset_id, 0)
        return list(set(downstream))

    def get_lineage_path(self, source_id: str, target_id: str) -> list[str] | None:
        """Find lineage path between two assets."""
        if source_id not in self.graph.assets or target_id not in self.graph.assets:
            return None

        # BFS to find shortest path
        queue = [(source_id, [source_id])]
        visited = set()

        while queue:
            current_id, path = queue.pop(0)

            if current_id == target_id:
                return path

            if current_id in visited:
                continue

            visited.add(current_id)

            for edge in self.graph.edges:
                if edge.source_asset_id == current_id:
                    new_path = path + [edge.target_asset_id]
                    queue.append((edge.target_asset_id, new_path))

        return None

    def analyze_impact(self, asset_id: str) -> dict[str, Any]:
        """Analyze potential impact of changes to an asset."""
        if asset_id not in self.graph.assets:
            raise ValueError(f"Asset {asset_id} not found")

        asset = self.graph.assets[asset_id]
        downstream_assets = self.get_downstream_assets(asset_id)

        # Group downstream assets by type
        impact_by_type = {}
        for downstream_id in downstream_assets:
            downstream_asset = self.graph.assets[downstream_id]
            asset_type = downstream_asset.type
            if asset_type not in impact_by_type:
                impact_by_type[asset_type] = []
            impact_by_type[asset_type].append(
                {
                    "id": downstream_id,
                    "name": downstream_asset.name,
                    "location": downstream_asset.location,
                }
            )

        # Find transformations involved
        transformations_involved = set()
        for edge in self.graph.edges:
            if (
                edge.source_asset_id == asset_id
                or edge.target_asset_id in downstream_assets
            ) and edge.transformation_id:
                transformations_involved.add(edge.transformation_id)

        transformation_details = [
            {
                "id": trans_id,
                "name": self.graph.transformations[trans_id].name,
                "type": self.graph.transformations[trans_id].type,
            }
            for trans_id in transformations_involved
        ]

        return {
            "asset": {"id": asset_id, "name": asset.name, "type": asset.type},
            "total_downstream_assets": len(downstream_assets),
            "impact_by_type": impact_by_type,
            "transformations_affected": transformation_details,
            "risk_level": self._calculate_risk_level(
                len(downstream_assets), len(transformation_details)
            ),
        }

    def _calculate_risk_level(
        self, downstream_count: int, transformation_count: int
    ) -> str:
        """Calculate risk level based on impact scope."""
        score = downstream_count + transformation_count * 2

        if score >= 20:
            return "HIGH"
        elif score >= 10:
            return "MEDIUM"
        elif score >= 5:
            return "LOW"
        else:
            return "MINIMAL"

    def export_lineage_graph(self, format: str = "json") -> str:
        """Export lineage graph in specified format."""
        if format == "json":
            return self.graph.model_dump_json(indent=2)
        elif format == "dot":
            return self._export_to_dot()
        else:
            raise ValueError(f"Unsupported export format: {format}")

    def _export_to_dot(self) -> str:
        """Export lineage graph to DOT format for visualization."""
        lines = ["digraph lineage {"]
        lines.append("  rankdir=LR;")
        lines.append("  node [shape=rectangle];")

        # Add nodes
        for asset_id, asset in self.graph.assets.items():
            label = f"{asset.name}\\n({asset.type})"
            lines.append(f'  "{asset_id}" [label="{label}"];')

        # Add edges
        for edge in self.graph.edges:
            if edge.transformation_id:
                trans = self.graph.transformations[edge.transformation_id]
                lines.append(
                    f'  "{edge.source_asset_id}" -> "{edge.target_asset_id}" '
                    f'[label="{trans.name}"];'
                )
            else:
                lines.append(f'  "{edge.source_asset_id}" -> "{edge.target_asset_id}";')

        lines.append("}")
        return "\n".join(lines)

    def get_asset_by_name(self, name: str) -> DataAsset | None:
        """Find asset by name."""
        for asset in self.graph.assets.values():
            if asset.name == name:
                return asset
        return None

    def get_transformation_by_name(self, name: str) -> DataTransformation | None:
        """Find transformation by name."""
        for transformation in self.graph.transformations.values():
            if transformation.name == name:
                return transformation
        return None

    def validate_lineage_integrity(self) -> list[str]:
        """Validate lineage graph integrity."""
        issues = []

        # Check for orphaned edges
        for edge in self.graph.edges:
            if edge.source_asset_id not in self.graph.assets:
                issues.append(
                    f"Edge references non-existent source asset: {edge.source_asset_id}"
                )
            if edge.target_asset_id not in self.graph.assets:
                issues.append(
                    f"Edge references non-existent target asset: {edge.target_asset_id}"
                )
            if (
                edge.transformation_id
                and edge.transformation_id not in self.graph.transformations
            ):
                issues.append(
                    f"Edge references non-existent transformation: {edge.transformation_id}"
                )

        # Check for circular dependencies
        for asset_id in self.graph.assets:
            if self._has_circular_dependency(asset_id):
                issues.append(
                    f"Circular dependency detected starting from asset: {asset_id}"
                )

        return issues

    def _has_circular_dependency(
        self, start_asset_id: str, visited: set[str] | None = None
    ) -> bool:
        """Check for circular dependencies starting from an asset."""
        if visited is None:
            visited = set()

        if start_asset_id in visited:
            return True

        visited.add(start_asset_id)

        for edge in self.graph.edges:
            if edge.source_asset_id == start_asset_id:
                if self._has_circular_dependency(edge.target_asset_id, visited.copy()):
                    return True

        return False
