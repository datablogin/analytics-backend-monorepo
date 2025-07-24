"""Feature lineage tracking and impact analysis."""
# mypy: disable-error-code=attr-defined,arg-type,call-overload,var-annotated

import json
from datetime import datetime
from enum import Enum
from typing import Any

import structlog
from libs.analytics_core.models import BaseModel as SQLBaseModel
from pydantic import BaseModel, Field
from sqlalchemy import (
    Boolean,
    Column,
    Float,
    ForeignKey,
    Integer,
    String,
    Text,
    select,
)

from .core import FeatureStoreService

logger = structlog.get_logger(__name__)


class LineageNodeType(str, Enum):
    """Types of nodes in the lineage graph."""

    DATA_SOURCE = "data_source"
    FEATURE = "feature"
    TRANSFORMATION = "transformation"
    MODEL = "model"
    DATASET = "dataset"
    PIPELINE = "pipeline"


class LineageRelationType(str, Enum):
    """Types of relationships in the lineage graph."""

    DERIVED_FROM = "derived_from"
    TRANSFORMS = "transforms"
    CONSUMES = "consumes"
    PRODUCES = "produces"
    DEPENDS_ON = "depends_on"


# Database Models
class LineageNode(SQLBaseModel):
    """Lineage graph node."""

    __tablename__ = "lineage_nodes"

    name: str = Column(String(255), nullable=False, unique=True, index=True)
    node_type: str = Column(String(50), nullable=False, index=True)
    node_metadata: str = Column(
        "metadata", Text, nullable=True
    )  # JSON string, aliased to avoid SQLAlchemy conflict
    description: str = Column(Text, nullable=True)
    owner: str = Column(String(255), nullable=True)
    tags: str = Column(Text, nullable=True)  # JSON string
    is_active: bool = Column(Boolean, default=True)


class LineageEdge(SQLBaseModel):
    """Lineage graph edge representing relationships."""

    __tablename__ = "lineage_edges"

    source_node_id: int = Column(
        Integer, ForeignKey("lineage_nodes.id"), nullable=False, index=True
    )
    target_node_id: int = Column(
        Integer, ForeignKey("lineage_nodes.id"), nullable=False, index=True
    )
    relation_type: str = Column(String(50), nullable=False, index=True)
    edge_metadata: str = Column(
        "metadata", Text, nullable=True
    )  # JSON string, aliased to avoid SQLAlchemy conflict
    strength: float = Column(Float, default=1.0)


# Pydantic Models
class LineageNodeModel(BaseModel):
    """Lineage node model."""

    id: int = Field(..., description="Node ID")
    name: str = Field(..., description="Node name")
    node_type: LineageNodeType = Field(..., description="Node type")
    metadata: dict[str, Any] = Field(default_factory=dict, description="Node metadata")
    description: str | None = Field(None, description="Node description")
    owner: str | None = Field(None, description="Node owner")
    tags: list[str] = Field(default_factory=list, description="Node tags")
    is_active: bool = Field(..., description="Whether node is active")
    created_at: datetime = Field(..., description="Creation timestamp")
    updated_at: datetime = Field(..., description="Last update timestamp")


class LineageEdgeModel(BaseModel):
    """Lineage edge model."""

    id: int = Field(..., description="Edge ID")
    source_node_id: int = Field(..., description="Source node ID")
    target_node_id: int = Field(..., description="Target node ID")
    relation_type: LineageRelationType = Field(..., description="Relation type")
    metadata: dict[str, Any] = Field(default_factory=dict, description="Edge metadata")
    strength: float = Field(..., description="Relationship strength")
    created_at: datetime = Field(..., description="Creation timestamp")


class LineageGraph(BaseModel):
    """Complete lineage graph."""

    nodes: list[LineageNodeModel] = Field(..., description="Graph nodes")
    edges: list[LineageEdgeModel] = Field(..., description="Graph edges")
    root_node: str = Field(..., description="Root node name")
    depth: int = Field(..., description="Graph depth")


class ImpactAnalysisResult(BaseModel):
    """Impact analysis result."""

    target_node: str = Field(..., description="Target node name")
    upstream_nodes: list[LineageNodeModel] = Field(
        ..., description="Upstream dependencies"
    )
    downstream_nodes: list[LineageNodeModel] = Field(
        ..., description="Downstream consumers"
    )
    critical_path: list[LineageNodeModel] = Field(
        ..., description="Critical path nodes"
    )
    impact_score: float = Field(..., description="Impact score (0-1)")
    analysis_timestamp: datetime = Field(
        default_factory=datetime.utcnow, description="Analysis timestamp"
    )


class LineageTracker:
    """Feature lineage tracking service."""

    def __init__(self, feature_store_service: FeatureStoreService):
        """Initialize lineage tracker."""
        self.feature_store_service = feature_store_service

    async def create_node(
        self,
        name: str,
        node_type: LineageNodeType,
        metadata: dict[str, Any] | None = None,
        description: str | None = None,
        owner: str | None = None,
        tags: list[str] | None = None,
    ) -> int:
        """Create a lineage node."""
        try:
            async with self.feature_store_service.db_manager.get_session() as session:  # type: ignore
                # Check if node already exists
                stmt = select(LineageNode).where(LineageNode.name == name)  # type: ignore
                result = await session.execute(stmt)
                existing_node = result.scalar_one_or_none()

                if existing_node:
                    logger.warning(
                        "Lineage node already exists",
                        node_name=name,
                        existing_id=existing_node.id,
                    )
                    return existing_node.id

                # Create new node
                node = LineageNode(
                    name=name,
                    node_type=node_type.value,
                    node_metadata=json.dumps(metadata or {}, default=str),
                    description=description,
                    owner=owner,
                    tags=json.dumps(tags or []),
                    is_active=True,
                )

                session.add(node)
                await session.commit()
                await session.refresh(node)

                logger.info(
                    "Lineage node created",
                    node_name=name,
                    node_type=node_type,
                    node_id=node.id,
                )

                return node.id

        except Exception as error:
            logger.error(
                "Failed to create lineage node",
                node_name=name,
                node_type=node_type,
                error=str(error),
            )
            raise

    async def create_relationship(
        self,
        source_node_name: str,
        target_node_name: str,
        relation_type: LineageRelationType,
        metadata: dict[str, Any] | None = None,
        strength: float = 1.0,
    ) -> int:
        """Create a relationship between two nodes."""
        try:
            async with self.feature_store_service.db_manager.get_session() as session:  # type: ignore
                # Get source and target nodes
                source_stmt = select(LineageNode).where(
                    LineageNode.name == source_node_name
                )
                source_result = await session.execute(source_stmt)
                source_node = source_result.scalar_one_or_none()

                target_stmt = select(LineageNode).where(
                    LineageNode.name == target_node_name
                )
                target_result = await session.execute(target_stmt)
                target_node = target_result.scalar_one_or_none()

                if not source_node:
                    raise ValueError(f"Source node '{source_node_name}' not found")
                if not target_node:
                    raise ValueError(f"Target node '{target_node_name}' not found")

                # Check if relationship already exists
                edge_stmt = select(LineageEdge).where(
                    LineageEdge.source_node_id == source_node.id,
                    LineageEdge.target_node_id == target_node.id,
                    LineageEdge.relation_type == relation_type.value,
                )
                edge_result = await session.execute(edge_stmt)
                existing_edge = edge_result.scalar_one_or_none()

                if existing_edge:
                    logger.warning(
                        "Lineage relationship already exists",
                        source_node=source_node_name,
                        target_node=target_node_name,
                        relation_type=relation_type,
                        existing_id=existing_edge.id,
                    )
                    return existing_edge.id

                # Create new relationship
                edge = LineageEdge(
                    source_node_id=source_node.id,
                    target_node_id=target_node.id,
                    relation_type=relation_type.value,
                    edge_metadata=json.dumps(metadata or {}, default=str),
                    strength=str(strength),
                )

                session.add(edge)
                await session.commit()
                await session.refresh(edge)

                logger.info(
                    "Lineage relationship created",
                    source_node=source_node_name,
                    target_node=target_node_name,
                    relation_type=relation_type,
                    edge_id=edge.id,
                )

                return edge.id

        except Exception as error:
            logger.error(
                "Failed to create lineage relationship",
                source_node=source_node_name,
                target_node=target_node_name,
                relation_type=relation_type,
                error=str(error),
            )
            raise

    async def track_feature_creation(
        self,
        feature_name: str,
        source_data: str | None = None,
        transformation_logic: str | None = None,
        pipeline_name: str | None = None,
    ) -> None:
        """Track feature creation lineage."""
        try:
            # Create feature node
            await self.create_node(
                name=feature_name,
                node_type=LineageNodeType.FEATURE,
                metadata={
                    "transformation_logic": transformation_logic,
                    "pipeline": pipeline_name,
                },
                description=f"Feature: {feature_name}",
            )

            # Create relationships
            if source_data:
                # Create or get source data node
                await self.create_node(
                    name=source_data,
                    node_type=LineageNodeType.DATA_SOURCE,
                    description=f"Data source: {source_data}",
                )

                # Create derived_from relationship
                await self.create_relationship(
                    source_node_name=source_data,
                    target_node_name=feature_name,
                    relation_type=LineageRelationType.DERIVED_FROM,
                )

            if pipeline_name:
                # Create or get pipeline node
                await self.create_node(
                    name=pipeline_name,
                    node_type=LineageNodeType.PIPELINE,
                    description=f"Pipeline: {pipeline_name}",
                )

                # Create produces relationship
                await self.create_relationship(
                    source_node_name=pipeline_name,
                    target_node_name=feature_name,
                    relation_type=LineageRelationType.PRODUCES,
                )

            logger.info(
                "Feature creation lineage tracked",
                feature_name=feature_name,
                source_data=source_data,
                pipeline_name=pipeline_name,
            )

        except Exception as error:
            logger.error(
                "Failed to track feature creation lineage",
                feature_name=feature_name,
                error=str(error),
            )
            raise

    async def track_model_consumption(
        self,
        model_name: str,
        feature_names: list[str],
        model_metadata: dict[str, Any] | None = None,
    ) -> None:
        """Track model consuming features."""
        try:
            # Create model node
            await self.create_node(
                name=model_name,
                node_type=LineageNodeType.MODEL,
                metadata=model_metadata or {},
                description=f"ML Model: {model_name}",
            )

            # Create consumes relationships
            for feature_name in feature_names:
                await self.create_relationship(
                    source_node_name=feature_name,
                    target_node_name=model_name,
                    relation_type=LineageRelationType.CONSUMES,
                )

            logger.info(
                "Model consumption lineage tracked",
                model_name=model_name,
                feature_count=len(feature_names),
            )

        except Exception as error:
            logger.error(
                "Failed to track model consumption lineage",
                model_name=model_name,
                error=str(error),
            )
            raise

    async def get_lineage_graph(
        self,
        root_node_name: str,
        depth: int = 3,
        direction: str = "both",  # "upstream", "downstream", or "both"
    ) -> LineageGraph:
        """Get lineage graph for a node."""
        try:
            async with self.feature_store_service.db_manager.get_session() as session:  # type: ignore
                # Get root node
                root_stmt = select(LineageNode).where(
                    LineageNode.name == root_node_name
                )
                root_result = await session.execute(root_stmt)
                root_node = root_result.scalar_one_or_none()

                if not root_node:
                    raise ValueError(f"Root node '{root_node_name}' not found")

                # Collect nodes and edges using recursive traversal
                visited_nodes = set()
                visited_edges = set()
                nodes_to_process = [(root_node.id, 0)]

                all_nodes = {}
                all_edges = []

                while nodes_to_process:
                    node_id, current_depth = nodes_to_process.pop(0)

                    if node_id in visited_nodes or current_depth >= depth:
                        continue

                    visited_nodes.add(node_id)

                    # Get node details
                    node_stmt = select(LineageNode).where(LineageNode.id == node_id)
                    node_result = await session.execute(node_stmt)
                    node = node_result.scalar_one_or_none()

                    if node:
                        all_nodes[node_id] = self._convert_node_to_model(node)

                    # Get connected edges based on direction
                    edge_conditions = []
                    if direction in ["upstream", "both"]:
                        edge_conditions.append(LineageEdge.target_node_id == node_id)
                    if direction in ["downstream", "both"]:
                        edge_conditions.append(LineageEdge.source_node_id == node_id)

                    if edge_conditions:
                        from sqlalchemy import or_

                        edge_stmt = select(LineageEdge).where(or_(*edge_conditions))
                        edge_result = await session.execute(edge_stmt)
                        edges = edge_result.scalars().all()

                        for edge in edges:
                            edge_key = (
                                edge.source_node_id,
                                edge.target_node_id,
                                edge.relation_type,
                            )
                            if edge_key not in visited_edges:
                                visited_edges.add(edge_key)
                                all_edges.append(self._convert_edge_to_model(edge))

                                # Add connected nodes to processing queue
                                if (
                                    direction in ["upstream", "both"]
                                    and edge.target_node_id == node_id
                                ):
                                    nodes_to_process.append(
                                        (edge.source_node_id, current_depth + 1)
                                    )
                                if (
                                    direction in ["downstream", "both"]
                                    and edge.source_node_id == node_id
                                ):
                                    nodes_to_process.append(
                                        (edge.target_node_id, current_depth + 1)
                                    )

                return LineageGraph(
                    nodes=list(all_nodes.values()),
                    edges=all_edges,
                    root_node=root_node_name,
                    depth=depth,
                )

        except Exception as error:
            logger.error(
                "Failed to get lineage graph",
                root_node=root_node_name,
                error=str(error),
            )
            raise

    async def analyze_impact(
        self,
        target_node_name: str,
        change_type: str = "modification",
    ) -> ImpactAnalysisResult:
        """Analyze impact of changes to a node."""
        try:
            # Get upstream and downstream lineage
            upstream_graph = await self.get_lineage_graph(
                target_node_name, depth=5, direction="upstream"
            )
            downstream_graph = await self.get_lineage_graph(
                target_node_name, depth=5, direction="downstream"
            )

            # Calculate impact score based on number of downstream consumers
            downstream_features = [
                node
                for node in downstream_graph.nodes
                if node.node_type == LineageNodeType.FEATURE
            ]
            downstream_models = [
                node
                for node in downstream_graph.nodes
                if node.node_type == LineageNodeType.MODEL
            ]

            # Simple impact scoring
            impact_score = min(
                (len(downstream_features) * 0.1 + len(downstream_models) * 0.3), 1.0
            )

            # Find critical path (nodes with highest connectivity)
            critical_path = self._find_critical_path(downstream_graph)

            logger.info(
                "Impact analysis completed",
                target_node=target_node_name,
                upstream_count=len(upstream_graph.nodes),
                downstream_count=len(downstream_graph.nodes),
                impact_score=impact_score,
            )

            return ImpactAnalysisResult(
                target_node=target_node_name,
                upstream_nodes=upstream_graph.nodes,
                downstream_nodes=downstream_graph.nodes,
                critical_path=critical_path,
                impact_score=impact_score,
            )

        except Exception as error:
            logger.error(
                "Failed to analyze impact",
                target_node=target_node_name,
                error=str(error),
            )
            raise

    def _find_critical_path(self, graph: LineageGraph) -> list[LineageNodeModel]:
        """Find critical path in the lineage graph."""
        # Simple implementation: nodes with most connections
        node_connections = {}

        for edge in graph.edges:
            node_connections[edge.source_node_id] = (
                node_connections.get(edge.source_node_id, 0) + 1
            )
            node_connections[edge.target_node_id] = (
                node_connections.get(edge.target_node_id, 0) + 1
            )

        # Sort nodes by connection count
        sorted_nodes = sorted(
            graph.nodes, key=lambda n: node_connections.get(n.id, 0), reverse=True
        )

        # Return top 5 most connected nodes as critical path
        return sorted_nodes[:5]

    def _convert_node_to_model(self, node: LineageNode) -> LineageNodeModel:
        """Convert database node to model."""
        return LineageNodeModel(
            id=node.id,
            name=node.name,
            node_type=LineageNodeType(node.node_type),
            metadata=json.loads(node.node_metadata or "{}"),
            description=node.description,
            owner=node.owner,
            tags=json.loads(node.tags or "[]"),
            is_active=node.is_active,
            created_at=node.created_at,
            updated_at=node.updated_at,
        )

    def _convert_edge_to_model(self, edge: LineageEdge) -> LineageEdgeModel:
        """Convert database edge to model."""
        return LineageEdgeModel(
            id=edge.id,
            source_node_id=edge.source_node_id,
            target_node_id=edge.target_node_id,
            relation_type=LineageRelationType(edge.relation_type),
            metadata=json.loads(edge.edge_metadata or "{}"),
            strength=float(edge.strength) if edge.strength else 1.0,
            created_at=edge.created_at,
        )

    async def get_feature_dependencies(self, feature_name: str) -> list[str]:
        """Get direct dependencies for a feature."""
        try:
            graph = await self.get_lineage_graph(
                feature_name, depth=1, direction="upstream"
            )

            # Filter for direct data sources and other features
            dependencies = [
                node.name
                for node in graph.nodes
                if node.name != feature_name
                and node.node_type
                in [
                    LineageNodeType.DATA_SOURCE,
                    LineageNodeType.FEATURE,
                ]
            ]

            return dependencies

        except Exception as error:
            logger.error(
                "Failed to get feature dependencies",
                feature_name=feature_name,
                error=str(error),
            )
            return []

    async def get_feature_consumers(self, feature_name: str) -> list[str]:
        """Get direct consumers of a feature."""
        try:
            graph = await self.get_lineage_graph(
                feature_name, depth=1, direction="downstream"
            )

            # Filter for models and other features that consume this feature
            consumers = [
                node.name
                for node in graph.nodes
                if node.name != feature_name
                and node.node_type
                in [
                    LineageNodeType.MODEL,
                    LineageNodeType.FEATURE,
                ]
            ]

            return consumers

        except Exception as error:
            logger.error(
                "Failed to get feature consumers",
                feature_name=feature_name,
                error=str(error),
            )
            return []

    async def cleanup_orphaned_nodes(self) -> int:
        """Clean up nodes with no connections."""
        try:
            async with self.feature_store_service.db_manager.get_session() as session:  # type: ignore
                # Find nodes with no incoming or outgoing edges
                orphaned_stmt = select(LineageNode).where(
                    ~LineageNode.id.in_(select(LineageEdge.source_node_id))
                    & ~LineageNode.id.in_(select(LineageEdge.target_node_id))
                )

                result = await session.execute(orphaned_stmt)
                orphaned_nodes = result.scalars().all()

                count = 0
                for node in orphaned_nodes:
                    await session.delete(node)
                    count += 1

                await session.commit()

                logger.info(
                    "Cleaned up orphaned lineage nodes",
                    count=count,
                )

                return count

        except Exception as error:
            logger.error(
                "Failed to cleanup orphaned nodes",
                error=str(error),
            )
            return 0
