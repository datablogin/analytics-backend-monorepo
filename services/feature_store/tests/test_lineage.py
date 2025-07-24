"""Tests for feature lineage tracking."""

from datetime import datetime
from unittest.mock import AsyncMock, Mock

import pytest
from services.feature_store.lineage import (
    ImpactAnalysisResult,
    LineageGraph,
    LineageNodeModel,
    LineageNodeType,
    LineageRelationType,
    LineageTracker,
)


@pytest.fixture
def mock_feature_store_service():
    """Mock feature store service."""
    service = Mock()
    service.db_manager = Mock()
    service.db_manager.get_session = AsyncMock()
    return service


@pytest.fixture
def lineage_tracker(mock_feature_store_service):
    """Lineage tracker with mocked dependencies."""
    return LineageTracker(mock_feature_store_service)


@pytest.fixture
def sample_node_model():
    """Sample lineage node model."""
    return LineageNodeModel(
        id=1,
        name="user_age",
        node_type=LineageNodeType.FEATURE,
        metadata={"transformation": "direct"},
        description="User age feature",
        owner="data_team",
        tags=["demographic"],
        is_active=True,
        created_at=datetime.utcnow(),
        updated_at=datetime.utcnow(),
    )


class TestLineageTracker:
    """Tests for LineageTracker."""

    @pytest.mark.asyncio
    async def test_create_node_new(self, lineage_tracker, mock_feature_store_service):
        """Test creating a new lineage node."""
        # Mock session
        mock_session = AsyncMock()
        mock_feature_store_service.db_manager.get_session.return_value.__aenter__.return_value = mock_session

        # Mock query result - no existing node
        mock_result = Mock()
        mock_result.scalar_one_or_none.return_value = None
        mock_session.execute.return_value = mock_result

        # Mock created node
        mock_node = Mock()
        mock_node.id = 1
        mock_session.refresh = AsyncMock()

        # Execute
        await lineage_tracker.create_node(
            name="user_age",
            node_type=LineageNodeType.FEATURE,
            description="User age feature",
            owner="data_team",
        )

        # Verify
        mock_session.add.assert_called_once()
        mock_session.commit.assert_called_once()
        mock_session.refresh.assert_called_once()

    @pytest.mark.asyncio
    async def test_create_node_existing(
        self, lineage_tracker, mock_feature_store_service
    ):
        """Test creating a node that already exists."""
        # Mock session
        mock_session = AsyncMock()
        mock_feature_store_service.db_manager.get_session.return_value.__aenter__.return_value = mock_session

        # Mock query result - existing node
        mock_existing_node = Mock()
        mock_existing_node.id = 1
        mock_result = Mock()
        mock_result.scalar_one_or_none.return_value = mock_existing_node
        mock_session.execute.return_value = mock_result

        # Execute
        node_id = await lineage_tracker.create_node(
            name="user_age",
            node_type=LineageNodeType.FEATURE,
        )

        # Verify returns existing ID
        assert node_id == 1
        # Should not add new node
        mock_session.add.assert_not_called()

    @pytest.mark.asyncio
    async def test_create_relationship(
        self, lineage_tracker, mock_feature_store_service
    ):
        """Test creating a relationship between nodes."""
        # Mock session
        mock_session = AsyncMock()
        mock_feature_store_service.db_manager.get_session.return_value.__aenter__.return_value = mock_session

        # Mock source and target nodes
        mock_source_node = Mock()
        mock_source_node.id = 1
        mock_target_node = Mock()
        mock_target_node.id = 2

        # Mock query results
        mock_results = [
            mock_source_node,
            mock_target_node,
            None,
        ]  # source, target, no existing edge
        mock_session.execute.side_effect = [
            Mock(scalar_one_or_none=Mock(return_value=mock_results[0])),
            Mock(scalar_one_or_none=Mock(return_value=mock_results[1])),
            Mock(scalar_one_or_none=Mock(return_value=mock_results[2])),
        ]

        # Mock created edge
        mock_edge = Mock()
        mock_edge.id = 1
        mock_session.refresh = AsyncMock()

        # Execute
        await lineage_tracker.create_relationship(
            source_node_name="data_source",
            target_node_name="user_age",
            relation_type=LineageRelationType.DERIVED_FROM,
        )

        # Verify
        mock_session.add.assert_called_once()
        mock_session.commit.assert_called_once()
        mock_session.refresh.assert_called_once()

    @pytest.mark.asyncio
    async def test_create_relationship_missing_nodes(
        self, lineage_tracker, mock_feature_store_service
    ):
        """Test creating relationship with missing nodes."""
        # Mock session
        mock_session = AsyncMock()
        mock_feature_store_service.db_manager.get_session.return_value.__aenter__.return_value = mock_session

        # Mock query results - source node not found
        mock_session.execute.side_effect = [
            Mock(scalar_one_or_none=Mock(return_value=None))
        ]

        # Execute and verify exception
        with pytest.raises(ValueError, match="Source node 'missing_source' not found"):
            await lineage_tracker.create_relationship(
                source_node_name="missing_source",
                target_node_name="user_age",
                relation_type=LineageRelationType.DERIVED_FROM,
            )

    @pytest.mark.asyncio
    async def test_track_feature_creation(self, lineage_tracker):
        """Test tracking feature creation lineage."""
        with pytest.mock.patch.object(
            lineage_tracker, "create_node"
        ) as mock_create_node:
            with pytest.mock.patch.object(
                lineage_tracker, "create_relationship"
            ) as mock_create_rel:
                mock_create_node.return_value = 1
                mock_create_rel.return_value = 1

                # Execute
                await lineage_tracker.track_feature_creation(
                    feature_name="user_age",
                    source_data="users_table",
                    transformation_logic="SELECT age FROM users",
                    pipeline_name="user_features_pipeline",
                )

                # Verify feature node creation
                mock_create_node.assert_any_call(
                    name="user_age",
                    node_type=LineageNodeType.FEATURE,
                    metadata={
                        "transformation_logic": "SELECT age FROM users",
                        "pipeline": "user_features_pipeline",
                    },
                    description="Feature: user_age",
                )

                # Verify source data node creation
                mock_create_node.assert_any_call(
                    name="users_table",
                    node_type=LineageNodeType.DATA_SOURCE,
                    description="Data source: users_table",
                )

                # Verify pipeline node creation
                mock_create_node.assert_any_call(
                    name="user_features_pipeline",
                    node_type=LineageNodeType.PIPELINE,
                    description="Pipeline: user_features_pipeline",
                )

                # Verify relationships
                mock_create_rel.assert_any_call(
                    source_node_name="users_table",
                    target_node_name="user_age",
                    relation_type=LineageRelationType.DERIVED_FROM,
                )

                mock_create_rel.assert_any_call(
                    source_node_name="user_features_pipeline",
                    target_node_name="user_age",
                    relation_type=LineageRelationType.PRODUCES,
                )

    @pytest.mark.asyncio
    async def test_track_model_consumption(self, lineage_tracker):
        """Test tracking model consumption lineage."""
        with pytest.mock.patch.object(
            lineage_tracker, "create_node"
        ) as mock_create_node:
            with pytest.mock.patch.object(
                lineage_tracker, "create_relationship"
            ) as mock_create_rel:
                mock_create_node.return_value = 1
                mock_create_rel.return_value = 1

                # Execute
                await lineage_tracker.track_model_consumption(
                    model_name="user_churn_model",
                    feature_names=["user_age", "user_activity"],
                    model_metadata={"version": "1.0", "type": "classification"},
                )

                # Verify model node creation
                mock_create_node.assert_called_with(
                    name="user_churn_model",
                    node_type=LineageNodeType.MODEL,
                    metadata={"version": "1.0", "type": "classification"},
                    description="ML Model: user_churn_model",
                )

                # Verify relationships for each feature
                assert mock_create_rel.call_count == 2
                mock_create_rel.assert_any_call(
                    source_node_name="user_age",
                    target_node_name="user_churn_model",
                    relation_type=LineageRelationType.CONSUMES,
                )
                mock_create_rel.assert_any_call(
                    source_node_name="user_activity",
                    target_node_name="user_churn_model",
                    relation_type=LineageRelationType.CONSUMES,
                )

    @pytest.mark.asyncio
    async def test_get_lineage_graph(
        self, lineage_tracker, mock_feature_store_service, sample_node_model
    ):
        """Test getting lineage graph."""
        # Mock session
        mock_session = AsyncMock()
        mock_feature_store_service.db_manager.get_session.return_value.__aenter__.return_value = mock_session

        # Mock root node lookup
        mock_root_node = Mock()
        mock_root_node.id = 1
        mock_root_node.name = "user_age"

        # Mock the lineage node conversion
        with pytest.mock.patch.object(
            lineage_tracker, "_convert_node_to_model"
        ) as mock_convert_node:
            with pytest.mock.patch.object(
                lineage_tracker, "_convert_edge_to_model"
            ) as mock_convert_edge:
                mock_convert_node.return_value = sample_node_model
                mock_convert_edge.return_value = Mock()

                # Mock query results
                mock_session.execute.side_effect = [
                    Mock(
                        scalar_one_or_none=Mock(return_value=mock_root_node)
                    ),  # Root node lookup
                    Mock(
                        scalar_one_or_none=Mock(return_value=mock_root_node)
                    ),  # Node details
                    Mock(
                        scalars=Mock(return_value=Mock(all=Mock(return_value=[])))
                    ),  # No edges
                ]

                # Execute
                graph = await lineage_tracker.get_lineage_graph("user_age", depth=2)

                # Verify
                assert isinstance(graph, LineageGraph)
                assert graph.root_node == "user_age"
                assert graph.depth == 2
                assert len(graph.nodes) >= 1

    @pytest.mark.asyncio
    async def test_get_lineage_graph_not_found(
        self, lineage_tracker, mock_feature_store_service
    ):
        """Test getting lineage graph for non-existent node."""
        # Mock session
        mock_session = AsyncMock()
        mock_feature_store_service.db_manager.get_session.return_value.__aenter__.return_value = mock_session

        # Mock query result - no root node found
        mock_session.execute.return_value = Mock(
            scalar_one_or_none=Mock(return_value=None)
        )

        # Execute and verify exception
        with pytest.raises(ValueError, match="Root node 'non_existent' not found"):
            await lineage_tracker.get_lineage_graph("non_existent")

    @pytest.mark.asyncio
    async def test_analyze_impact(self, lineage_tracker, sample_node_model):
        """Test impact analysis."""
        # Mock lineage graphs
        mock_upstream_graph = LineageGraph(
            nodes=[sample_node_model], edges=[], root_node="user_age", depth=5
        )

        mock_downstream_graph = LineageGraph(
            nodes=[
                sample_node_model,
                LineageNodeModel(
                    id=2,
                    name="churn_model",
                    node_type=LineageNodeType.MODEL,
                    metadata={},
                    description="Churn prediction model",
                    owner="ml_team",
                    tags=[],
                    is_active=True,
                    created_at=datetime.utcnow(),
                    updated_at=datetime.utcnow(),
                ),
            ],
            edges=[],
            root_node="user_age",
            depth=5,
        )

        with pytest.mock.patch.object(
            lineage_tracker, "get_lineage_graph"
        ) as mock_get_graph:
            mock_get_graph.side_effect = [mock_upstream_graph, mock_downstream_graph]

            # Execute
            impact_result = await lineage_tracker.analyze_impact("user_age")

            # Verify
            assert isinstance(impact_result, ImpactAnalysisResult)
            assert impact_result.target_node == "user_age"
            assert len(impact_result.upstream_nodes) == 1
            assert len(impact_result.downstream_nodes) == 2
            assert 0.0 <= impact_result.impact_score <= 1.0
            assert len(impact_result.critical_path) <= 5

    @pytest.mark.asyncio
    async def test_get_feature_dependencies(self, lineage_tracker, sample_node_model):
        """Test getting feature dependencies."""
        # Mock upstream graph with dependencies
        upstream_graph = LineageGraph(
            nodes=[
                sample_node_model,
                LineageNodeModel(
                    id=2,
                    name="users_table",
                    node_type=LineageNodeType.DATA_SOURCE,
                    metadata={},
                    description="Users data source",
                    owner="data_team",
                    tags=[],
                    is_active=True,
                    created_at=datetime.utcnow(),
                    updated_at=datetime.utcnow(),
                ),
            ],
            edges=[],
            root_node="user_age",
            depth=1,
        )

        with pytest.mock.patch.object(
            lineage_tracker, "get_lineage_graph"
        ) as mock_get_graph:
            mock_get_graph.return_value = upstream_graph

            # Execute
            dependencies = await lineage_tracker.get_feature_dependencies("user_age")

            # Verify
            assert "users_table" in dependencies
            assert "user_age" not in dependencies  # Should not include itself

    @pytest.mark.asyncio
    async def test_get_feature_consumers(self, lineage_tracker, sample_node_model):
        """Test getting feature consumers."""
        # Mock downstream graph with consumers
        downstream_graph = LineageGraph(
            nodes=[
                sample_node_model,
                LineageNodeModel(
                    id=2,
                    name="churn_model",
                    node_type=LineageNodeType.MODEL,
                    metadata={},
                    description="Churn model",
                    owner="ml_team",
                    tags=[],
                    is_active=True,
                    created_at=datetime.utcnow(),
                    updated_at=datetime.utcnow(),
                ),
            ],
            edges=[],
            root_node="user_age",
            depth=1,
        )

        with pytest.mock.patch.object(
            lineage_tracker, "get_lineage_graph"
        ) as mock_get_graph:
            mock_get_graph.return_value = downstream_graph

            # Execute
            consumers = await lineage_tracker.get_feature_consumers("user_age")

            # Verify
            assert "churn_model" in consumers
            assert "user_age" not in consumers  # Should not include itself

    def test_find_critical_path(self, lineage_tracker, sample_node_model):
        """Test finding critical path in lineage graph."""
        # Create graph with multiple nodes
        nodes = [sample_node_model]
        for i in range(2, 8):
            nodes.append(
                LineageNodeModel(
                    id=i,
                    name=f"node_{i}",
                    node_type=LineageNodeType.FEATURE,
                    metadata={},
                    description=f"Node {i}",
                    owner="team",
                    tags=[],
                    is_active=True,
                    created_at=datetime.utcnow(),
                    updated_at=datetime.utcnow(),
                )
            )

        # Create edges (node connections)
        from services.feature_store.lineage import LineageEdgeModel

        edges = [
            LineageEdgeModel(
                id=1,
                source_node_id=1,
                target_node_id=2,
                relation_type=LineageRelationType.DERIVED_FROM,
                metadata={},
                strength=1.0,
                created_at=datetime.utcnow(),
            )
        ]

        graph = LineageGraph(nodes=nodes, edges=edges, root_node="user_age", depth=3)

        # Execute
        critical_path = lineage_tracker._find_critical_path(graph)

        # Verify
        assert len(critical_path) <= 5
        assert all(isinstance(node, LineageNodeModel) for node in critical_path)

    @pytest.mark.asyncio
    async def test_cleanup_orphaned_nodes(
        self, lineage_tracker, mock_feature_store_service
    ):
        """Test cleaning up orphaned nodes."""
        # Mock session
        mock_session = AsyncMock()
        mock_feature_store_service.db_manager.get_session.return_value.__aenter__.return_value = mock_session

        # Mock orphaned nodes
        mock_orphaned_node = Mock()
        mock_orphaned_node.id = 99
        mock_orphaned_node.name = "orphaned_node"

        mock_result = Mock()
        mock_result.scalars.return_value.all.return_value = [mock_orphaned_node]
        mock_session.execute.return_value = mock_result

        # Execute
        count = await lineage_tracker.cleanup_orphaned_nodes()

        # Verify
        assert count == 1
        mock_session.delete.assert_called_once_with(mock_orphaned_node)
        mock_session.commit.assert_called_once()
