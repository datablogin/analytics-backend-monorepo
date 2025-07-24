"""Add pipeline, monitoring, and lineage tables.

Revision ID: 002_pipeline_monitoring_lineage
Revises: 001_feature_store
Create Date: 2024-01-02 00:00:00.000000

"""

import sqlalchemy as sa
from alembic import op
from sqlalchemy import text

# revision identifiers
revision = "002_pipeline_monitoring_lineage"
down_revision = "001_feature_store"
branch_labels = None
depends_on = None


def upgrade() -> None:
    """Add pipeline, monitoring, and lineage tables."""

    # Create feature_pipeline_runs table
    op.create_table(
        "feature_pipeline_runs",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column("pipeline_name", sa.String(length=255), nullable=False),
        sa.Column("pipeline_type", sa.String(length=50), nullable=False),
        sa.Column("status", sa.String(length=50), nullable=False),
        sa.Column("config", sa.Text(), nullable=True),
        sa.Column("start_time", sa.DateTime(timezone=True), nullable=True),
        sa.Column("end_time", sa.DateTime(timezone=True), nullable=True),
        sa.Column("features_processed", sa.Integer(), nullable=False, default=0),
        sa.Column("records_processed", sa.Integer(), nullable=False, default=0),
        sa.Column("error_message", sa.Text(), nullable=True),
        sa.Column("metrics", sa.Text(), nullable=True),
        sa.PrimaryKeyConstraint("id"),
    )

    # Create indexes for feature_pipeline_runs
    op.create_index("ix_feature_pipeline_runs_id", "feature_pipeline_runs", ["id"])
    op.create_index(
        "ix_feature_pipeline_runs_pipeline_name",
        "feature_pipeline_runs",
        ["pipeline_name"],
    )
    op.create_index(
        "ix_feature_pipeline_runs_status", "feature_pipeline_runs", ["status"]
    )

    # Create feature_monitoring_profiles table
    op.create_table(
        "feature_monitoring_profiles",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column("feature_name", sa.String(length=255), nullable=False, unique=True),
        sa.Column("baseline_stats", sa.Text(), nullable=False),
        sa.Column("monitoring_config", sa.Text(), nullable=True),
        sa.Column("last_updated", sa.DateTime(timezone=True), nullable=False),
        sa.Column("is_active", sa.Boolean(), nullable=False, default=True),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("feature_name"),
    )

    # Create indexes for feature_monitoring_profiles
    op.create_index(
        "ix_feature_monitoring_profiles_id", "feature_monitoring_profiles", ["id"]
    )
    op.create_index(
        "ix_feature_monitoring_profiles_feature_name",
        "feature_monitoring_profiles",
        ["feature_name"],
    )

    # Create feature_drift_detections table
    op.create_table(
        "feature_drift_detections",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column("feature_name", sa.String(length=255), nullable=False),
        sa.Column("drift_score", sa.Float(), nullable=False),
        sa.Column("drift_status", sa.String(length=50), nullable=False),
        sa.Column("detection_method", sa.String(length=100), nullable=False),
        sa.Column("baseline_period_start", sa.DateTime(timezone=True), nullable=False),
        sa.Column("baseline_period_end", sa.DateTime(timezone=True), nullable=False),
        sa.Column("current_period_start", sa.DateTime(timezone=True), nullable=False),
        sa.Column("current_period_end", sa.DateTime(timezone=True), nullable=False),
        sa.Column("statistics", sa.Text(), nullable=True),
        sa.Column(
            "alert_triggered", sa.Boolean(), nullable=False, default=False
        ),
        sa.PrimaryKeyConstraint("id"),
    )

    # Create indexes for feature_drift_detections
    op.create_index(
        "ix_feature_drift_detections_id", "feature_drift_detections", ["id"]
    )
    op.create_index(
        "ix_feature_drift_detections_feature_name",
        "feature_drift_detections",
        ["feature_name"],
    )

    # Create feature_alerts table
    op.create_table(
        "feature_alerts",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column("feature_name", sa.String(length=255), nullable=False),
        sa.Column("alert_type", sa.String(length=100), nullable=False),
        sa.Column("alert_level", sa.String(length=50), nullable=False),
        sa.Column("message", sa.Text(), nullable=False),
        sa.Column("alert_data", sa.Text(), nullable=True),
        sa.Column(
            "acknowledged", sa.Boolean(), nullable=False, default=False
        ),
        sa.Column("acknowledged_by", sa.String(length=255), nullable=True),
        sa.Column("acknowledged_at", sa.DateTime(timezone=True), nullable=True),
        sa.PrimaryKeyConstraint("id"),
    )

    # Create indexes for feature_alerts
    op.create_index("ix_feature_alerts_id", "feature_alerts", ["id"])
    op.create_index(
        "ix_feature_alerts_feature_name", "feature_alerts", ["feature_name"]
    )
    op.create_index("ix_feature_alerts_alert_level", "feature_alerts", ["alert_level"])

    # Create lineage_nodes table
    op.create_table(
        "lineage_nodes",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column("name", sa.String(length=255), nullable=False, unique=True),
        sa.Column("node_type", sa.String(length=50), nullable=False),
        sa.Column("metadata", sa.Text(), nullable=True),
        sa.Column("description", sa.Text(), nullable=True),
        sa.Column("owner", sa.String(length=255), nullable=True),
        sa.Column("tags", sa.Text(), nullable=True),
        sa.Column("is_active", sa.Boolean(), nullable=False, default=True),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("name"),
    )

    # Create indexes for lineage_nodes
    op.create_index("ix_lineage_nodes_id", "lineage_nodes", ["id"])
    op.create_index("ix_lineage_nodes_name", "lineage_nodes", ["name"])
    op.create_index("ix_lineage_nodes_node_type", "lineage_nodes", ["node_type"])

    # Create lineage_edges table
    op.create_table(
        "lineage_edges",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column("source_node_id", sa.Integer(), nullable=False),
        sa.Column("target_node_id", sa.Integer(), nullable=False),
        sa.Column("relation_type", sa.String(length=50), nullable=False),
        sa.Column("metadata", sa.Text(), nullable=True),
        sa.Column("strength", sa.Float(), nullable=False, default=1.0),
        sa.PrimaryKeyConstraint("id"),
        sa.ForeignKeyConstraint(
            ["source_node_id"],
            ["lineage_nodes.id"],
        ),
        sa.ForeignKeyConstraint(
            ["target_node_id"],
            ["lineage_nodes.id"],
        ),
    )

    # Create indexes for lineage_edges
    op.create_index("ix_lineage_edges_id", "lineage_edges", ["id"])
    op.create_index(
        "ix_lineage_edges_source_node_id", "lineage_edges", ["source_node_id"]
    )
    op.create_index(
        "ix_lineage_edges_target_node_id", "lineage_edges", ["target_node_id"]
    )
    op.create_index(
        "ix_lineage_edges_relation_type", "lineage_edges", ["relation_type"]
    )

    # Add update triggers for new tables (PostgreSQL specific)
    try:
        # Pipeline runs table
        op.execute(
            text("""
            CREATE TRIGGER update_feature_pipeline_runs_updated_at
                BEFORE UPDATE ON feature_pipeline_runs
                FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();
        """)
        )

        # Monitoring profiles table
        op.execute(
            text("""
            CREATE TRIGGER update_feature_monitoring_profiles_updated_at
                BEFORE UPDATE ON feature_monitoring_profiles
                FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();
        """)
        )

        # Drift detections table
        op.execute(
            text("""
            CREATE TRIGGER update_feature_drift_detections_updated_at
                BEFORE UPDATE ON feature_drift_detections
                FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();
        """)
        )

        # Alerts table
        op.execute(
            text("""
            CREATE TRIGGER update_feature_alerts_updated_at
                BEFORE UPDATE ON feature_alerts
                FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();
        """)
        )

        # Lineage nodes table
        op.execute(
            text("""
            CREATE TRIGGER update_lineage_nodes_updated_at
                BEFORE UPDATE ON lineage_nodes
                FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();
        """)
        )

        # Lineage edges table
        op.execute(
            text("""
            CREATE TRIGGER update_lineage_edges_updated_at
                BEFORE UPDATE ON lineage_edges
                FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();
        """)
        )
    except Exception:
        # Ignore trigger creation errors (e.g., for SQLite)
        pass


def downgrade() -> None:
    """Drop pipeline, monitoring, and lineage tables."""

    # Drop triggers (PostgreSQL specific)
    try:
        op.execute(
            text(
                "DROP TRIGGER IF EXISTS update_feature_pipeline_runs_updated_at ON feature_pipeline_runs;"
            )
        )
        op.execute(
            text(
                "DROP TRIGGER IF EXISTS update_feature_monitoring_profiles_updated_at ON feature_monitoring_profiles;"
            )
        )
        op.execute(
            text(
                "DROP TRIGGER IF EXISTS update_feature_drift_detections_updated_at ON feature_drift_detections;"
            )
        )
        op.execute(
            text(
                "DROP TRIGGER IF EXISTS update_feature_alerts_updated_at ON feature_alerts;"
            )
        )
        op.execute(
            text(
                "DROP TRIGGER IF EXISTS update_lineage_nodes_updated_at ON lineage_nodes;"
            )
        )
        op.execute(
            text(
                "DROP TRIGGER IF EXISTS update_lineage_edges_updated_at ON lineage_edges;"
            )
        )
    except Exception:
        # Ignore trigger drop errors
        pass

    # Drop indexes and tables in reverse order
    op.drop_index("ix_lineage_edges_relation_type", table_name="lineage_edges")
    op.drop_index("ix_lineage_edges_target_node_id", table_name="lineage_edges")
    op.drop_index("ix_lineage_edges_source_node_id", table_name="lineage_edges")
    op.drop_index("ix_lineage_edges_id", table_name="lineage_edges")
    op.drop_table("lineage_edges")

    op.drop_index("ix_lineage_nodes_node_type", table_name="lineage_nodes")
    op.drop_index("ix_lineage_nodes_name", table_name="lineage_nodes")
    op.drop_index("ix_lineage_nodes_id", table_name="lineage_nodes")
    op.drop_table("lineage_nodes")

    op.drop_index("ix_feature_alerts_alert_level", table_name="feature_alerts")
    op.drop_index("ix_feature_alerts_feature_name", table_name="feature_alerts")
    op.drop_index("ix_feature_alerts_id", table_name="feature_alerts")
    op.drop_table("feature_alerts")

    op.drop_index(
        "ix_feature_drift_detections_feature_name",
        table_name="feature_drift_detections",
    )
    op.drop_index(
        "ix_feature_drift_detections_id", table_name="feature_drift_detections"
    )
    op.drop_table("feature_drift_detections")

    op.drop_index(
        "ix_feature_monitoring_profiles_feature_name",
        table_name="feature_monitoring_profiles",
    )
    op.drop_index(
        "ix_feature_monitoring_profiles_id", table_name="feature_monitoring_profiles"
    )
    op.drop_table("feature_monitoring_profiles")

    op.drop_index("ix_feature_pipeline_runs_status", table_name="feature_pipeline_runs")
    op.drop_index(
        "ix_feature_pipeline_runs_pipeline_name", table_name="feature_pipeline_runs"
    )
    op.drop_index("ix_feature_pipeline_runs_id", table_name="feature_pipeline_runs")
    op.drop_table("feature_pipeline_runs")
