"""Initial feature store schema migration.

Revision ID: 001_feature_store
Revises:
Create Date: 2024-01-01 00:00:00.000000

"""

import sqlalchemy as sa
from alembic import op
from sqlalchemy import text

# revision identifiers
revision = "001_feature_store"
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    """Create feature store tables."""

    # Create features table
    op.create_table(
        "features",
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
        sa.Column("name", sa.String(length=255), nullable=False),
        sa.Column("feature_group", sa.String(length=255), nullable=False),
        sa.Column("description", sa.Text(), nullable=True),
        sa.Column(
            "feature_type",
            sa.Enum(
                "string",
                "integer",
                "float",
                "boolean",
                "timestamp",
                "json",
                name="featuretype",
            ),
            nullable=False,
        ),
        sa.Column(
            "status",
            sa.Enum("active", "inactive", "deprecated", name="featurestatus"),
            nullable=False,
        ),
        sa.Column("default_value", sa.Text(), nullable=True),
        sa.Column("validation_rules", sa.Text(), nullable=True),
        sa.Column("tags", sa.Text(), nullable=True),
        sa.Column("owner", sa.String(length=255), nullable=True),
        sa.Column("version", sa.Integer(), nullable=False),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("name"),
    )

    # Create indexes for features table
    op.create_index("ix_features_id", "features", ["id"])
    op.create_index("ix_features_name", "features", ["name"])
    op.create_index("ix_features_feature_group", "features", ["feature_group"])
    op.create_index("ix_features_status", "features", ["status"])

    # Create feature_values table
    op.create_table(
        "feature_values",
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
        sa.Column("entity_id", sa.String(length=255), nullable=False),
        sa.Column("value", sa.Text(), nullable=False),
        sa.Column("timestamp", sa.DateTime(timezone=True), nullable=False),
        sa.Column("version", sa.Integer(), nullable=False),
        sa.PrimaryKeyConstraint("id"),
    )

    # Create indexes for feature_values table
    op.create_index("ix_feature_values_id", "feature_values", ["id"])
    op.create_index(
        "ix_feature_values_feature_name", "feature_values", ["feature_name"]
    )
    op.create_index("ix_feature_values_entity_id", "feature_values", ["entity_id"])
    op.create_index("ix_feature_values_timestamp", "feature_values", ["timestamp"])
    op.create_index(
        "ix_feature_values_composite",
        "feature_values",
        ["feature_name", "entity_id", "timestamp"],
    )

    # Add update trigger for updated_at columns (PostgreSQL specific)
    # This will be ignored for SQLite
    try:
        op.execute(
            text("""
            CREATE OR REPLACE FUNCTION update_updated_at_column()
            RETURNS TRIGGER AS $$
            BEGIN
                NEW.updated_at = now();
                RETURN NEW;
            END;
            $$ language 'plpgsql';
        """)
        )

        op.execute(
            text("""
            CREATE TRIGGER update_features_updated_at
                BEFORE UPDATE ON features
                FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();
        """)
        )

        op.execute(
            text("""
            CREATE TRIGGER update_feature_values_updated_at
                BEFORE UPDATE ON feature_values
                FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();
        """)
        )
    except Exception:
        # Ignore trigger creation errors (e.g., for SQLite)
        pass


def downgrade() -> None:
    """Drop feature store tables."""

    # Drop triggers (PostgreSQL specific)
    try:
        op.execute(
            text("DROP TRIGGER IF EXISTS update_features_updated_at ON features;")
        )
        op.execute(
            text(
                "DROP TRIGGER IF EXISTS update_feature_values_updated_at ON feature_values;"
            )
        )
        op.execute(text("DROP FUNCTION IF EXISTS update_updated_at_column();"))
    except Exception:
        # Ignore trigger drop errors
        pass

    # Drop indexes
    op.drop_index("ix_feature_values_composite", table_name="feature_values")
    op.drop_index("ix_feature_values_timestamp", table_name="feature_values")
    op.drop_index("ix_feature_values_entity_id", table_name="feature_values")
    op.drop_index("ix_feature_values_feature_name", table_name="feature_values")
    op.drop_index("ix_feature_values_id", table_name="feature_values")

    op.drop_index("ix_features_status", table_name="features")
    op.drop_index("ix_features_feature_group", table_name="features")
    op.drop_index("ix_features_name", table_name="features")
    op.drop_index("ix_features_id", table_name="features")

    # Drop tables
    op.drop_table("feature_values")
    op.drop_table("features")

    # Drop enums
    op.execute(text("DROP TYPE IF EXISTS featurestatus;"))
    op.execute(text("DROP TYPE IF EXISTS featuretype;"))
