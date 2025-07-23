"""Add foreign key constraints and relationships

Revision ID: e5356177cc7e
Revises: 51373eb465d5
Create Date: 2025-07-23 12:42:09.842419

"""

from collections.abc import Sequence

import sqlalchemy as sa

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "e5356177cc7e"
down_revision: str | Sequence[str] | None = "51373eb465d5"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Upgrade schema."""
    # Add is_active column to roles table
    with op.batch_alter_table("roles", schema=None) as batch_op:
        batch_op.add_column(
            sa.Column("is_active", sa.Boolean(), nullable=False, server_default="1")
        )

    # Add foreign key constraints using batch mode for SQLite compatibility
    with op.batch_alter_table("audit_logs", schema=None) as batch_op:
        batch_op.create_foreign_key(
            op.f("fk_audit_logs_user_id_users"), "users", ["user_id"], ["id"]
        )

    with op.batch_alter_table("user_roles", schema=None) as batch_op:
        batch_op.create_foreign_key(
            op.f("fk_user_roles_user_id_users"), "users", ["user_id"], ["id"]
        )
        batch_op.create_foreign_key(
            op.f("fk_user_roles_role_id_roles"), "roles", ["role_id"], ["id"]
        )


def downgrade() -> None:
    """Downgrade schema."""
    # Drop foreign key constraints using batch mode for SQLite compatibility
    with op.batch_alter_table("user_roles", schema=None) as batch_op:
        batch_op.drop_constraint(
            op.f("fk_user_roles_role_id_roles"), type_="foreignkey"
        )
        batch_op.drop_constraint(
            op.f("fk_user_roles_user_id_users"), type_="foreignkey"
        )

    with op.batch_alter_table("audit_logs", schema=None) as batch_op:
        batch_op.drop_constraint(
            op.f("fk_audit_logs_user_id_users"), type_="foreignkey"
        )

    # Remove is_active column from roles table
    with op.batch_alter_table("roles", schema=None) as batch_op:
        batch_op.drop_column("is_active")
