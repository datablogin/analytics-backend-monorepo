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
    # Data validation: Check for orphaned records before adding foreign key constraints
    connection = op.get_bind()

    # Check for orphaned user_roles records
    orphaned_user_roles = connection.execute(sa.text("""
        SELECT COUNT(*) FROM user_roles ur
        WHERE ur.user_id NOT IN (SELECT id FROM users)
           OR ur.role_id NOT IN (SELECT id FROM roles)
    """)).scalar()

    if orphaned_user_roles > 0:
        # Clean up orphaned user_roles records
        connection.execute(sa.text("""
            DELETE FROM user_roles
            WHERE user_id NOT IN (SELECT id FROM users)
               OR role_id NOT IN (SELECT id FROM roles)
        """))
        print(f"Cleaned up {orphaned_user_roles} orphaned user_roles records")

    # Check for orphaned audit_logs records
    orphaned_audit_logs = connection.execute(sa.text("""
        SELECT COUNT(*) FROM audit_logs al
        WHERE al.user_id IS NOT NULL
          AND al.user_id NOT IN (SELECT id FROM users)
    """)).scalar()

    if orphaned_audit_logs > 0:
        # Set orphaned audit_logs user_id to NULL instead of deleting
        connection.execute(sa.text("""
            UPDATE audit_logs
            SET user_id = NULL
            WHERE user_id IS NOT NULL
              AND user_id NOT IN (SELECT id FROM users)
        """))
        print(f"Cleaned up {orphaned_audit_logs} orphaned audit_logs records by setting user_id to NULL")

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
