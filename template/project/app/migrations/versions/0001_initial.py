"""initial schema: users and items

Shipped with the template so a freshly generated project can run
``alembic upgrade head`` immediately, rather than having to autogenerate its
first migration before it can start.

Once you change the models, generate the next one::

    uv run alembic revision --autogenerate -m "describe the change"

Autogenerating against these tables should produce an *empty* migration. If it
does not, the models and this file have drifted — investigate rather than
committing the diff, because a spurious diff usually means something (a type, a
server default, a naming convention) is not what you think it is.

Constraint names come from the naming convention on ``keel.database.Model``'s
metadata, which is why they read as ``pk_users`` and ``fk_items_owner_id_users``
rather than whatever Postgres would have picked. That is what makes ``downgrade``
able to name what it drops.

Revision ID: 0001_initial
Revises:
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0001_initial"
down_revision: str | None = None
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Create the users and items tables."""
    op.create_table(
        "users",
        sa.Column("email", sa.String(length=320), nullable=False),
        sa.Column("full_name", sa.String(length=200), nullable=False),
        sa.Column("hashed_password", sa.String(length=255), nullable=False),
        sa.Column("is_active", sa.Boolean(), server_default=sa.text("true"), nullable=False),
        sa.Column("id", sa.Uuid(), nullable=False),
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
        sa.PrimaryKeyConstraint("id", name=op.f("pk_users")),
    )
    op.create_index(op.f("ix_users_email"), "users", ["email"], unique=True)

    op.create_table(
        "items",
        sa.Column("name", sa.String(length=200), nullable=False),
        sa.Column("description", sa.Text(), nullable=True),
        sa.Column("owner_id", sa.Uuid(), nullable=False),
        sa.Column("id", sa.Uuid(), nullable=False),
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
        sa.ForeignKeyConstraint(
            ["owner_id"],
            ["users.id"],
            name=op.f("fk_items_owner_id_users"),
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_items")),
    )
    op.create_index(op.f("ix_items_owner_id"), "items", ["owner_id"], unique=False)


def downgrade() -> None:
    """Drop them again, in dependency order."""
    op.drop_index(op.f("ix_items_owner_id"), table_name="items")
    op.drop_table("items")
    op.drop_index(op.f("ix_users_email"), table_name="users")
    op.drop_table("users")
