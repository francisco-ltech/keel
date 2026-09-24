"""Password resets: a single-use code, hashed at rest, per request.

Revision ID: 0004_password_resets
Revises: 0003_public_ids

The code itself is never stored: ``code_digest`` is its SHA-256, like the
bearer token store keeps. ``used_at`` is set once by the redeeming statement
and is what makes a code single-use under concurrency.
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision: str = "0004_password_resets"
down_revision: str | None = "0003_public_ids"
branch_labels: str | None = None
depends_on: str | None = None


def upgrade() -> None:
    """Create the password_resets table."""
    op.create_table(
        "password_resets",
        sa.Column("user_id", sa.Uuid(), nullable=False),
        sa.Column("code_digest", sa.String(length=64), nullable=False),
        sa.Column("expires_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("used_at", sa.DateTime(timezone=True), nullable=True),
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
            ["user_id"],
            ["users.id"],
            name=op.f("fk_password_resets_user_id_users"),
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_password_resets")),
    )
    op.create_index(
        op.f("ix_password_resets_code_digest"), "password_resets", ["code_digest"], unique=True
    )
    op.create_index(
        op.f("ix_password_resets_user_id"), "password_resets", ["user_id"], unique=False
    )


def downgrade() -> None:
    """Drop it again."""
    op.drop_index(op.f("ix_password_resets_user_id"), table_name="password_resets")
    op.drop_index(op.f("ix_password_resets_code_digest"), table_name="password_resets")
    op.drop_table("password_resets")
