"""store workflow template provenance

Revision ID: 2a4b6c8d0e1f
Revises: 1f3a7b9c2d4e
Create Date: 2026-09-30 10:00:00.000000
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "2a4b6c8d0e1f"
down_revision: str | Sequence[str] | None = "1f3a7b9c2d4e"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.add_column(
        "automation_workflows",
        sa.Column("template_key", sa.String(length=100), nullable=True),
    )
    op.add_column(
        "automation_workflows",
        sa.Column("template_version", sa.Integer(), nullable=True),
    )
    op.add_column(
        "automation_workflows",
        sa.Column("template_parameters", sa.JSON(), nullable=True),
    )


def downgrade() -> None:
    op.drop_column("automation_workflows", "template_parameters")
    op.drop_column("automation_workflows", "template_version")
    op.drop_column("automation_workflows", "template_key")