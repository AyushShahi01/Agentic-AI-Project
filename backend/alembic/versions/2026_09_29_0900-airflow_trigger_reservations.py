"""guard concurrent automation DAG triggers

Revision ID: 1f3a7b9c2d4e
Revises: b3f1c9a2d4e7
Create Date: 2026-09-29 09:00:00.000000
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "1f3a7b9c2d4e"
down_revision: str | Sequence[str] | None = "b3f1c9a2d4e7"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "airflow_trigger_reservations",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("connection_id", sa.Uuid(), nullable=False),
        sa.Column("dag_id", sa.String(length=250), nullable=False),
        sa.Column("workflow_run_id", sa.Uuid(), nullable=False),
        sa.Column("node_id", sa.String(length=100), nullable=False),
        sa.Column("airflow_run_id", sa.String(length=250), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["connection_id"], ["airflow_connections.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["workflow_run_id"], ["workflow_runs.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("connection_id", "dag_id"),
    )
    op.create_index(
        "ix_airflow_trigger_reservations_connection_id",
        "airflow_trigger_reservations",
        ["connection_id"],
    )
    op.create_index(
        "ix_airflow_trigger_reservations_workflow_run",
        "airflow_trigger_reservations",
        ["workflow_run_id"],
    )


def downgrade() -> None:
    op.drop_index(
        "ix_airflow_trigger_reservations_workflow_run",
        table_name="airflow_trigger_reservations",
    )
    op.drop_index(
        "ix_airflow_trigger_reservations_connection_id", table_name="airflow_trigger_reservations"
    )
    op.drop_table("airflow_trigger_reservations")