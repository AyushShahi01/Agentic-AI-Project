"""workflow run to-do list (one output can link to several blocks)

Revision ID: b3f1c9a2d4e7
Revises: 74a06e885cae
Create Date: 2026-09-28 12:00:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'b3f1c9a2d4e7'
down_revision: Union[str, Sequence[str], None] = '74a06e885cae'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    with op.batch_alter_table('workflow_runs', schema=None) as batch_op:
        batch_op.add_column(sa.Column('pending_nodes', sa.JSON(), nullable=True))


def downgrade() -> None:
    """Downgrade schema."""
    with op.batch_alter_table('workflow_runs', schema=None) as batch_op:
        batch_op.drop_column('pending_nodes')
