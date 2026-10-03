"""stored incident diagnosis (regex / model / operator)

Revision ID: c7e2a91f5b30
Revises: b3f1c9a2d4e7
Create Date: 2026-10-01 12:00:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'c7e2a91f5b30'
down_revision: Union[str, Sequence[str], None] = 'b3f1c9a2d4e7'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    with op.batch_alter_table('incidents', schema=None) as batch_op:
        batch_op.add_column(sa.Column('diagnosis', sa.JSON(), nullable=True))


def downgrade() -> None:
    """Downgrade schema."""
    with op.batch_alter_table('incidents', schema=None) as batch_op:
        batch_op.drop_column('diagnosis')
