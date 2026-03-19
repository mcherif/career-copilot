"""Add rule_status to jobs

Revision ID: 8d6b7f2e1c4a
Revises: 4f2a6d8f1c0b
Create Date: 2026-03-19 15:35:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '8d6b7f2e1c4a'
down_revision: Union[str, Sequence[str], None] = '4f2a6d8f1c0b'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    op.add_column('jobs', sa.Column('rule_status', sa.String(), nullable=True))


def downgrade() -> None:
    """Downgrade schema."""
    op.drop_column('jobs', 'rule_status')
