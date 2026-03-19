"""Add llm analysis fields

Revision ID: 4f2a6d8f1c0b
Revises: 2733ee392aa1
Create Date: 2026-03-19 10:30:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '4f2a6d8f1c0b'
down_revision: Union[str, Sequence[str], None] = '2733ee392aa1'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    op.add_column('jobs', sa.Column('llm_fit_score', sa.Integer(), nullable=True))
    op.add_column('jobs', sa.Column('llm_strengths', sa.Text(), nullable=True))
    op.add_column('jobs', sa.Column('fit_explanation', sa.Text(), nullable=True))
    op.add_column('jobs', sa.Column('skill_gaps', sa.Text(), nullable=True))
    op.add_column('jobs', sa.Column('recommendation', sa.String(), nullable=True))
    op.add_column('jobs', sa.Column('llm_confidence', sa.Integer(), nullable=True))
    op.add_column('jobs', sa.Column('llm_status', sa.String(), nullable=True))


def downgrade() -> None:
    """Downgrade schema."""
    op.drop_column('jobs', 'llm_status')
    op.drop_column('jobs', 'llm_confidence')
    op.drop_column('jobs', 'recommendation')
    op.drop_column('jobs', 'skill_gaps')
    op.drop_column('jobs', 'fit_explanation')
    op.drop_column('jobs', 'llm_strengths')
    op.drop_column('jobs', 'llm_fit_score')
