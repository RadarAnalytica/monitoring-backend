"""added table products

Revision ID: 146c6819faa7
Revises: 1c01a368417b
Create Date: 2024-10-15 13:06:36.633921

"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = "146c6819faa7"
down_revision: Union[str, None] = "1c01a368417b"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # ### commands auto generated by Alembic - please adjust! ###
    op.create_table(
        "products",
        sa.Column("wb_id", sa.Integer(), nullable=False),
        sa.Column("name", sa.String(), nullable=False),
        sa.Column("brand", sa.String(), nullable=True),
        sa.Column("brandId", sa.Integer(), nullable=True),
        sa.Column("supplier", sa.String(), nullable=True),
        sa.Column("supplierId", sa.Integer(), nullable=True),
        sa.Column("entity", sa.String(), nullable=True),
        sa.PrimaryKeyConstraint("wb_id"),
    )
    # ### end Alembic commands ###


def downgrade() -> None:
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_table("products")
    # ### end Alembic commands ###
