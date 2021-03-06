"""empty message

Revision ID: 465785295fcb
Revises: 40f2531a009a
Create Date: 2016-10-14 16:33:36.961293

"""

# revision identifiers, used by Alembic.
revision = '465785295fcb'
down_revision = '40f2531a009a'

from alembic import op
import sqlalchemy as sa


def upgrade():
    ### commands auto generated by Alembic - please adjust! ###
    op.create_table('geographies',
    sa.Column('geography_id', sa.SmallInteger(), nullable=False),
    sa.Column('geography_type', sa.String(), nullable=False),
    sa.Column('geography_name', sa.String(), nullable=False),
    sa.PrimaryKeyConstraint('geography_id')
    )
    op.create_table('quarters',
    sa.Column('quarter_id', sa.SmallInteger(), nullable=False),
    sa.Column('year', sa.Integer(), nullable=False),
    sa.Column('quarter', sa.Integer(), nullable=False),
    sa.PrimaryKeyConstraint('quarter_id')
    )
    op.create_table('jobs_importance',
    sa.Column('quarter_id', sa.SmallInteger(), nullable=False),
    sa.Column('geography_id', sa.SmallInteger(), nullable=False),
    sa.Column('job_uuid', sa.String(), nullable=False),
    sa.Column('importance', sa.Float(), nullable=True),
    sa.ForeignKeyConstraint(['geography_id'], ['geographies.geography_id'], ),
    sa.ForeignKeyConstraint(['quarter_id'], ['quarters.quarter_id'], ),
    sa.PrimaryKeyConstraint('quarter_id', 'geography_id', 'job_uuid')
    )
    ### end Alembic commands ###


def downgrade():
    ### commands auto generated by Alembic - please adjust! ###
    op.drop_table('jobs_importance')
    op.drop_table('quarters')
    op.drop_table('geographies')
    ### end Alembic commands ###
