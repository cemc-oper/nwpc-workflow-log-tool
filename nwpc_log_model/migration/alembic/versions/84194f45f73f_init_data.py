"""init data

Revision ID: 84194f45f73f
Revises: 96fe21aa6523
Create Date: 2017-01-07 12:08:39.334963

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '84194f45f73f'
down_revision = '96fe21aa6523'
branch_labels = None
depends_on = None

from sqlalchemy.orm import sessionmaker
Session = sessionmaker()

import os
import sys
sys.path.append(os.path.dirname(os.path.abspath(__file__)) + "/../../../../")
from nwpc_log_model.migration import init_data


def upgrade():
    bind = op.get_bind()
    session = Session(bind=bind)
    init_data.initial_users(session)
    init_data.init_repos(session)


def downgrade():
    bind = op.get_bind()
    session = Session(bind=bind)
    init_data.remove_repos(session)
    init_data.remove_users(session)
