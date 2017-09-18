"""init data

Revision ID: 65b0675136d9
Revises: 4d08c32a7936
Create Date: 2017-09-18 08:14:36.791249

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '65b0675136d9'
down_revision = '4d08c32a7936'
branch_labels = None
depends_on = None


from sqlalchemy.orm import sessionmaker
Session = sessionmaker()

import os
import sys
sys.path.append(os.path.dirname(os.path.abspath(__file__)) + "/../../../../../")
from nwpc_log_model.migration import init_data


def upgrade():
    bind = op.get_bind()
    session = Session(bind=bind)
    init_data.initial_users(session)
    init_data.init_repos(session)
    init_data.init_sms_repos(session)


def downgrade():
    bind = op.get_bind()
    session = Session(bind=bind)
    init_data.remove_sms_repos(session)
    init_data.remove_repos(session)
    init_data.remove_users(session)
