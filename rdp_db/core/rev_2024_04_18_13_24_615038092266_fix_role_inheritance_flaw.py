"""fix role inheritance flaw

Revision ID: 615038092266
Revises: b158d45bc708
Create Date: 2024-04-18 13:24:41.464530

"""
import os
import logging

from alembic import op
import sqlalchemy as sa

import rdp_db.utils.db_version as db_version

logger = logging.getLogger(__name__)

# revision identifiers, used by Alembic.
revision = '615038092266'
down_revision = 'b158d45bc708'
branch_labels = None
depends_on = None


def upgrade():
    """Fixes the inheritance and group membership issue"""

    data_vis_user = os.environ['POSTGRES_DATA_VIS_USER']
    data_source_user = os.environ['POSTGRES_DATA_SOURCE_USER']
    data_pub_vis_user = os.environ['POSTGRES_DATA_PUB_VIS_USER']

    if db_version.get_version_int() >= 160000:
        # The WITH INHERIT clause is only supported from 16.0 on. For prior versions, it looks like the inheritance
        # fix is not needed.
        op.execute(sa.text(f"""
            GRANT view_internal TO "{data_vis_user}", "{data_source_user}" WITH INHERIT TRUE;
            GRANT view_public TO "{data_vis_user}", "{data_source_user}", "{data_pub_vis_user}" WITH INHERIT TRUE;
            GRANT data_source_base TO "{data_source_user}" WITH INHERIT TRUE;
        """))
    else:
        logger.info("Skip role fix (615038092266) for PG versions <16.0")


def downgrade():
    """Introduces the credential bugs again."""
    data_vis_user = os.environ['POSTGRES_DATA_VIS_USER']
    data_source_user = os.environ['POSTGRES_DATA_SOURCE_USER']

    if db_version.get_version_int() >= 160000:
        # The WITH INHERIT clause is only supported from 16.0 on. For prior versions, it looks like the inheritance
        # fix is not needed.
        op.execute(sa.text(f"""
            GRANT view_internal TO "{data_vis_user}", "{data_source_user}" WITH INHERIT FALSE;
            GRANT view_public TO "{data_vis_user}", "{data_source_user}" WITH INHERIT FALSE;
            GRANT data_source_base TO "{data_source_user}" WITH INHERIT FALSE;
        """))
    else:
        logger.info("Skip role fix (615038092266) for PG versions <16.0")
