"""Initial DB Setup

Revision ID: 49bc8370e2fd
Revises: 
Create Date: 2022-06-28 14:53:16.574769

"""
import os
import re

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '49bc8370e2fd'
down_revision = None
branch_labels = None
depends_on = None


def upgrade():
    """Creates the database stub and activates Timescale DB"""
    op.execute("""
        CREATE TABLE data_points (
            id SERIAL NOT NULL,
            name VARCHAR(128) NOT NULL,
            device_id VARCHAR(128) DEFAULT NULL,
            location_code VARCHAR(128) NOT NULL,
            data_provider VARCHAR(128) NOT NULL,
            unit TEXT DEFAULT NULL,
            PRIMARY KEY (id)
        );
    """)


def downgrade():
    """Removes the entire database"""
    op.execute("DROP TABLE data_points")